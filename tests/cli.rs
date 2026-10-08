use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};

use tempfile::TempDir;

const KI: usize = 1024;

struct Run {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
}

impl Run {
    #[track_caller]
    fn success(self) -> Self {
        assert_eq!(self.code, Some(0), "stderr:\n{}", self.stderr);
        self
    }

    #[track_caller]
    fn failure(self, code: i32, message: &str) -> Self {
        assert_eq!(self.code, Some(code), "stderr:\n{}", self.stderr);
        assert!(self.stderr.contains(message), "expected {message:?} in stderr:\n{}", self.stderr);
        self
    }

    /// The `checksum: <hex>` value reported on stderr
    #[track_caller]
    fn checksum(&self) -> String {
        self.stderr
            .lines()
            .find_map(|l| l.split("checksum: ").nth(1))
            .unwrap_or_else(|| panic!("no checksum in stderr:\n{}", self.stderr))
            .trim()
            .to_string()
    }
}

/// Run randstream in `dir`, optionally feeding `stdin`
fn randstream(dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> Run {
    let mut child = Command::new(env!("CARGO_BIN_EXE_randstream"))
        .current_dir(dir)
        .args(args)
        .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(data) = stdin {
        let mut pipe = child.stdin.take().unwrap();
        let data = data.to_vec();
        // the process may exit before consuming everything, so ignore write errors
        std::thread::spawn(move || pipe.write_all(&data));
    }
    let out = child.wait_with_output().unwrap();
    Run {
        code: out.status.code(),
        stdout: out.stdout,
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Generate a stream on stdout: the single threaded reference implementation
fn reference(args: &[&str]) -> Vec<u8> {
    let tmp = TempDir::new().unwrap();
    randstream(tmp.path(), &[&["generate"], args].concat(), None).success().stdout
}

/// The stream format is stable: pinned checksums, identical bytes whatever the
/// output or job count, and validation from a file or stdin agrees.
#[test]
fn stream_format_is_stable() {
    #[rustfmt::skip]
    let cases = [
        // size,    chunk,   seed,    jobs, checksum
        ("0",       "32Ki",  "0",     "2",  "00000000"), // empty stream
        ("1Ki",     "1Ki",   "0",     "4",  "48181a23"), // more jobs than chunks
        ("32Ki",    "32Ki",  "1",     "1",  "855bfcd1"),
        ("33Ki",    "32Ki",  "2",     "1",  "aa0e5a26"), // short tail chunk
        ("32770",   "32Ki",  "0",     "2",  "3be0f192"), // tail too short for a checksum
        ("10000",   "1001",  "0",     "3",  "d3e31dfb"), // chunk size not a multiple of 8
        ("64Ki",    "32Ki",  "3",     "2",  "5af3b3b3"),
        ("256Ki",   "64Ki",  "99",    "3",  "48bdc095"),
        ("1Mi",     "32Ki",  "12345", "1",  "9e904c23"),
        ("1Mi",     "32Ki",  "12345", "4",  "9e904c23"),
    ];
    for (size, chunk, seed, jobs, checksum) in cases {
        let ctx = format!("size={size} chunk={chunk} seed={seed} jobs={jobs}");
        let dir = TempDir::new().unwrap();
        let d = dir.path();
        let opts = ["--size", size, "--chunk-size", chunk, "--seed", seed];

        let generated =
            randstream(d, &[&["generate", "--jobs", jobs], &opts[..], &["out.bin"]].concat(), None)
                .success();
        assert_eq!(generated.checksum(), checksum, "{ctx}");
        let data = fs::read(d.join("out.bin")).unwrap();
        assert!(data == reference(&opts), "file and stdout outputs differ for {ctx}");

        let from_file =
            randstream(d, &["validate", "--chunk-size", chunk, "--jobs", jobs, "out.bin"], None)
                .success();
        assert_eq!(from_file.checksum(), checksum, "{ctx}");
        let from_stdin = randstream(d, &["validate", "--chunk-size", chunk], Some(&data)).success();
        assert_eq!(from_stdin.checksum(), checksum, "{ctx}");
    }
}

#[test]
fn defaults_are_seed_0_and_32ki_chunks() {
    assert!(reference(&["-s", "40Ki"]) == reference(&["-s", "40Ki", "-S", "0", "-c", "32Ki"]));
    assert!(reference(&["-s", "40Ki", "-S", "1"]) != reference(&["-s", "40Ki", "-S", "2"]));
}

#[test]
fn aliases() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    let w = randstream(d, &["write", "-s", "32Ki", "out.bin"], None).success();
    let r = randstream(d, &["read", "out.bin"], None).success();
    assert_eq!(w.checksum(), r.checksum());
}

/// Without --size, the stream fills the file from --position to its end
#[test]
fn size_defaults_to_the_file_size() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    fs::write(d.join("out.bin"), vec![0xffu8; 100 * KI]).unwrap();
    let g = randstream(d, &["generate", "-p", "10Ki", "out.bin"], None).success();
    let data = fs::read(d.join("out.bin")).unwrap();
    assert_eq!(data.len(), 100 * KI);
    assert!(data[..10 * KI].iter().all(|&b| b == 0xff));
    assert!(data[10 * KI..] == reference(&["-s", "90Ki"]));
    let v = randstream(d, &["validate", "-p", "10Ki", "out.bin"], None).success();
    assert_eq!(g.checksum(), v.checksum());
}

#[test]
fn truncation() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    let path = d.join("out.bin");
    // (initial size, --no-truncate, expected final size)
    for (initial, no_truncate, expected) in
        [(128, false, 32), (128, true, 128), (16, true, 32), (16, false, 32)]
    {
        fs::write(&path, vec![0u8; initial * KI]).unwrap();
        let flag = if no_truncate { "--no-truncate" } else { "--seed=0" };
        randstream(d, &["generate", "-s", "32Ki", flag, "out.bin"], None).success();
        assert_eq!(
            fs::metadata(&path).unwrap().len() as usize,
            expected * KI,
            "{initial}Ki {flag}"
        );
    }
}

/// --position places the stream at any byte offset, without touching the
/// surrounding data, and validate can read it back from a file or stdin.
#[test]
fn position() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    let path = d.join("out.bin");
    fs::write(&path, vec![0xffu8; 128 * KI]).unwrap();
    let opts = ["--size", "64Ki", "--chunk-size", "4Ki", "--seed", "7"];

    let g = randstream(
        d,
        &[&["generate", "--no-truncate", "-p", "1001"], &opts[..], &["out.bin"]].concat(),
        None,
    )
    .success();
    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), 128 * KI);
    assert!(data[..1001].iter().all(|&b| b == 0xff));
    assert!(data[1001..1001 + 64 * KI] == reference(&opts));
    assert!(data[1001 + 64 * KI..].iter().all(|&b| b == 0xff));

    let validate = [&["validate", "-p", "1001"], &opts[..2], &opts[2..4]].concat();
    let from_file = randstream(d, &[&validate[..], &["out.bin"]].concat(), None).success();
    let from_stdin = randstream(d, &validate, Some(&data[..1001 + 64 * KI])).success();
    assert_eq!(g.checksum(), from_file.checksum());
    assert_eq!(g.checksum(), from_stdin.checksum());
}

/// Every kind of damage is detected and located, both from a file and from stdin
#[test]
fn validate_detects_corruption() {
    let stream = reference(&["--size", "65538", "--chunk-size", "32Ki"]);
    let flip = |offset: usize| {
        let mut data = stream.clone();
        data[offset] ^= 0x01;
        data
    };
    let cases = [
        ("payload of chunk 0", flip(1000), "Invalid checksum at chunk 0"),
        ("payload of chunk 1", flip(32 * KI + 100), "Invalid checksum at chunk 1"),
        ("checksum of chunk 1", flip(64 * KI - 1), "Invalid checksum at chunk 1"),
        ("zero filled tail", flip(64 * KI + 1), "Invalid non-zero value"),
        ("truncated mid chunk", stream[..50 * KI].to_vec(), "Invalid checksum at chunk 1"),
    ];
    for (what, data, message) in cases {
        let dir = TempDir::new().unwrap();
        let d = dir.path();
        fs::write(d.join("out.bin"), &data).unwrap();
        for jobs in ["1", "3"] {
            randstream(d, &["validate", "-j", jobs, "out.bin"], None).failure(1, message);
        }
        randstream(d, &["validate"], Some(&data)).failure(1, message);
        eprintln!("ok: {what}");
    }
}

#[test]
fn expected_checksum() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    let checksum = randstream(d, &["generate", "-s", "64Ki", "out.bin"], None).success().checksum();
    randstream(d, &["validate", "-e", &checksum, "out.bin"], None).success();
    randstream(d, &["validate", "-e", "deadbeef", "out.bin"], None).failure(
        1,
        &format!("Checksum mismatch. It was expected to be deadbeef, but is actually {checksum}"),
    );
}

#[test]
fn errors() {
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    fs::write(d.join("small.bin"), vec![0u8; 32 * KI]).unwrap();
    let cases: &[(&[&str], i32, &str)] = &[
        // usage errors
        (&[], 2, "Usage"),
        (&["generate", "--does-not-exist"], 2, "unexpected argument"),
        (&["generate", "-s", "1Ki", "-p", "0"], 2, "required arguments were not provided"),
        (&["generate", "-s", "nope", "out.bin"], 2, "invalid value"),
        // runtime errors
        (&["generate", "missing.bin"], 1, "Size can't be determined"),
        (&["generate", "-p", "64Ki", "small.bin"], 1, "greater than the file size"),
        (&["validate", "-p", "64Ki", "small.bin"], 1, "greater than the file size"),
        (&["validate", "missing.bin"], 1, "No such file or directory"),
    ];
    for (args, code, message) in cases {
        randstream(d, args, None).failure(*code, message);
    }
}

/// SIGINT stops the workers and exits with the conventional 130 code
#[test]
fn interrupt() {
    let dir = TempDir::new().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_randstream"))
        .current_dir(dir.path())
        .args(["generate", "-s", "100G", "out.bin"])
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(500));
    Command::new("kill").args(["-INT", &child.id().to_string()]).status().unwrap();
    let status = child.wait_with_output().unwrap().status;
    assert_eq!(status.code(), Some(130));
}
