use std::fs;
use std::io::Write as _;
use std::process::{Command, Stdio};

use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_randstream"))
}

/// Run a generate command in `dir`, return the Command output.
fn generate(dir: &TempDir, extra_args: &[&str]) -> std::process::Output {
    let mut cmd = bin();
    cmd.current_dir(dir.path()).arg("generate").args(["--no-progress"]).args(extra_args);
    cmd.output().expect("failed to spawn randstream generate")
}

/// Run a validate command in `dir`, return the Command output.
fn validate(dir: &TempDir, extra_args: &[&str]) -> std::process::Output {
    let mut cmd = bin();
    cmd.current_dir(dir.path()).arg("validate").args(["--no-progress"]).args(extra_args);
    cmd.output().expect("failed to spawn randstream validate")
}

/// Extract the `checksum: <hex>` value from stderr (where `info!` logs go).
fn parse_checksum(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        if let Some(rest) = line.split("checksum: ").nth(1) {
            return rest.trim().to_string();
        }
    }
    panic!("no checksum found in stderr:\n{stderr}");
}

// ---------------------------------------------------------------------------
// generate – basic
// ---------------------------------------------------------------------------

#[test]
fn generate_creates_file_with_correct_size() {
    let dir = TempDir::new().unwrap();
    let out = generate(&dir, &["--size", "64Ki", "--seed", "0", "out.bin"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let size = fs::metadata(dir.path().join("out.bin")).unwrap().len();
    assert_eq!(size, 64 * 1024);
}

#[test]
fn generate_size_equals_existing_file() {
    // When --size is omitted the file's own size is used.
    let dir = TempDir::new().unwrap();
    // First pass: create a 32 KiB file.
    let out = generate(&dir, &["--size", "32Ki", "--seed", "1", "out.bin"]);
    assert!(out.status.success());
    // Second pass without --size: regenerate using stored file size.
    let out2 = generate(&dir, &["--seed", "1", "out.bin"]);
    assert!(out2.status.success(), "{}", String::from_utf8_lossy(&out2.stderr));
    // Validation must still pass.
    let v = validate(&dir, &["out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

#[test]
fn generate_default_seed_is_zero() {
    // Two invocations with no --seed must produce the same checksum.
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let o1 = generate(&dir1, &["--size", "32Ki", "out.bin"]);
    let o2 = generate(&dir2, &["--size", "32Ki", "out.bin"]);
    assert!(o1.status.success());
    assert!(o2.status.success());
    assert_eq!(parse_checksum(&o1), parse_checksum(&o2));
    let b1 = fs::read(dir1.path().join("out.bin")).unwrap();
    let b2 = fs::read(dir2.path().join("out.bin")).unwrap();
    assert_eq!(b1, b2);
}

#[test]
fn generate_different_seeds_produce_different_data() {
    let dir = TempDir::new().unwrap();
    let o1 = generate(&dir, &["--size", "32Ki", "--seed", "1", "a.bin"]);
    let o2 = generate(&dir, &["--size", "32Ki", "--seed", "2", "b.bin"]);
    assert!(o1.status.success());
    assert!(o2.status.success());
    let b1 = fs::read(dir.path().join("a.bin")).unwrap();
    let b2 = fs::read(dir.path().join("b.bin")).unwrap();
    assert_ne!(b1, b2);
}

#[test]
fn generate_is_deterministic_across_invocations() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let o1 = generate(&dir1, &["--size", "256Ki", "--seed", "42", "out.bin"]);
    let o2 = generate(&dir2, &["--size", "256Ki", "--seed", "42", "out.bin"]);
    assert!(o1.status.success());
    assert!(o2.status.success());
    let b1 = fs::read(dir1.path().join("out.bin")).unwrap();
    let b2 = fs::read(dir2.path().join("out.bin")).unwrap();
    assert_eq!(b1, b2);
    assert_eq!(parse_checksum(&o1), parse_checksum(&o2));
}

#[test]
fn generate_checksum_is_deterministic_regardless_of_job_count() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let o1 = generate(&dir1, &["--size", "256Ki", "--seed", "7", "--jobs", "1", "out.bin"]);
    let o2 = generate(&dir2, &["--size", "256Ki", "--seed", "7", "--jobs", "4", "out.bin"]);
    assert!(o1.status.success());
    assert!(o2.status.success());
    assert_eq!(parse_checksum(&o1), parse_checksum(&o2));
    let b1 = fs::read(dir1.path().join("out.bin")).unwrap();
    let b2 = fs::read(dir2.path().join("out.bin")).unwrap();
    assert_eq!(b1, b2);
}

// ---------------------------------------------------------------------------
// generate – write alias
// ---------------------------------------------------------------------------

#[test]
fn generate_write_alias_works() {
    let dir = TempDir::new().unwrap();
    let out = bin()
        .current_dir(dir.path())
        .args(["write", "--no-progress", "--size", "32Ki", "--seed", "0", "out.bin"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(fs::metadata(dir.path().join("out.bin")).unwrap().len(), 32 * 1024);
}

// ---------------------------------------------------------------------------
// generate – chunk-size variants
// ---------------------------------------------------------------------------

#[test]
fn generate_validate_small_chunk_size() {
    let dir = TempDir::new().unwrap();
    let o = generate(&dir, &["--size", "64Ki", "--chunk-size", "1Ki", "--seed", "5", "out.bin"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v = validate(&dir, &["--chunk-size", "1Ki", "out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

#[test]
fn generate_validate_large_chunk_size() {
    let dir = TempDir::new().unwrap();
    let o = generate(&dir, &["--size", "1Mi", "--chunk-size", "128Ki", "--seed", "9", "out.bin"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v = validate(&dir, &["--chunk-size", "128Ki", "out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

#[test]
fn generate_validate_size_not_multiple_of_chunk() {
    // stream size (70000 bytes) is not a multiple of chunk size (32Ki = 32768) –
    // the tail chunk is smaller.
    let dir = TempDir::new().unwrap();
    let o = generate(&dir, &["--size", "70000", "--chunk-size", "32Ki", "--seed", "3", "out.bin"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v = validate(&dir, &["--chunk-size", "32Ki", "out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

// ---------------------------------------------------------------------------
// generate – stdout path
// ---------------------------------------------------------------------------

#[test]
fn generate_to_stdout_produces_correct_size() {
    let out = bin()
        .args(["generate", "--no-progress", "--size", "32Ki", "--seed", "0"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(out.stdout.len(), 32 * 1024);
}

#[test]
fn generate_to_stdout_is_deterministic() {
    let o1 = bin()
        .args(["generate", "--no-progress", "--size", "64Ki", "--seed", "99"])
        .output()
        .unwrap();
    let o2 = bin()
        .args(["generate", "--no-progress", "--size", "64Ki", "--seed", "99"])
        .output()
        .unwrap();
    assert!(o1.status.success());
    assert!(o2.status.success());
    assert_eq!(o1.stdout, o2.stdout);
}

#[test]
fn generate_stdout_matches_file_output() {
    let dir = TempDir::new().unwrap();
    // Generate to file.
    generate(&dir, &["--size", "32Ki", "--seed", "55", "out.bin"]);
    let file_bytes = fs::read(dir.path().join("out.bin")).unwrap();
    // Generate to stdout.
    let out = bin()
        .args(["generate", "--no-progress", "--size", "32Ki", "--seed", "55"])
        .output()
        .unwrap();
    assert_eq!(file_bytes, out.stdout);
}

// ---------------------------------------------------------------------------
// generate – --position
// ---------------------------------------------------------------------------

#[test]
fn generate_with_position_fills_only_the_specified_region() {
    const CHUNK: usize = 32 * 1024; // 32Ki
    let dir = TempDir::new().unwrap();
    // Create a full 128 KiB file.
    let full =
        generate(&dir, &["--size", "128Ki", "--chunk-size", "32Ki", "--seed", "10", "full.bin"]);
    assert!(full.status.success());

    // Write only the second 32 KiB chunk (at position 32 KiB) into a pre-allocated file.
    let partial_path = dir.path().join("partial.bin");
    fs::write(&partial_path, vec![0u8; 128 * 1024]).unwrap();
    let partial = generate(
        &dir,
        &[
            "--size",
            "32Ki",
            "--chunk-size",
            "32Ki",
            "--seed",
            "10",
            "--position",
            "32Ki",
            "--no-truncate",
            "partial.bin",
        ],
    );
    assert!(partial.status.success(), "{}", String::from_utf8_lossy(&partial.stderr));

    let full_bytes = fs::read(dir.path().join("full.bin")).unwrap();
    let partial_bytes = fs::read(&partial_path).unwrap();

    // The written region [32Ki..64Ki) must match the full file.
    assert_eq!(&full_bytes[CHUNK..2 * CHUNK], &partial_bytes[CHUNK..2 * CHUNK]);
    // The region before the position must still be zeros.
    assert_eq!(&partial_bytes[..CHUNK], vec![0u8; CHUNK].as_slice());
}

#[test]
fn generate_position_not_multiple_of_chunk_is_error() {
    let dir = TempDir::new().unwrap();
    // Create target file first.
    fs::write(dir.path().join("out.bin"), vec![0u8; 64 * 1024]).unwrap();
    // 1Ki = 1024 is not a multiple of 32Ki = 32768.
    let out =
        generate(&dir, &["--size", "32Ki", "--chunk-size", "32Ki", "--position", "1Ki", "out.bin"]);
    assert!(!out.status.success());
}

// ---------------------------------------------------------------------------
// generate – --no-truncate
// ---------------------------------------------------------------------------

#[test]
fn generate_no_truncate_preserves_file_size() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("out.bin");
    // Create a larger pre-existing file (128 KiB).
    fs::write(&path, vec![0xffu8; 128 * 1024]).unwrap();
    // Generate only 32 KiB into it without truncating.
    let out = generate(&dir, &["--size", "32Ki", "--seed", "0", "--no-truncate", "out.bin"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(fs::metadata(&path).unwrap().len(), 128 * 1024);
}

#[test]
fn generate_truncates_file_by_default() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("out.bin");
    fs::write(&path, vec![0u8; 128 * 1024]).unwrap();
    let out = generate(&dir, &["--size", "32Ki", "--seed", "0", "out.bin"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(fs::metadata(&path).unwrap().len(), 32 * 1024);
}

// ---------------------------------------------------------------------------
// generate – error cases
// ---------------------------------------------------------------------------

#[test]
fn generate_no_size_no_existing_file_is_error() {
    let dir = TempDir::new().unwrap();
    // File does not exist and no --size provided.
    let out = generate(&dir, &["--seed", "0", "missing.bin"]);
    assert!(!out.status.success());
}

#[test]
fn generate_position_greater_than_file_size_is_error() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("out.bin"), vec![0u8; 32 * 1024]).unwrap();
    // position 64Ki > file size 32Ki.
    let out = generate(&dir, &["--chunk-size", "32Ki", "--position", "64Ki", "out.bin"]);
    assert!(!out.status.success());
}

// ---------------------------------------------------------------------------
// validate – basic
// ---------------------------------------------------------------------------

#[test]
fn validate_passes_for_freshly_generated_file() {
    let dir = TempDir::new().unwrap();
    let g = generate(&dir, &["--size", "1Mi", "--chunk-size", "1Ki", "--seed", "1234", "out.bin"]);
    assert!(g.status.success());
    let v = validate(&dir, &["--chunk-size", "1Ki", "out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

#[test]
fn validate_read_alias_works() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "32Ki", "--seed", "0", "out.bin"]);
    let out =
        bin().current_dir(dir.path()).args(["read", "--no-progress", "out.bin"]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn validate_emits_checksum_on_stderr() {
    let dir = TempDir::new().unwrap();
    let g = generate(&dir, &["--size", "32Ki", "--seed", "0", "out.bin"]);
    let v = validate(&dir, &["out.bin"]);
    assert!(v.status.success());
    // Both generate and validate must report the same checksum.
    assert_eq!(parse_checksum(&g), parse_checksum(&v));
}

#[test]
fn validate_expected_checksum_matches() {
    let dir = TempDir::new().unwrap();
    let g = generate(&dir, &["--size", "32Ki", "--seed", "0", "out.bin"]);
    let checksum = parse_checksum(&g);
    let v = validate(&dir, &["--expected-checksum", &checksum, "out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

#[test]
fn validate_wrong_expected_checksum_fails() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "32Ki", "--seed", "0", "out.bin"]);
    let v = validate(&dir, &["--expected-checksum", "deadbeef", "out.bin"]);
    assert!(!v.status.success());
}

// ---------------------------------------------------------------------------
// validate – corruption detection
// ---------------------------------------------------------------------------

#[test]
fn validate_detects_single_byte_corruption() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "64Ki", "--chunk-size", "32Ki", "--seed", "42", "out.bin"]);
    let path = dir.path().join("out.bin");
    let mut data = fs::read(&path).unwrap();
    // Flip a byte in the middle of the first chunk (well before the trailing CRC).
    data[1000] ^= 0xff;
    fs::write(&path, &data).unwrap();
    let v = validate(&dir, &["--chunk-size", "32Ki", "out.bin"]);
    assert!(!v.status.success());
}

#[test]
fn validate_detects_corruption_in_second_chunk() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "64Ki", "--chunk-size", "32Ki", "--seed", "42", "out.bin"]);
    let path = dir.path().join("out.bin");
    let mut data = fs::read(&path).unwrap();
    // Flip a byte in the second chunk.
    data[32 * 1024 + 100] ^= 0x01;
    fs::write(&path, &data).unwrap();
    let v = validate(&dir, &["--chunk-size", "32Ki", "out.bin"]);
    assert!(!v.status.success());
}

#[test]
fn validate_detects_truncated_file() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "64Ki", "--chunk-size", "32Ki", "--seed", "1", "out.bin"]);
    let path = dir.path().join("out.bin");
    let mut data = fs::read(&path).unwrap();
    data.truncate(data.len() - 1000);
    fs::write(&path, &data).unwrap();
    // Validate with the original size so it notices the short read.
    let v = validate(&dir, &["--size", "64Ki", "--chunk-size", "32Ki", "out.bin"]);
    assert!(!v.status.success());
}

// ---------------------------------------------------------------------------
// validate – stdin path
// ---------------------------------------------------------------------------

#[test]
fn validate_stdin_passes_for_valid_stream() {
    // Generate to stdout, pipe into validate stdin.
    let generator = bin()
        .args(["generate", "--no-progress", "--size", "64Ki", "--seed", "7"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let val = bin()
        .args(["validate", "--no-progress"])
        .stdin(generator.stdout.unwrap())
        .output()
        .unwrap();
    assert!(val.status.success(), "{}", String::from_utf8_lossy(&val.stderr));
}

#[test]
fn validate_stdin_detects_corruption() {
    // Generate 64 KiB, corrupt a byte, then validate via stdin.
    let mut gen_data = bin()
        .args([
            "generate",
            "--no-progress",
            "--size",
            "64Ki",
            "--chunk-size",
            "32Ki",
            "--seed",
            "8",
        ])
        .output()
        .unwrap()
        .stdout;
    gen_data[500] ^= 0xff;

    let mut val_proc = bin()
        .args(["validate", "--no-progress", "--chunk-size", "32Ki"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    val_proc.stdin.as_mut().unwrap().write_all(&gen_data).unwrap();
    drop(val_proc.stdin.take());
    let status = val_proc.wait().unwrap();
    assert!(!status.success());
}

#[test]
fn validate_stdin_checksum_matches_file_checksum() {
    let dir = TempDir::new().unwrap();
    // Generate to file and capture checksum.
    let g = generate(&dir, &["--size", "32Ki", "--seed", "3", "out.bin"]);
    let file_checksum = parse_checksum(&g);

    // Now validate the same data via stdin.
    let data = fs::read(dir.path().join("out.bin")).unwrap();
    let mut val_proc = bin()
        .args(["validate", "--no-progress"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    val_proc.stdin.as_mut().unwrap().write_all(&data).unwrap();
    drop(val_proc.stdin.take());
    let out = val_proc.wait_with_output().unwrap();
    assert!(out.status.success());
    assert_eq!(parse_checksum(&out), file_checksum);
}

// ---------------------------------------------------------------------------
// validate – --position
// ---------------------------------------------------------------------------

#[test]
fn validate_with_position_skips_earlier_chunks() {
    let dir = TempDir::new().unwrap();
    // Generate 128 KiB.
    generate(&dir, &["--size", "128Ki", "--chunk-size", "32Ki", "--seed", "20", "out.bin"]);
    // Validate starting from the second chunk (position=32Ki, size=96Ki remaining).
    let v = validate(
        &dir,
        &["--size", "96Ki", "--chunk-size", "32Ki", "--position", "32Ki", "out.bin"],
    );
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

#[test]
fn validate_position_not_multiple_of_chunk_is_error() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "128Ki", "--chunk-size", "32Ki", "--seed", "21", "out.bin"]);
    // 1Ki is not a multiple of 32Ki.
    let v = validate(&dir, &["--chunk-size", "32Ki", "--position", "1Ki", "out.bin"]);
    assert!(!v.status.success());
}

// ---------------------------------------------------------------------------
// validate – --size flag
// ---------------------------------------------------------------------------

#[test]
fn validate_explicit_size_smaller_than_file_only_checks_that_region() {
    let dir = TempDir::new().unwrap();
    generate(&dir, &["--size", "128Ki", "--chunk-size", "32Ki", "--seed", "30", "out.bin"]);
    // Validate only the first 64 KiB.
    let v = validate(&dir, &["--size", "64Ki", "--chunk-size", "32Ki", "out.bin"]);
    assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
}

// ---------------------------------------------------------------------------
// generate + validate – round-trip matrix
// ---------------------------------------------------------------------------

/// Table-driven round-trip: (size, chunk_size, seed, jobs).
/// All sizes use binary prefixes (Ki / Mi) for unambiguous byte counts.
#[test]
fn round_trip_various_sizes_and_seeds() {
    struct Case {
        size: &'static str,
        chunk_size: &'static str,
        seed: &'static str,
        jobs: &'static str,
    }
    let cases = [
        Case { size: "1Ki", chunk_size: "1Ki", seed: "0", jobs: "1" },
        Case { size: "32Ki", chunk_size: "32Ki", seed: "1", jobs: "1" },
        // size not a multiple of chunk_size (tail chunk is smaller)
        Case { size: "33Ki", chunk_size: "32Ki", seed: "2", jobs: "1" },
        Case { size: "64Ki", chunk_size: "32Ki", seed: "3", jobs: "2" },
        Case { size: "256Ki", chunk_size: "64Ki", seed: "99", jobs: "3" },
        Case { size: "1Mi", chunk_size: "32Ki", seed: "12345", jobs: "1" },
        // same data, different thread count – checksums must be identical
        Case { size: "1Mi", chunk_size: "32Ki", seed: "12345", jobs: "4" },
    ];

    for case in &cases {
        let dir = TempDir::new().unwrap();
        let label = format!(
            "size={} chunk={} seed={} jobs={}",
            case.size, case.chunk_size, case.seed, case.jobs
        );
        let g = generate(
            &dir,
            &[
                "--size",
                case.size,
                "--chunk-size",
                case.chunk_size,
                "--seed",
                case.seed,
                "--jobs",
                case.jobs,
                "out.bin",
            ],
        );
        assert!(
            g.status.success(),
            "generate failed for {label}: {}",
            String::from_utf8_lossy(&g.stderr)
        );

        let v = validate(&dir, &["--chunk-size", case.chunk_size, "out.bin"]);
        assert!(
            v.status.success(),
            "validate failed for {label}: {}",
            String::from_utf8_lossy(&v.stderr)
        );

        assert_eq!(parse_checksum(&g), parse_checksum(&v), "checksum mismatch for {label}");
    }
}

// ---------------------------------------------------------------------------
// CLI argument validation
// ---------------------------------------------------------------------------

#[test]
fn no_subcommand_exits_nonzero() {
    let out = bin().output().unwrap();
    assert!(!out.status.success());
}

#[test]
fn unknown_flag_exits_nonzero() {
    let out = bin().args(["generate", "--does-not-exist"]).output().unwrap();
    assert!(!out.status.success());
}

#[test]
fn position_requires_file_argument() {
    // --position without a file path is rejected by clap (requires="file").
    let out = bin()
        .args(["generate", "--no-progress", "--size", "32Ki", "--position", "0"])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn help_flag_succeeds() {
    let out = bin().arg("--help").output().unwrap();
    assert!(out.status.success());
}

#[test]
fn generate_help_flag_succeeds() {
    let out = bin().args(["generate", "--help"]).output().unwrap();
    assert!(out.status.success());
}

#[test]
fn validate_help_flag_succeeds() {
    let out = bin().args(["validate", "--help"]).output().unwrap();
    assert!(out.status.success());
}
