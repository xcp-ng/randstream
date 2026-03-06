use std::process::{Command, Stdio};

use crc32fast::Hasher;
use divan::Bencher;
use rand::SeedableRng as _;
use rand_pcg::Pcg64Mcg;
use randstream::generate::generate_chunk;
use randstream::validate::validate_chunk;
use tempfile::TempDir;

fn main() {
    divan::main();
}

// ---------------------------------------------------------------------------
// Binary path helper
// ---------------------------------------------------------------------------

/// The bench binary lives at `target/<profile>/deps/throughput-<hash>`.
/// The randstream binary lives at `target/<profile>/randstream`.
fn bin() -> Command {
    let exe = std::env::current_exe().unwrap();
    let profile_dir = exe.parent().unwrap().parent().unwrap();
    let bin = profile_dir.join("randstream");
    let mut cmd = Command::new(bin);
    // Suppress the `checksum: …` info line and any other stderr output.
    cmd.stderr(Stdio::null());
    cmd
}

// ---------------------------------------------------------------------------
// Microbenchmarks — generate_chunk
// ---------------------------------------------------------------------------

#[divan::bench(consts = [1024, 32768, 131072])]
fn bench_generate_chunk<const N: usize>(bencher: Bencher) {
    let mut rng = Pcg64Mcg::seed_from_u64(0);
    let mut buffer = vec![0u8; N];
    let mut hasher = Hasher::new();

    bencher.bench_local(|| {
        generate_chunk(&mut rng, &mut buffer, N, &mut hasher);
    });
}

// ---------------------------------------------------------------------------
// Microbenchmarks — validate_chunk
// ---------------------------------------------------------------------------

#[divan::bench(consts = [1024, 32768, 131072])]
fn bench_validate_chunk<const N: usize>(bencher: Bencher) {
    // Pre-generate a valid chunk so validate_chunk never errors.
    let mut rng = Pcg64Mcg::seed_from_u64(0);
    let mut buffer = vec![0u8; N];
    let mut hasher = Hasher::new();
    generate_chunk(&mut rng, &mut buffer, N, &mut hasher);
    let chunk_index: u64 = 0;

    bencher.bench_local(|| {
        let mut h = Hasher::new();
        validate_chunk(chunk_index, &buffer, &mut h).unwrap();
    });
}

// ---------------------------------------------------------------------------
// End-to-end benchmarks
// ---------------------------------------------------------------------------

const E2E_SIZE: &str = "1Gi";
const SEED: &str = "42";

fn e2e_dir() -> TempDir {
    TempDir::new().expect("failed to create temp dir")
}

#[divan::bench(sample_count = 10)]
fn bench_generate_file_single_thread(bencher: Bencher) {
    let dir = e2e_dir();
    let out = dir.path().join("out.bin");

    bencher.bench_local(|| {
        let status = bin()
            .args(["generate", "--no-progress", "--size", E2E_SIZE, "--seed", SEED, "--jobs", "1"])
            .arg(&out)
            .status()
            .expect("failed to spawn randstream");
        assert!(status.success(), "randstream generate failed");
    });
}

#[divan::bench(sample_count = 10)]
fn bench_generate_file_parallel(bencher: Bencher) {
    let dir = e2e_dir();
    let out = dir.path().join("out.bin");

    bencher.bench_local(|| {
        let status = bin()
            .args(["generate", "--no-progress", "--size", E2E_SIZE, "--seed", SEED])
            .arg(&out)
            .status()
            .expect("failed to spawn randstream");
        assert!(status.success(), "randstream generate failed");
    });
}

#[divan::bench(sample_count = 10)]
fn bench_generate_stdout(bencher: Bencher) {
    bencher.bench_local(|| {
        let status = bin()
            .args(["generate", "--no-progress", "--size", E2E_SIZE, "--seed", SEED])
            .stdout(Stdio::null())
            .status()
            .expect("failed to spawn randstream");
        assert!(status.success(), "randstream generate failed");
    });
}

#[divan::bench(sample_count = 10)]
fn bench_validate_file(bencher: Bencher) {
    let dir = e2e_dir();
    let out = dir.path().join("out.bin");

    // Setup: generate the file once before timing starts.
    let status = bin()
        .args(["generate", "--no-progress", "--size", E2E_SIZE, "--seed", SEED, "--jobs", "1"])
        .arg(&out)
        .status()
        .expect("failed to spawn randstream generate (setup)");
    assert!(status.success(), "setup generate failed");

    bencher.bench_local(|| {
        let status = bin()
            .args(["validate", "--no-progress"])
            .arg(&out)
            .status()
            .expect("failed to spawn randstream validate");
        assert!(status.success(), "randstream validate failed");
    });
}
