use crc32fast::Hasher;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::SeedableRng as _;
use rand_pcg::Pcg64Mcg;
use randstream::generate::generate_chunk;
use randstream::validate::validate_chunk;
use std::process::{Command, Stdio};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Binary path helper
// ---------------------------------------------------------------------------

/// Run randstream binary with given args.
fn randstream(args: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_randstream"));
    // Suppress the `checksum: …` info line and any other stderr output.
    cmd.stderr(Stdio::null());
    cmd.args(args);
    cmd
}

// ---------------------------------------------------------------------------
// Microbenchmarks — generate_chunk
// ---------------------------------------------------------------------------

fn bench_generate_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("generate_chunk");

    for chunk_size in [1024, 32768, 131072].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", chunk_size)),
            chunk_size,
            |b, &chunk_size| {
                let mut rng = Pcg64Mcg::seed_from_u64(0);
                let mut buffer = vec![0u8; chunk_size];
                let mut hasher = Hasher::new();

                b.iter(|| {
                    generate_chunk(&mut rng, &mut buffer, chunk_size, &mut hasher);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Microbenchmarks — validate_chunk
// ---------------------------------------------------------------------------

fn bench_validate_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_chunk");

    for chunk_size in [1024, 32768, 131072].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", chunk_size)),
            chunk_size,
            |b, &chunk_size| {
                // Pre-generate a valid chunk so validate_chunk never errors.
                let mut rng = Pcg64Mcg::seed_from_u64(0);
                let mut buffer = vec![0u8; chunk_size];
                let mut hasher = Hasher::new();
                generate_chunk(&mut rng, &mut buffer, chunk_size, &mut hasher);
                let chunk_index: u64 = 0;

                b.iter(|| {
                    let mut h = Hasher::new();
                    validate_chunk(black_box(chunk_index), black_box(&buffer), &mut h).unwrap();
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// End-to-end benchmarks
// ---------------------------------------------------------------------------

const E2E_SIZE: &str = "1Gi";
const SEED: &str = "42";

fn bench_generate_file_single_thread(c: &mut Criterion) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let out = dir.path().join("out.bin");

    c.bench_function("generate_file_single_thread", |b| {
        b.iter(|| {
            let status = randstream(&[
                "generate",
                "--no-progress",
                "--size",
                E2E_SIZE,
                "--seed",
                SEED,
                "--jobs",
                "1",
            ])
            .arg(&out)
            .status()
            .expect("failed to spawn randstream");
            assert!(status.success(), "randstream generate failed");
        });
    });
}

fn bench_generate_file_parallel(c: &mut Criterion) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let out = dir.path().join("out.bin");

    c.bench_function("generate_file_parallel", |b| {
        b.iter(|| {
            let status =
                randstream(&["generate", "--no-progress", "--size", E2E_SIZE, "--seed", SEED])
                    .arg(&out)
                    .status()
                    .expect("failed to spawn randstream");
            assert!(status.success(), "randstream generate failed");
        });
    });
}

fn bench_generate_stdout(c: &mut Criterion) {
    c.bench_function("generate_stdout", |b| {
        b.iter(|| {
            let status =
                randstream(&["generate", "--no-progress", "--size", E2E_SIZE, "--seed", SEED])
                    .stdout(Stdio::null())
                    .status()
                    .expect("failed to spawn randstream");
            assert!(status.success(), "randstream generate failed");
        });
    });
}

fn bench_validate_file(c: &mut Criterion) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let out = dir.path().join("out.bin");

    // Setup: generate the file once before timing starts.
    let status = randstream(&[
        "generate",
        "--no-progress",
        "--size",
        E2E_SIZE,
        "--seed",
        SEED,
        "--jobs",
        "1",
    ])
    .arg(&out)
    .status()
    .expect("failed to spawn randstream generate (setup)");
    assert!(status.success(), "setup generate failed");

    c.bench_function("validate_file", |b| {
        b.iter(|| {
            let status = randstream(&["validate", "--no-progress"])
                .arg(&out)
                .status()
                .expect("failed to spawn randstream validate");
            assert!(status.success(), "randstream validate failed");
        });
    });
}

// ---------------------------------------------------------------------------
// Criterion groups and main
// ---------------------------------------------------------------------------

criterion_group!(
    name = microbench;
    config = Criterion::default();
    targets = bench_generate_chunk, bench_validate_chunk
);

criterion_group!(
    name = e2e;
    config = Criterion::default().sample_size(10);
    targets =
        bench_generate_file_single_thread,
        bench_generate_file_parallel,
        bench_generate_stdout,
        bench_validate_file
);

criterion_main!(microbench, e2e);
