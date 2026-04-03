use std::alloc::{Layout, alloc_zeroed};
use std::io::{self, Write};
use std::path::PathBuf;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use anyhow::anyhow;
use clap::Args;
use compio::buf::IntoInner as _;
use compio::buf::IoBuf as _;
use compio::driver::ProactorBuilder;
use compio::fs::OpenOptions;
use compio::io::AsyncWriteAtExt as _;
use compio_runtime::RuntimeBuilder;
use crc32fast::Hasher;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use human_units::{FormatDuration as _, FormatSize as _};
use itertools::Itertools as _;
use log::{debug, info};
use nix::libc;
use parse_size::parse_size;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;

use crate::Progress;
use crate::cli::CommonArgs;
use crate::{default_io_depth, read_file_size};

#[derive(Clone)]
struct StreamParams {
    seed: u64,
    position: u64,
    stream_size: u64,
    chunk_size: usize,
    buffer_size: usize,
}

#[derive(Clone)]
struct ThreadWork {
    thread_index: u64,
    chunks_per_thread: u64,
    num_chunks: u64,
}

/// Generate a random stream
#[derive(Args, Debug)]
#[command(alias = "write")]
pub struct GenerateArgs {
    /// The output file
    #[arg()]
    pub file: Option<PathBuf>,

    /// The stream position
    #[clap(short, long, default_value = "0", value_parser=|s: &str| parse_size(s), requires="file")]
    pub position: u64,

    /// The random generator seed
    #[clap(short = 'S', long, default_value = "0")]
    pub seed: u64,

    /// Don't truncate the file
    #[clap(short = 't', long)]
    pub no_truncate: bool,

    #[clap(flatten)]
    pub common: CommonArgs,
}

pub fn generate(args: &GenerateArgs) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = args.common.chunk_size as usize;
    // we need to write a multiple of 64 bits to be able to use advance()
    let buffer_size = chunk_size.div_ceil(8) * 8;
    let stream_size = resolve_stream_size(args)?;
    let mut pb = Progress::new(Some(stream_size), args.common.no_progress)?;

    debug!("position: {}", args.position);
    debug!("stream size: {stream_size}");
    debug!("chunk size: {chunk_size}");
    debug!("seed: {}", args.seed);

    let (bytes_generated, checksum) = if let Some(file) = &args.file {
        generate_to_file(args, file, stream_size, chunk_size, buffer_size, &mut pb)?
    } else {
        generate_to_stdout(args, stream_size, chunk_size, &mut pb)?
    };

    info!("checksum: {checksum:08x}");
    debug!("written bytes: {bytes_generated}");
    debug!(
        "throughput: {}/s",
        ((bytes_generated as f32 / start.elapsed().as_micros() as f32 * 1000000.0) as usize)
            .format_size()
    );
    debug!("run in {}", start.elapsed().format_duration());
    Ok(0)
}

fn resolve_stream_size(args: &GenerateArgs) -> anyhow::Result<u64> {
    if let Some(size) = &args.common.size {
        return Ok(*size);
    }
    if let Some(file) = &args.file
        && file.exists()
    {
        let size = read_file_size(file)?;
        if args.position > size {
            return Err(anyhow!(
                "The position {} is greater than the file size {size}",
                args.position
            ));
        }
        return Ok(size - args.position);
    }
    Err(anyhow!("Size can't be determined. Use --size to provide a stream size."))
}

/// Allocate a `Vec<u8>` of `size` bytes with 512-byte alignment.
///
/// Required for O_DIRECT I/O: the kernel rejects buffers whose address is not
/// aligned to the logical block size (512 bytes on virtually all drives).
/// Regular `vec![0u8; n]` provides no alignment guarantee.
fn alloc_aligned(size: usize) -> Vec<u8> {
    // SAFETY: Layout is valid (size > 0 and align is a power of two).
    // We immediately wrap the pointer in a Vec with the correct length/capacity,
    // ensuring it will be freed via the same allocator.
    unsafe {
        let layout = Layout::from_size_align(size, 512).expect("invalid layout");
        let ptr = alloc_zeroed(layout);
        assert!(!ptr.is_null(), "allocation failed");
        Vec::from_raw_parts(ptr, size, size)
    }
}

fn generate_to_file(
    args: &GenerateArgs,
    file: &PathBuf,
    stream_size: u64,
    chunk_size: usize,
    buffer_size: usize,
    pb: &mut Option<Progress>,
) -> anyhow::Result<(u64, u32)> {
    // Pre-create the file synchronously and set its size before opening async handles
    let f = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(file)?;
    if file.is_file() {
        let end_position = stream_size + args.position;
        if end_position > f.metadata()?.len() || !args.no_truncate {
            f.set_len(end_position)?;
        }
    }
    drop(f);

    let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
    let io_depth = args.common.io_depth.unwrap_or_else(|| default_io_depth(num_threads));
    let io_depth_per_thread = ((io_depth as usize).div_ceil(num_threads)).max(1);
    let direct = args.common.direct;
    debug!("number of threads: {num_threads}");
    debug!("io_depth: {io_depth} ({io_depth_per_thread} per thread)");
    debug!("direct I/O: {direct}");

    let num_chunks = stream_size.div_ceil(chunk_size as u64);
    let chunks_per_thread = num_chunks.div_ceil(num_threads as u64);

    let stream = StreamParams {
        seed: args.seed,
        position: args.position,
        stream_size,
        chunk_size,
        buffer_size,
    };

    let (tx, rx) = mpsc::channel::<u64>();
    let cancel: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..num_threads as u64)
        .map(|thread_index| {
            let file = file.clone();
            let tx = tx.clone();
            let cancel = cancel.clone();
            let stream = stream.clone();
            let work = ThreadWork { thread_index, chunks_per_thread, num_chunks };

            thread::spawn(move || -> anyhow::Result<(u64, Hasher)> {
                let mut proactor = ProactorBuilder::new();
                proactor.capacity(io_depth_per_thread as u32);
                let runtime = RuntimeBuilder::new().with_proactor(proactor).build()?;
                debug!("generate thread {thread_index}: compio driver = {:?}", runtime.driver_type());
                let result = runtime.block_on(write_chunk_range(
                    &file,
                    &stream,
                    &work,
                    io_depth_per_thread,
                    direct,
                    &tx,
                    &cancel,
                ));
                if result.is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
                result
            })
        })
        .collect();

    // Drive progress from the main thread while workers run
    drop(tx);
    let mut total_bytes: u64 = 0;
    if let Some(p) = pb {
        while let Ok(bytes) = rx.recv() {
            total_bytes += bytes;
            p.tick(total_bytes);
        }
        p.finish();
    } else {
        while rx.recv().is_ok() {}
    }

    let thread_data: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;
    let written_bytes = thread_data.iter().map(|(b, _)| b).sum();
    let mut hasher = thread_data[0].1.clone();
    for (_, partial) in &thread_data[1..] {
        hasher.combine(partial);
    }

    Ok((written_bytes, hasher.finalize()))
}

type ChunkFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = anyhow::Result<(u64, u64, Hasher, Vec<u8>)>>>,
>;

async fn write_chunk_range(
    file: &PathBuf,
    stream: &StreamParams,
    work: &ThreadWork,
    io_depth: usize,
    direct: bool,
    tx: &mpsc::Sender<u64>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, Hasher)> {
    let open_flags = if direct { libc::O_DIRECT } else { 0 };
    let file = Rc::new(
        OpenOptions::new().write(true).custom_flags(open_flags).open(file).await?,
    );
    let start_chunk = work.thread_index * work.chunks_per_thread;
    let end_chunk = ((work.thread_index + 1) * work.chunks_per_thread).min(work.num_chunks);

    // Nothing to do if this thread has no chunks (more threads than chunks).
    if start_chunk >= end_chunk {
        return Ok((0, Hasher::new()));
    }

    // Pre-allocate io_depth buffers (buffer_size each) to avoid per-chunk allocation.
    // With O_DIRECT, buffers must be 512-byte aligned; use alloc_aligned() for that.
    // Each in-flight task owns one buffer; completed tasks return it to the free list.
    let mut free_buffers: VecDeque<Vec<u8>> =
        (0..io_depth).map(|_| alloc_aligned(stream.buffer_size)).collect();

    // in_flight: (chunk_index, future result)
    // We keep a Vec so we can accumulate results in chunk order for deterministic hasher.combine().
    let mut in_flight: FuturesUnordered<ChunkFuture> = FuturesUnordered::new();

    // results[i] stores the (write_size, hasher) for chunk start_chunk+i, filled in as futures complete.
    let num_chunks = (end_chunk - start_chunk) as usize;
    let mut results: Vec<Option<(u64, Hasher)>> = vec![None; num_chunks];

    let mut next_chunk = start_chunk;

    loop {
        // Launch new tasks as long as we have free buffers and pending chunks.
        while next_chunk < end_chunk && !free_buffers.is_empty() {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            let chunk = next_chunk;
            next_chunk += 1;

            let mut buffer = free_buffers.pop_front().expect("checked non-empty above");
            let file = Rc::clone(&file);
            let tx = tx.clone();
            let stream = stream.clone();

            in_flight.push(Box::pin(async move {
                let mut rng = Pcg64Mcg::seed_from_u64(stream.seed);
                // advance() is O(log2(128)) — effectively free
                let advance_amount = chunk
                    .checked_mul(stream.buffer_size as u64)
                    .ok_or_else(|| {
                        anyhow!("arithmetic overflow: chunk * buffer_size exceeds u64 max")
                    })?
                    / 8;
                rng.advance(advance_amount.into());

                let mut thread_hasher = Hasher::new();
                let mut local_hasher = Hasher::new();
                let write_size = (stream.stream_size - chunk * stream.chunk_size as u64)
                    .min(stream.chunk_size as u64) as usize;
                generate_chunk(
                    &mut rng,
                    &mut buffer,
                    write_size,
                    &mut thread_hasher,
                    &mut local_hasher,
                );

                let offset = stream.position + chunk * stream.chunk_size as u64;
                // Use .slice(0..write_size) so compio sees the correct buffer length
                // without truncating/reallocating. into_inner() recovers the full Vec.
                let buf_result =
                    (&*file).write_all_at(buffer.slice(0..write_size), offset).await;
                buf_result.0?;
                let buffer = buf_result.1.into_inner();

                let _ = tx.send(write_size as u64);
                Ok((chunk, write_size as u64, thread_hasher, buffer))
            }));
        }

        if in_flight.is_empty() {
            break;
        }

        // Wait for any one future to complete.
        match in_flight.next().await {
            Some(Ok((chunk, write_size, hasher, buffer))) => {
                free_buffers.push_back(buffer);
                let idx = (chunk - start_chunk) as usize;
                results[idx] = Some((write_size, hasher));
            }
            Some(Err(e)) => {
                cancel.store(true, Ordering::Relaxed);
                return Err(e);
            }
            None => break,
        }
    }

    // Combine results in chunk order so hasher.combine() is deterministic.
    let mut total_bytes: u64 = 0;
    let mut combined_hasher = Hasher::new();
    for (bytes, hasher) in results.into_iter().flatten() {
        total_bytes += bytes;
        combined_hasher.combine(&hasher);
    }

    Ok((total_bytes, combined_hasher))
}

fn generate_to_stdout(
    args: &GenerateArgs,
    stream_size: u64,
    chunk_size: usize,
    pb: &mut Option<Progress>,
) -> anyhow::Result<(u64, u32)> {
    debug!("number of threads: 1");
    let mut writer = io::stdout();
    let mut rng = Pcg64Mcg::seed_from_u64(args.seed);
    let buffer_size = chunk_size.div_ceil(8) * 8;
    let mut buffer = vec![0u8; buffer_size];
    let mut bytes_generated: u64 = 0;
    let mut hasher = Hasher::new();
    let mut local_hasher = Hasher::new();
    while bytes_generated < stream_size {
        let write_size = (stream_size - bytes_generated).min(chunk_size as u64) as usize;
        generate_chunk(&mut rng, &mut buffer, write_size, &mut hasher, &mut local_hasher);
        writer.write_all(&buffer[..write_size])?;
        bytes_generated += write_size as u64;
        if let Some(p) = pb {
            p.tick(bytes_generated);
        }
    }
    Ok((bytes_generated, hasher.finalize()))
}

pub fn generate_chunk(
    rng: &mut Pcg64Mcg,
    buffer: &mut [u8],
    write_size: usize,
    global_hasher: &mut Hasher,
    local_hasher: &mut Hasher,
) {
    use rand::Rng as _;
    if write_size >= 4 {
        rng.fill_bytes(&mut buffer[..]);
        local_hasher.reset();
        local_hasher.update(&buffer[..write_size - 4]);
        global_hasher.combine(local_hasher);
        let checksum_bytes = local_hasher.clone().finalize().to_le_bytes();
        let end_slice = &mut buffer[write_size - 4..write_size];
        end_slice.copy_from_slice(&checksum_bytes);
    } else {
        // not enough room to fit the checksum, just push some zeros in there
        buffer[..write_size].fill(0);
        global_hasher.update(&buffer[..write_size]);
    }
}
