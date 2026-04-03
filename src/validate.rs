use std::alloc::{Layout, alloc_zeroed};
use std::collections::VecDeque;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
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
use compio::io::AsyncReadAtExt as _;
use compio_runtime::RuntimeBuilder;
use crc32fast::Hasher;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use human_units::{FormatDuration, FormatSize as _};
use itertools::Itertools as _;
use log::{debug, info};
use nix::libc;
use parse_size::parse_size;

use crate::Progress;
use crate::cli::CommonArgs;
use crate::{default_io_depth, read_exact_or_eof, read_file_size};

#[derive(Clone)]
struct StreamParams {
    position: u64,
    stream_size: u64,
    chunk_size: usize,
}

#[derive(Clone)]
struct ThreadWork {
    thread_index: u64,
    chunks_per_thread: u64,
    num_chunks: u64,
}

/// Validate a random stream
///
/// If the input is a regular file or a block device, the data will be read
/// from multiple locations in parallel to maximize the throughput.
#[derive(Args, Debug)]
#[command(alias = "read")]
pub struct ValidateArgs {
    /// The input file
    #[arg()]
    pub file: Option<PathBuf>,

    /// The stream position
    #[clap(short, long, default_value = "0", value_parser=|s: &str| parse_size(s))]
    pub position: u64,

    /// The expected checksum
    ///
    /// Generates an error if it doesn't match the stream checksum
    #[clap(short, long)]
    pub expected_checksum: Option<String>,

    #[clap(flatten)]
    pub common: CommonArgs,
}

pub fn validate(args: &ValidateArgs) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = args.common.chunk_size as usize;

    let (bytes_validated, checksum) = if let Some(file) = &args.file {
        let stream_size = resolve_stream_size(args, file)?;
        let mut pb = Progress::new(Some(stream_size), args.common.no_progress)?;

        debug!("position: {}", args.position);
        debug!("stream size: {stream_size}");
        debug!("chunk size: {chunk_size}");

        validate_from_file(args, file, stream_size, chunk_size, &mut pb)?
    } else {
        let mut pb = Progress::new(None, args.common.no_progress)?;

        debug!("position: {}", args.position);
        debug!(
            "stream size: {}",
            if let Some(size) = args.common.size { size.to_string() } else { "∞".to_string() }
        );
        debug!("chunk size: {chunk_size}");

        validate_from_stdin(args, chunk_size, &mut pb)?
    };

    if let Some(expected_checksum) = &args.expected_checksum
        && expected_checksum != &format!("{checksum:08x}")
    {
        return Err(anyhow!(
            "Checksum mismatch. It was expected to be {expected_checksum}, but is actually {checksum:x}"
        ));
    }
    info!("checksum: {checksum:08x}");
    debug!("read bytes: {bytes_validated}");
    debug!(
        "throughput: {}/s",
        ((bytes_validated as f32 / start.elapsed().as_micros() as f32 * 1000000.0) as usize)
            .format_size()
    );
    debug!("run in {}", start.elapsed().format_duration());
    Ok(0)
}

fn resolve_stream_size(args: &ValidateArgs, file: &Path) -> anyhow::Result<u64> {
    if let Some(size) = &args.common.size {
        return Ok(*size);
    }
    let size = read_file_size(file)?;
    if args.position > size {
        return Err(anyhow!("The position {} is greater than the file size {size}", args.position));
    }
    Ok(size - args.position)
}

/// Allocate a `Vec<u8>` of `size` bytes with 512-byte alignment.
///
/// Required for O_DIRECT I/O: the kernel rejects buffers whose address is not
/// aligned to the logical block size (512 bytes on virtually all drives).
fn alloc_aligned(size: usize) -> Vec<u8> {
    unsafe {
        let layout = Layout::from_size_align(size, 512).expect("invalid layout");
        let ptr = alloc_zeroed(layout);
        assert!(!ptr.is_null(), "allocation failed");
        Vec::from_raw_parts(ptr, size, size)
    }
}

fn validate_from_file(
    args: &ValidateArgs,
    file: &Path,
    stream_size: u64,
    chunk_size: usize,
    pb: &mut Option<Progress>,
) -> anyhow::Result<(u64, u32)> {
    let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
    let io_depth = args.common.io_depth.unwrap_or_else(|| default_io_depth(num_threads));
    let io_depth_per_thread = ((io_depth as usize).div_ceil(num_threads)).max(1);
    let direct = args.common.direct;
    debug!("number of threads: {num_threads}");
    debug!("io_depth: {io_depth} ({io_depth_per_thread} per thread)");
    debug!("direct I/O: {direct}");

    let num_chunks = stream_size.div_ceil(chunk_size as u64);
    let chunks_per_thread = num_chunks.div_ceil(num_threads as u64);

    let (tx, rx) = mpsc::channel::<u64>();
    let cancel: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    let stream = StreamParams { position: args.position, stream_size, chunk_size };

    let handles: Vec<_> = (0..num_threads as u64)
        .map(|thread_index| {
            let file = file.to_path_buf();
            let tx = tx.clone();
            let cancel = cancel.clone();
            let stream = stream.clone();
            let work = ThreadWork { thread_index, chunks_per_thread, num_chunks };

            thread::spawn(move || -> anyhow::Result<(u64, Hasher)> {
                let mut proactor = ProactorBuilder::new();
                proactor.capacity(io_depth_per_thread as u32);
                let runtime = RuntimeBuilder::new().with_proactor(proactor).build()?;
                debug!("validate thread {thread_index}: compio driver = {:?}", runtime.driver_type());
                let result = runtime.block_on(read_chunk_range(
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
    let read_bytes = thread_data.iter().map(|(b, _)| b).sum();
    let mut hasher = thread_data[0].1.clone();
    for (_, partial) in &thread_data[1..] {
        hasher.combine(partial);
    }

    Ok((read_bytes, hasher.finalize()))
}

type ChunkFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = anyhow::Result<(u64, u64, Hasher, Vec<u8>)>>>,
>;

async fn read_chunk_range(
    file: &Path,
    stream: &StreamParams,
    work: &ThreadWork,
    io_depth: usize,
    direct: bool,
    tx: &mpsc::Sender<u64>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, Hasher)> {
    let open_flags = if direct { libc::O_DIRECT } else { 0 };
    let file = Rc::new(OpenOptions::new().read(true).custom_flags(open_flags).open(file).await?);
    let start_chunk = work.thread_index * work.chunks_per_thread;
    let end_chunk = ((work.thread_index + 1) * work.chunks_per_thread).min(work.num_chunks);

    // Nothing to do if this thread has no chunks (more threads than chunks).
    if start_chunk >= end_chunk {
        return Ok((0, Hasher::new()));
    }

    // Pre-allocate io_depth buffers (chunk_size each) to avoid per-chunk allocation.
    // With O_DIRECT, buffers must be 512-byte aligned; use alloc_aligned() for that.
    // Each in-flight task owns one buffer; completed tasks return it to the free list.
    let mut free_buffers: VecDeque<Vec<u8>> =
        (0..io_depth).map(|_| alloc_aligned(stream.chunk_size)).collect();

    // in_flight futures return (chunk_index, read_size, hasher, buffer) on completion.
    let mut in_flight: FuturesUnordered<ChunkFuture> = FuturesUnordered::new();

    // results[i] stores (read_size, hasher) for chunk start_chunk+i, filled as futures complete.
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

            let buffer = free_buffers.pop_front().expect("checked non-empty above");
            let file = Rc::clone(&file);
            let tx = tx.clone();
            let stream = stream.clone();

            in_flight.push(Box::pin(async move {
                let read_size = (stream.stream_size - chunk * stream.chunk_size as u64)
                    .min(stream.chunk_size as u64) as usize;
                let offset = stream.position + chunk * stream.chunk_size as u64;

                // Use .slice(0..read_size) so compio sees the correct buffer length
                // without reallocating (buf_capacity() of Vec = capacity(), not len()).
                // into_inner() recovers the full Vec after the read.
                let buf_result = file.read_exact_at(buffer.slice(0..read_size), offset).await;
                buf_result.0?;
                let buffer = buf_result.1.into_inner();

                let mut thread_hasher = Hasher::new();
                validate_chunk(chunk, &buffer[..read_size], &mut thread_hasher)?;

                let _ = tx.send(read_size as u64);

                Ok((chunk, read_size as u64, thread_hasher, buffer))
            }));
        }

        if in_flight.is_empty() {
            break;
        }

        // Wait for any one future to complete.
        match in_flight.next().await {
            Some(Ok((chunk, read_size, hasher, buffer))) => {
                free_buffers.push_back(buffer);
                let idx = (chunk - start_chunk) as usize;
                results[idx] = Some((read_size, hasher));
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

fn validate_from_stdin(
    args: &ValidateArgs,
    chunk_size: usize,
    pb: &mut Option<Progress>,
) -> anyhow::Result<(u64, u32)> {
    debug!("number of threads: 1");
    // discard the first values up to position
    io::copy(&mut io::stdin().take(args.position), &mut io::sink())?;
    let mut buffer = vec![0; chunk_size];
    let mut stream_size: u64 = 0;
    let mut chunk: u64 = 0;
    let mut hasher = Hasher::new();
    while args.common.size.map(|s| stream_size < s).unwrap_or(true) {
        let read_size = read_exact_or_eof(&mut io::stdin(), &mut buffer)?;
        if read_size == 0 {
            // End of input stream (EOF)
            break;
        }
        validate_chunk(chunk, &buffer[..read_size], &mut hasher)?;
        stream_size += read_size as u64;
        chunk += 1;
        if let Some(p) = pb {
            p.tick(stream_size);
        }
    }
    Ok((stream_size, hasher.finalize()))
}

pub fn validate_chunk(chunk: u64, buffer: &[u8], global_hasher: &mut Hasher) -> anyhow::Result<()> {
    let mut hasher = Hasher::new();
    let read_size = buffer.len();
    if read_size >= 4 {
        hasher.update(&buffer[..read_size - 4]);
        global_hasher.combine(&hasher);
        let stream_checksum =
            u32::from_le_bytes(buffer[read_size - 4..read_size].try_into().unwrap());
        let checksum = hasher.finalize();
        if stream_checksum != checksum {
            return Err(anyhow!(
                "Invalid checksum at chunk {chunk}. Expected {:08x}, found {:08x}.",
                stream_checksum,
                checksum
            ));
        }
    } else {
        global_hasher.update(&buffer[..read_size]);
        for v in buffer[..read_size].iter() {
            if *v != 0 {
                return Err(anyhow!("Invalid non-zero value at the end of the file"));
            }
        }
    }
    Ok(())
}
