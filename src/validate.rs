use anyhow::anyhow;
use clap::Args;
use crc32fast::Hasher;
use itertools::Itertools as _;
use log::{debug, info};
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

use crate::cli::CommonArgs;
use crate::{Progress, log_metrics, read_exact_or_eof, read_file_size, receive_progress};

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

pub fn validate(args: &ValidateArgs, cancel: Arc<AtomicBool>) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = args.common.chunk_size as usize;

    let (bytes_validated, checksum) = if let Some(file) = &args.file {
        let stream_size = resolve_stream_size(args, file)?;
        let mut pb = Progress::new(Some(stream_size), args.common.no_progress)?;

        debug!("position: {}", args.position);
        debug!("stream size: {stream_size}");
        debug!("chunk size: {chunk_size}");

        validate_from_file(args, file, stream_size, chunk_size, &mut pb, &cancel)?
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

    // Check if operation was cancelled
    if cancel.load(Ordering::Relaxed) {
        log_metrics(start, bytes_validated, "read bytes");
        return Ok(130);
    }

    if let Some(expected_checksum) = &args.expected_checksum
        && expected_checksum != &format!("{checksum:08x}")
    {
        return Err(anyhow!(
            "Checksum mismatch. It was expected to be {expected_checksum}, but is actually {checksum:x}"
        ));
    }
    info!("checksum: {checksum:08x}");
    log_metrics(start, bytes_validated, "read bytes");
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

fn validate_from_file(
    args: &ValidateArgs,
    file: &Path,
    stream_size: u64,
    chunk_size: usize,
    pb: &mut Option<Progress>,
    cancel: &Arc<AtomicBool>,
) -> anyhow::Result<(u64, u32)> {
    let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
    debug!("number of threads: {num_threads}");

    let num_chunks = stream_size.div_ceil(chunk_size as u64);
    let chunks_per_thread = num_chunks.div_ceil(num_threads as u64);
    let (tx, rx) = mpsc::channel::<u64>();

    let handles: Vec<_> = (0..num_threads as u64)
        .map(|i| {
            let file = file.to_path_buf();
            let tx = tx.clone();
            let cancel = cancel.clone();
            let position = args.position;
            thread::spawn(move || -> anyhow::Result<_> {
                let result = validate_chunk_range(
                    &file,
                    chunk_size,
                    i,
                    chunks_per_thread,
                    num_chunks,
                    position,
                    &tx,
                    &cancel,
                );
                if result.is_err() {
                    // tell the other threads to stop
                    cancel.store(true, Ordering::Relaxed);
                }
                result
            })
        })
        .collect();

    receive_progress(pb, &rx, tx);
    let thread_data: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;

    let read_bytes = thread_data.iter().map(|(b, _)| b).sum();
    let thread_hashers: Vec<_> = thread_data.iter().map(|(_, h)| h).collect();
    let mut hasher = thread_hashers[0].clone();
    for partial_hasher in thread_hashers[1..].iter() {
        hasher.combine(partial_hasher);
    }

    Ok((read_bytes, hasher.finalize()))
}

#[allow(clippy::too_many_arguments)]
fn validate_chunk_range(
    file: &Path,
    chunk_size: usize,
    thread_index: u64,
    chunks_per_thread: u64,
    num_chunks: u64,
    position: u64,
    tx: &mpsc::Sender<u64>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, Hasher)> {
    let mut file = File::open(file)?;
    let mut thread_hasher = Hasher::new();
    let start_chunk = thread_index * chunks_per_thread;
    let end_chunk = ((thread_index + 1) * chunks_per_thread).min(num_chunks);
    let mut buffer = vec![0; chunk_size];
    file.seek(io::SeekFrom::Start(position + start_chunk * chunk_size as u64))?;
    let mut total_read_size: u64 = 0;
    let mut progress_bytes: u64 = 0;
    for chunk in start_chunk..end_chunk {
        let read_size = read_exact_or_eof(&mut file, &mut buffer)?;
        validate_chunk(chunk, &buffer[..read_size], &mut thread_hasher)?;
        total_read_size += read_size as u64;
        progress_bytes += read_size as u64;
        if chunk % 100 == 0 {
            tx.send(progress_bytes)?;
            progress_bytes = 0;
        }
        if cancel.load(Ordering::Relaxed) {
            return Ok((total_read_size, thread_hasher));
        }
    }
    Ok((total_read_size, thread_hasher))
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
        // global_hasher.update(&buffer[read_size - 4..read_size]);
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
