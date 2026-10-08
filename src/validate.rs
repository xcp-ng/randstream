use anyhow::anyhow;
use clap::Args;
use crc32fast::Hasher;
use log::{debug, info};
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::cli::CommonArgs;
use crate::{
    Layout, Progress, ThreadProgress, log_metrics, process_in_parallel, read_exact_or_eof,
    read_file_size,
};

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

        validate_from_file(args, file, stream_size, &mut pb, &cancel)?
    } else {
        let mut pb = Progress::new(None, args.common.no_progress)?;

        debug!("position: {}", args.position);
        debug!(
            "stream size: {}",
            if let Some(size) = args.common.size { size.to_string() } else { "∞".to_string() }
        );
        debug!("chunk size: {chunk_size}");

        validate_from_stdin(args, chunk_size, &mut pb, &cancel)?
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
    if let Some(size) = args.common.size {
        if args.position.checked_add(size).is_none() {
            return Err(anyhow!(
                "The position {} plus the size {size} is too large",
                args.position
            ));
        }
        return Ok(size);
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
    pb: &mut Option<Progress>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, u32)> {
    let layout =
        Layout { position: args.position, stream_size, chunk_size: args.common.chunk_size };
    process_in_parallel(
        layout.num_chunks(),
        args.common.num_threads(),
        pb,
        cancel,
        |chunks, progress| validate_chunk_range(file, &layout, chunks, progress, cancel),
    )
}

fn validate_chunk_range(
    file: &Path,
    layout: &Layout,
    chunks: Range<u64>,
    progress: &mut ThreadProgress,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, Hasher)> {
    let mut file = File::open(file)?;
    let mut thread_hasher = Hasher::new();
    let mut buffer = vec![0; layout.chunk_size as usize];
    file.seek(io::SeekFrom::Start(layout.offset(chunks.start)))?;
    let mut total_read_size: u64 = 0;
    for chunk in chunks {
        let expected = layout.chunk_len(chunk);
        let read_size = read_exact_or_eof(&mut file, &mut buffer[..expected])?;
        if read_size < expected {
            return Err(anyhow!(
                "Unexpected end of stream at chunk {chunk}. Expected {expected} bytes, found {read_size}."
            ));
        }
        validate_chunk(chunk, &buffer[..read_size], &mut thread_hasher)?;
        total_read_size += read_size as u64;
        progress.add(read_size);
        if cancel.load(Ordering::Relaxed) {
            break;
        }
    }
    Ok((total_read_size, thread_hasher))
}

fn validate_from_stdin(
    args: &ValidateArgs,
    chunk_size: usize,
    pb: &mut Option<Progress>,
    cancel: &AtomicBool,
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
        if let Some(size) = args.common.size {
            let expected = (size - stream_size).min(chunk_size as u64) as usize;
            if read_size < expected {
                return Err(anyhow!(
                    "Unexpected end of stream at chunk {chunk}. Expected {expected} bytes, found {read_size}."
                ));
            }
        }
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
        if cancel.load(Ordering::Relaxed) {
            break;
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
