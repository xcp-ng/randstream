use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use human_units::{FormatDuration, FormatSize as _};
use itertools::Itertools as _;
use log::{debug, info};
use std::fs::File;
use std::io::{self, Seek};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use crate::cli::CommonArgs;
use crate::{read_exact_or_eof, read_file_size, receive_progress, set_up_progress_bar};

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
    let chunk_size = args.common.chunk_size.0 as usize;
    let (bytes_validated, checksum) = if let Some(file) = &args.file {
        let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
        let stream_size: u64 =
            if let Some(size) = &args.common.size { size.0 } else { read_file_size(file)? };
        let pb = (!args.common.no_progress)
            .then_some(set_up_progress_bar(Some(stream_size)))
            .transpose()?;

        debug!("read size: {stream_size}");
        debug!("number of threads: {num_threads}");
        debug!("chunk size: {chunk_size}");

        let num_chunks = (stream_size as f64 / chunk_size as f64).ceil() as u64;
        let chunks_per_thread = (num_chunks as f64 / num_threads as f64).ceil() as u64;
        let (tx, rx) = mpsc::channel::<u64>();

        let handles: Vec<_> = (0..num_threads as u64)
            .map(|i| {
                let file = file.clone();
                let tx = tx.clone();
                thread::spawn(move || -> anyhow::Result<_> {
                    let mut file = File::open(file)?;
                    let mut thread_hasher = Hasher::new();
                    let start_chunk = i * chunks_per_thread;
                    let end_chunk = ((i + 1) * chunks_per_thread).min(num_chunks);
                    let mut buffer = vec![0; chunk_size];
                    file.seek(io::SeekFrom::Start(start_chunk * chunk_size as u64))?;
                    let mut total_read_size: u64 = 0;
                    let mut progress_bytes: u64 = 0;
                    for chunk in start_chunk..end_chunk {
                        let read_size = read_exact_or_eof(&mut file, &mut buffer)?;
                        validate_chunk(chunk, &buffer[..read_size])?;
                        thread_hasher.update(&buffer[..read_size]);
                        total_read_size += read_size as u64;
                        progress_bytes += read_size as u64;
                        if chunk % 100 == 0 {
                            tx.send(progress_bytes)?;
                            progress_bytes = 0;
                        }
                    }
                    Ok((total_read_size, thread_hasher))
                })
            })
            .collect();
        receive_progress(&pb, &rx, tx);
        let thread_data: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;

        let read_bytes = thread_data.iter().map(|(b, _)| b).sum();

        let thread_hashers: Vec<_> = thread_data.iter().map(|(_, h)| h).collect();
        let mut hasher = thread_hashers[0].clone();
        for partial_hasher in thread_hashers[1..].iter() {
            hasher.combine(partial_hasher)
        }

        (read_bytes, hasher.finalize())
    } else {
        debug!("read size: ∞");
        debug!("number of threads: 1");
        debug!("chunk size: {chunk_size}");
        let mut buffer = vec![0; chunk_size];
        let mut stream_size: u64 = 0;
        let mut chunk: u64 = 0;
        let mut hasher = Hasher::new();
        let pb = (!args.common.no_progress).then_some(set_up_progress_bar(None)).transpose()?;
        loop {
            let read_size = read_exact_or_eof(&mut io::stdin(), &mut buffer)?;
            if read_size == 0 {
                // End of input stream (EOF)
                break;
            }
            validate_chunk(chunk, &buffer[..read_size])?;
            hasher.update(&buffer[..read_size]);
            stream_size += read_size as u64;
            chunk += 1;
            if let Some(pb) = &pb {
                pb.set_position(stream_size);
            }
        }
        (stream_size, hasher.finalize())
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

fn validate_chunk(chunk: u64, buffer: &[u8]) -> anyhow::Result<()> {
    let mut hasher = Hasher::new();
    let read_size = buffer.len();
    if read_size >= 4 {
        hasher.update(&buffer[..read_size - 4]);
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
        for v in buffer[..read_size].iter() {
            if *v != 0 {
                return Err(anyhow!("Invalid non-zero value at the end of the file"));
            }
        }
    }
    Ok(())
}
