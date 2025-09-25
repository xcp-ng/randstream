use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use itertools::Itertools as _;
use log::debug;
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, Seek};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

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

    /// The stream size
    ///
    /// Defaults to the provided file size
    #[clap(short, long)]
    pub size: Option<String>,

    /// The number of parallel jobs
    ///
    /// Defaults to the number of physical cores on the host
    #[clap(short, long)]
    pub jobs: Option<usize>,

    /// The chunk size
    #[clap(short, long, default_value = "32ki")]
    pub chunk_size: String,

    /// Hide the progress bar
    #[clap(short, long)]
    pub no_progress: bool,
}

pub fn validate(args: &ValidateArgs) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = parse_size(&args.chunk_size)? as usize;
    let bytes_validated = if let Some(file) = &args.file {
        let num_threads = args.jobs.unwrap_or(num_cpus::get_physical());
        let file_len: u64 =
            if let Some(size) = &args.size { parse_size(size)? } else { read_file_size(file)? };
        let pb = (!args.no_progress).then_some(set_up_progress_bar(Some(file_len))).transpose()?;

        debug!("read size: {file_len}");
        debug!("number of threads: {num_threads}");
        debug!("chunk size: {chunk_size}");

        let num_chunks = (file_len as f64 / chunk_size as f64).ceil() as u64;
        let chunks_per_thread = (num_chunks as f64 / num_threads as f64).ceil() as u64;

        let (tx, rx) = mpsc::channel::<u64>();

        let handles: Vec<_> = (0..num_threads as u64)
            .map(|i| {
                let file = file.clone();
                let tx = tx.clone();
                thread::spawn(move || -> anyhow::Result<u64> {
                    let mut file = File::open(file)?;
                    let start_chunk = i * chunks_per_thread;
                    let end_chunk = ((i + 1) * chunks_per_thread).min(num_chunks);
                    let mut buffer = vec![0; chunk_size];
                    file.seek(io::SeekFrom::Start(start_chunk * chunk_size as u64))?;
                    let mut total_read_size: u64 = 0;
                    let mut progress_bytes: u64 = 0;
                    for chunk in start_chunk..end_chunk {
                        let read_size = read_exact_or_eof(&mut file, &mut buffer)?;
                        validate_chunk(chunk, &buffer[..read_size])?;
                        total_read_size += read_size as u64;
                        progress_bytes += read_size as u64;
                        if chunk % 100 == 0 {
                            tx.send(progress_bytes)?;
                            progress_bytes = 0;
                        }
                    }
                    Ok(total_read_size)
                })
            })
            .collect();
        receive_progress(&pb, &rx, tx);
        let read_bytes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;
        read_bytes.iter().sum()
    } else {
        debug!("read size: ∞");
        debug!("number of threads: 1");
        debug!("chunk size: {chunk_size}");
        let mut buffer = vec![0; chunk_size];
        let mut stream_size: u64 = 0;
        let mut chunk: u64 = 0;
        let pb = (!args.no_progress).then_some(set_up_progress_bar(None)).transpose()?;
        loop {
            let read_size = read_exact_or_eof(&mut io::stdin(), &mut buffer)?;
            if read_size == 0 {
                // End of input stream (EOF)
                break;
            }
            validate_chunk(chunk, &buffer[..read_size])?;
            stream_size += read_size as u64;
            chunk += 1;
            if let Some(pb) = &pb {
                pb.set_position(stream_size);
            }
        }
        stream_size
    };
    debug!("read bytes: {bytes_validated}");
    debug!(
        "throughput: {:.2?}GBi/s",
        (bytes_validated / (1024 * 1024 * 1024)) as f32 / start.elapsed().as_micros() as f32
            * 1000000.0
    );
    debug!("run in {:.2?}", start.elapsed());
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
