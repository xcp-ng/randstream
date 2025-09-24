use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use itertools::Itertools as _;
use log::debug;
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, Seek};
use std::path::PathBuf;
use std::thread;

use crate::{read_exact_or_eof, read_file_size};

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
}

pub fn validate(args: &ValidateArgs) -> anyhow::Result<i32> {
    let chunk_size = parse_size(&args.chunk_size)? as usize;
    if let Some(file) = &args.file {
        let num_threads = args.jobs.unwrap_or(num_cpus::get_physical());
        let file_len: u64 =
            if let Some(size) = &args.size { parse_size(size)? } else { read_file_size(file)? };
        debug!("read size: {file_len}");
        debug!("number of threads: {num_threads}");
        debug!("chunk size: {chunk_size}");

        let num_chunks = (file_len as f64 / chunk_size as f64).ceil() as u64;
        let chunks_per_thread = (num_chunks as f64 / num_threads as f64).ceil() as u64;

        let handles: Vec<_> = (0..num_threads as u64)
            .map(|i| {
                let file = file.clone();
                thread::spawn(move || -> anyhow::Result<u64> {
                    let mut file = File::open(file)?;
                    let start_chunk = i * chunks_per_thread;
                    let end_chunk = ((i + 1) * chunks_per_thread).min(num_chunks);
                    let mut buffer = vec![0; chunk_size];
                    file.seek(io::SeekFrom::Start(start_chunk * chunk_size as u64))?;
                    let mut total_read_size: u64 = 0;
                    for chunk in start_chunk..end_chunk {
                        let read_size = read_exact_or_eof(&mut file, &mut buffer)?;
                        validate_chunk(chunk, &buffer[..read_size])?;
                        total_read_size += read_size as u64;
                    }
                    Ok(total_read_size)
                })
            })
            .collect();
        let read_bytes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;
        debug!("read bytes: {}", read_bytes.iter().sum::<u64>());
    } else {
        debug!("read size: ∞");
        debug!("number of threads: 1");
        debug!("chunk size: {chunk_size}");
        let mut buffer = vec![0; chunk_size];
        let mut stream_size: u64 = 0;
        let mut chunk: u64 = 0;
        loop {
            let read_size = read_exact_or_eof(&mut io::stdin(), &mut buffer)?;
            if read_size == 0 {
                // End of input stream (EOF)
                break;
            }
            validate_chunk(chunk, &buffer[..read_size])?;
            stream_size += read_size as u64;
            chunk += 1;
        }
        debug!("read bytes: {stream_size}");
    };
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
