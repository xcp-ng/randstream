use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use itertools::Itertools as _;
use log::debug;
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::PathBuf;
use std::thread;

use crate::read_file_size;

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
    #[clap(short, long, default_value = "1Mi")]
    pub chunk_size: String,
}

pub fn validate(args: &ValidateArgs) -> anyhow::Result<i32> {
    let chunk_size = parse_size(&args.chunk_size)? as usize;
    let checksum = if let Some(file) = &args.file {
        let num_threads = args.jobs.unwrap_or(num_cpus::get_physical());
        let file_len: u64 =
            if let Some(size) = &args.size { parse_size(size)? } else { read_file_size(file)? };
        debug!("read size: {file_len}");
        debug!("number of threads: {num_threads}");
        debug!("chunk size: {chunk_size}");

        let num_chunks = (file_len as f64 / chunk_size as f64).ceil() as usize;
        let chunks_per_thread = (num_chunks as f64 / num_threads as f64).ceil() as usize;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let file = file.clone();
                thread::spawn(move || -> io::Result<Hasher> {
                    let mut file = File::open(file)?;
                    let start_chunk = i * chunks_per_thread;
                    let end_chunk = ((i + 1) * chunks_per_thread).min(num_chunks);
                    let mut buffer = vec![0; chunk_size];
                    let mut hasher = Hasher::new();
                    for chunk in start_chunk..end_chunk {
                        let start_offset = (chunk * chunk_size) as u64;
                        file.seek(io::SeekFrom::Start(start_offset))?;
                        let read_size = file.read(&mut buffer)?;
                        hasher.update(&buffer[..read_size]);
                    }
                    Ok(hasher)
                })
            })
            .collect();

        let partial_hashers: Vec<_> =
            handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;

        let mut hasher = partial_hashers[0].clone();
        for partial_hasher in partial_hashers[1..].iter() {
            hasher.combine(partial_hasher)
        }

        hasher.finalize()
    } else {
        debug!("read size: ∞");
        debug!("number of threads: 1");
        debug!("chunk size: {chunk_size}");
        let mut buffer = vec![0; chunk_size];
        let mut hasher = Hasher::new();
        let mut stream_size: u64 = 0;
        loop {
            let bytes_read = io::stdin().read(&mut buffer)?;
            if bytes_read == 0 {
                // End of input stream (EOF)
                break;
            }
            hasher.update(&buffer[..bytes_read]);
            stream_size += bytes_read as u64;
        }
        debug!("read bytes: {stream_size}");
        hasher.finalize()
    };
    if let Some(expected_checksum) = &args.expected_checksum
        && expected_checksum != &format!("{checksum:08x}")
    {
        return Err(anyhow!(
            "Checksum mismatch. It was expected to be {expected_checksum}, but is actually {checksum:x}"
        ));
    }
    println!("{checksum:08x}");
    Ok(0)
}
