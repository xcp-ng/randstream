use clap::{Args, arg, command};
use crc32fast::Hasher;
use itertools::Itertools as _;
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::PathBuf;
use std::thread;

/// Validate a random stream
///
/// If the input is a regular file or a block device, the data will be read
/// from multiple locations in parallel to maximize the throughput.
#[derive(Args, Debug)]
#[command(alias = "read")]
pub struct ValidateArgs {
    /// The input file
    #[arg()]
    pub file: PathBuf,

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
}

const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB

pub fn validate(args: &ValidateArgs) -> anyhow::Result<i32> {
    let num_threads = args.jobs.unwrap_or(num_cpus::get_physical());
    let file_len: u64 =
        if let Some(size) = &args.size { parse_size(size)? } else { args.file.metadata()?.len() };

    let num_chunks = (file_len as f64 / CHUNK_SIZE as f64).ceil() as usize;
    let chunks_per_thread = (num_chunks as f64 / num_threads as f64).ceil() as usize;

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let file = args.file.clone();
            thread::spawn(move || -> io::Result<Hasher> {
                let mut file = File::open(file)?;
                let start_chunk = i * chunks_per_thread;
                let end_chunk = ((i + 1) * chunks_per_thread).min(num_chunks);
                let mut buffer = vec![0; CHUNK_SIZE];
                let mut hasher = Hasher::new();
                for chunk in start_chunk..end_chunk {
                    let start_offset = (chunk * CHUNK_SIZE) as u64;
                    file.seek(io::SeekFrom::Start(start_offset))?;
                    let read_size = file.read(&mut buffer)?;
                    hasher.update(&buffer[..read_size]);
                }
                Ok(hasher)
            })
        })
        .collect();

    let partial_hashers: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;

    let mut hasher = partial_hashers[0].clone();
    for partial_hasher in partial_hashers[1..].iter() {
        hasher.combine(partial_hasher)
    }

    println!("{:x}", hasher.finalize());

    Ok(0)
}
