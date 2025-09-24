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
    #[clap(short, long, default_value = "1Mi")]
    pub chunk_size: String,
}

pub fn validate(args: &ValidateArgs) -> anyhow::Result<i32> {
    if let Some(file) = &args.file {
        let num_threads = args.jobs.unwrap_or(num_cpus::get_physical());
        let file_len: u64 =
            if let Some(size) = &args.size { parse_size(size)? } else { file.metadata()?.len() };
        let chunk_size = parse_size(&args.chunk_size)? as usize;

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

        println!("{:x}", hasher.finalize());

        Ok(0)
    } else {
        let chunk_size = parse_size(&args.chunk_size)? as usize;
        let mut buffer = vec![0; chunk_size];
        let mut hasher = Hasher::new();
        loop {
            let bytes_read = io::stdin().read(&mut buffer)?;
            if bytes_read == 0 {
                // End of input stream (EOF)
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        println!("{:x}", hasher.finalize());

        Ok(0)
    }
}
