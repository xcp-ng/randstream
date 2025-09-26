use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use itertools::Itertools as _;
use log::debug;
use parse_size::parse_size;
use rand::{RngCore, SeedableRng};
use rand_pcg::Pcg64Mcg;
use std::fs::OpenOptions;
use std::io::{self, Seek as _, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use crate::{read_file_size, receive_progress, set_up_progress_bar};

/// Generate a random stream
#[derive(Args, Debug)]
#[command(alias = "write")]
pub struct GenerateArgs {
    /// The output file
    #[arg()]
    pub file: Option<PathBuf>,

    /// The stream size
    ///
    /// Defaults to the provide file size if it exists, generates an infinite
    /// stream otherwise
    #[clap(short, long)]
    pub size: Option<String>,

    /// The random generator seed
    ///
    /// An hexidecimal notation is expected. The size can't exceed 16 bytes
    #[clap(short = 'S', long)]
    pub seed: Option<String>,

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

pub fn generate(args: &GenerateArgs) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = parse_size(&args.chunk_size)? as usize;
    // we need to write a multiple a 64 bits to be able to use advance()
    let buffer_size = chunk_size.div_ceil(8) * 8;
    debug!("chunk size: {chunk_size}");
    let stream_size = if let Some(size) = &args.size {
        parse_size(size)?
    } else if let Some(file) = &args.file
        && file.exists()
    {
        read_file_size(file)?
    } else {
        return Err(anyhow!("Size can't be determined. Use --size to provide a stream size."));
    };
    debug!("write size: {stream_size}",);

    let mut seed = [0u8; 16];
    if let Some(seed_hex) = &args.seed {
        hex::decode_to_slice(format!("{:0>32}", seed_hex), &mut seed)?;
    }
    debug!("seed: {}", hex::encode(seed));

    let mut bytes_generated: u64 = 0;
    let pb = (!args.no_progress).then_some(set_up_progress_bar(Some(stream_size))).transpose()?;

    if let Some(file) = &args.file {
        {
            // make sure the output file exists, before opening it in the threads
            let f = OpenOptions::new().create(true).truncate(false).write(true).open(file)?;
            // and that the file size matches the requested size
            if file.is_file() {
                f.set_len(stream_size)?;
            }
        }
        let num_threads = args.jobs.unwrap_or(num_cpus::get_physical());
        debug!("number of threads: {num_threads}");
        let num_chunks = (stream_size as f64 / chunk_size as f64).ceil() as u64;
        let chunks_per_thread = (num_chunks as f64 / num_threads as f64).ceil() as u64;
        let (tx, rx) = mpsc::channel::<u64>();

        let handles: Vec<_> = (0..num_threads as u64)
            .map(|i| {
                let file = file.clone();
                let tx = tx.clone();
                thread::spawn(move || -> anyhow::Result<u64> {
                    let mut writer = OpenOptions::new().write(true).open(file)?;
                    let mut rng = Pcg64Mcg::from_seed(seed);
                    let mut buffer = vec![0; buffer_size];
                    let start_chunk = i * chunks_per_thread;
                    let end_chunk = ((i + 1) * chunks_per_thread).min(num_chunks);
                    writer.seek(io::SeekFrom::Start(start_chunk * chunk_size as u64))?;
                    rng.advance(((start_chunk * buffer_size as u64) / 8).into());
                    let mut total_write_size: u64 = 0;
                    let mut progress_bytes: u64 = 0;
                    for chunk in start_chunk..end_chunk {
                        let write_size =
                            ((stream_size - (chunk * chunk_size as u64)) as usize).min(chunk_size);
                        generate_chunk(&mut rng, &mut buffer, write_size);
                        writer.write_all(&buffer[..write_size])?;
                        total_write_size += write_size as u64;
                        progress_bytes += write_size as u64;
                        if chunk % 100 == 0 {
                            tx.send(progress_bytes)?;
                            progress_bytes = 0;
                        }
                    }
                    Ok(total_write_size)
                })
            })
            .collect();
        receive_progress(&pb, &rx, tx);
        let written_bytes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;
        bytes_generated = written_bytes.iter().sum();
    } else {
        debug!("number of threads: 1");
        let mut writer = io::stdout();
        let mut rng = Pcg64Mcg::from_seed(seed);
        let mut buffer = vec![0u8; chunk_size];
        while bytes_generated < stream_size {
            let write_size = (stream_size - bytes_generated).min(chunk_size as u64) as usize;
            generate_chunk(&mut rng, &mut buffer, write_size);
            writer.write_all(&buffer[..write_size])?;
            bytes_generated += write_size as u64;
            if let Some(pb) = &pb {
                pb.set_position(bytes_generated);
            }
        }
    };
    debug!("written bytes: {bytes_generated}");
    debug!(
        "throughput: {:.2?}GBi/s",
        (bytes_generated / (1024 * 1024 * 1024)) as f32 / start.elapsed().as_micros() as f32
            * 1000000.0
    );
    debug!("run in {:.2?}", start.elapsed());
    Ok(0)
}

fn generate_chunk(rng: &mut Pcg64Mcg, buffer: &mut [u8], write_size: usize) {
    if write_size >= 4 {
        rng.fill_bytes(&mut buffer[..]);
        let mut hasher = Hasher::new();
        hasher.update(&buffer[..write_size - 4]);
        let checksum_bytes = hasher.finalize().to_le_bytes();
        let end_slice = &mut buffer[write_size - 4..write_size];
        end_slice.copy_from_slice(&checksum_bytes);
    } else {
        // not enough room to fit the checksum, just push some zeros in there
        buffer[..write_size].fill(0);
    }
}
