use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use human_units::{FormatDuration as _, FormatSize as _};
use itertools::Itertools as _;
use log::{debug, info};
use parse_size::parse_size;
use rand::{RngCore, SeedableRng};
use rand_pcg::Pcg64Mcg;
use std::fs::OpenOptions;
use std::io::{self, Seek as _, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

use crate::cli::CommonArgs;
use crate::{read_file_size, receive_progress, set_up_progress_bar};

/// Generate a random stream
#[derive(Args, Debug)]
#[command(alias = "write")]
pub struct GenerateArgs {
    /// The output file
    #[arg()]
    pub file: Option<PathBuf>,

    /// The stream position
    ///
    /// Must be a multiple of the chunk size
    #[clap(short, long, default_value = "0", value_parser=|s: &str| parse_size(s), requires="file")]
    pub position: u64,

    /// The random generator seed
    #[clap(short = 'S', long, default_value = "0")]
    pub seed: u64,

    /// Don't truncate the file
    #[clap(short = 't', long)]
    pub no_truncate: bool,

    #[clap(flatten)]
    pub common: CommonArgs,
}

pub fn generate(args: &GenerateArgs) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = args.common.chunk_size as usize;
    // we need to write a multiple a 64 bits to be able to use advance()
    let buffer_size = chunk_size.div_ceil(8) * 8;
    if !args.position.is_multiple_of(args.common.chunk_size) {
        return Err(anyhow!(
            "The start position {} is not a multiple of the chunk size {chunk_size}",
            args.position
        ));
    }
    let stream_size = if let Some(size) = &args.common.size {
        *size
    } else if let Some(file) = &args.file
        && file.exists()
    {
        let size = read_file_size(file)?;
        if args.position > size {
            return Err(anyhow!(
                "The position {} is greater than the file size {size}",
                args.position
            ));
        }
        size - args.position
    } else {
        return Err(anyhow!("Size can't be determined. Use --size to provide a stream size."));
    };

    let pb =
        (!args.common.no_progress).then_some(set_up_progress_bar(Some(stream_size))).transpose()?;

    debug!("position: {}", args.position);
    debug!("stream size: {stream_size}");
    debug!("chunk size: {chunk_size}");
    debug!("seed: {}", args.seed);

    let (bytes_generated, checksum) = if let Some(file) = &args.file {
        {
            // make sure the output file exists, before opening it in the threads
            let f = OpenOptions::new().create(true).truncate(false).write(true).open(file)?;
            // and that the file size matches the requested size
            if file.is_file() {
                let end_position = stream_size + args.position;
                if end_position > file.metadata()?.len() || !args.no_truncate {
                    f.set_len(end_position)?;
                }
            }
        }
        let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
        debug!("number of threads: {num_threads}");
        let num_chunks = stream_size.div_ceil(chunk_size as u64);
        let chunks_per_thread = num_chunks.div_ceil(num_threads as u64);
        let chunk_position = args.position / chunk_size as u64;
        let (tx, rx) = mpsc::channel::<u64>();
        let cancel: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        let handles: Vec<_> = (0..num_threads as u64)
            .map(|i| {
                let seed = args.seed;
                let file = file.clone();
                let position = args.position;
                let tx = tx.clone();
                let cancel = cancel.clone();
                thread::spawn(move || -> anyhow::Result<_> {
                    let run = || {
                        let mut writer = OpenOptions::new().write(true).open(file)?;
                        let mut thread_hasher = Hasher::new();
                        let mut rng = Pcg64Mcg::seed_from_u64(seed);
                        let mut buffer = vec![0; buffer_size];
                        let start_chunk = i * chunks_per_thread + chunk_position;
                        let end_chunk =
                            ((i + 1) * chunks_per_thread).min(num_chunks) + chunk_position;
                        writer.seek(io::SeekFrom::Start(start_chunk * chunk_size as u64))?;
                        rng.advance(((start_chunk * buffer_size as u64) / 8).into());
                        let mut total_write_size: u64 = 0;
                        let mut progress_bytes: u64 = 0;
                        for chunk in start_chunk..end_chunk {
                            let write_size =
                                ((position + stream_size - (chunk * chunk_size as u64)) as usize)
                                    .min(chunk_size);
                            generate_chunk(&mut rng, &mut buffer, write_size, &mut thread_hasher);
                            writer.write_all(&buffer[..write_size])?;
                            total_write_size += write_size as u64;
                            progress_bytes += write_size as u64;
                            if chunk % 100 == 0 {
                                tx.send(progress_bytes)?;
                                progress_bytes = 0;
                            }
                            if cancel.load(Ordering::Relaxed) {
                                // just quit early
                                return Ok((total_write_size, thread_hasher));
                            }
                        }
                        Ok((total_write_size, thread_hasher))
                    };
                    let result = run();
                    if result.is_err() {
                        // tell the other thread to stop there
                        cancel.store(true, Ordering::Relaxed);
                    }
                    result
                })
            })
            .collect();
        receive_progress(&pb, &rx, tx);
        let thread_data: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;

        let write_bytes = thread_data.iter().map(|(b, _)| b).sum();

        let thread_hashers: Vec<_> = thread_data.iter().map(|(_, h)| h).collect();
        let mut hasher = thread_hashers[0].clone();
        for partial_hasher in thread_hashers[1..].iter() {
            hasher.combine(partial_hasher)
        }

        (write_bytes, hasher.finalize())
    } else {
        debug!("number of threads: 1");
        let mut writer = io::stdout();
        let mut rng = Pcg64Mcg::seed_from_u64(args.seed);
        let mut buffer = vec![0u8; chunk_size];
        let mut bytes_generated: u64 = 0;
        let mut hasher = Hasher::new();
        while bytes_generated < stream_size {
            let write_size = (stream_size - bytes_generated).min(chunk_size as u64) as usize;
            generate_chunk(&mut rng, &mut buffer, write_size, &mut hasher);
            writer.write_all(&buffer[..write_size])?;
            bytes_generated += write_size as u64;
            if let Some(pb) = &pb {
                pb.set_position(bytes_generated);
            }
        }
        (bytes_generated, hasher.finalize())
    };
    info!("checksum: {checksum:08x}");
    debug!("written bytes: {bytes_generated}");
    debug!(
        "throughput: {}/s",
        ((bytes_generated as f32 / start.elapsed().as_micros() as f32 * 1000000.0) as usize)
            .format_size()
    );
    debug!("run in {}", start.elapsed().format_duration());
    Ok(0)
}

fn generate_chunk(
    rng: &mut Pcg64Mcg,
    buffer: &mut [u8],
    write_size: usize,
    global_hasher: &mut Hasher,
) {
    if write_size >= 4 {
        rng.fill_bytes(&mut buffer[..]);
        let mut hasher = Hasher::new();
        hasher.update(&buffer[..write_size - 4]);
        global_hasher.combine(&hasher);
        let checksum_bytes = hasher.finalize().to_le_bytes();
        let end_slice = &mut buffer[write_size - 4..write_size];
        end_slice.copy_from_slice(&checksum_bytes);
        global_hasher.update(&checksum_bytes);
    } else {
        // not enough room to fit the checksum, just push some zeros in there
        buffer[..write_size].fill(0);
        global_hasher.update(&buffer[..write_size]);
    }
}
