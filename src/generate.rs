use aligned_vec::{AVec, RuntimeAlign};
use anyhow::anyhow;
use clap::Args;
use crc32fast::Hasher;
use itertools::Itertools as _;
use log::{debug, info};
use parse_size::parse_size;
use rand::Rng as _;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;
use std::fs::OpenOptions;
use std::io::{self, Seek as _, Write};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

use crate::cli::CommonArgs;
use crate::{
    DIRECT_ALIGN, PeakRate, Progress, check_direct_alignment, direct_aligned_stream_size,
    log_summary, read_file_size, receive_progress,
};

/// Describes the logical random stream being generated
#[derive(Clone, Debug)]
pub(crate) struct StreamParams {
    pub(crate) seed: u64,
    pub(crate) position: u64,
    pub(crate) stream_size: u64,
    pub(crate) chunk_size: usize,
    pub(crate) buffer_size: usize,
    pub(crate) direct: bool,
}

/// Describes the work slice assigned to one thread
#[derive(Clone, Debug)]
pub(crate) struct ThreadWork {
    pub(crate) thread_index: u64,
    pub(crate) chunks_per_thread: u64,
    pub(crate) num_chunks: u64,
}

/// Generate a random stream
#[derive(Args, Debug)]
#[command(alias = "write")]
pub struct GenerateArgs {
    /// The output file
    #[arg()]
    pub file: Option<PathBuf>,

    /// The stream position
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

pub fn generate(args: &GenerateArgs, cancel: Arc<AtomicBool>) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = args.common.chunk_size as usize;
    // we need to write a multiple a 64 bits to be able to use advance()
    let buffer_size = chunk_size.div_ceil(8) * 8;
    let stream_size = resolve_stream_size(args)?;
    let mut pb = Progress::new(Some(stream_size), args.common.no_progress)?;

    debug!("position: {}", args.position);
    debug!("stream size: {stream_size}");
    debug!("chunk size: {chunk_size}");
    debug!("seed: {}", args.seed);

    let (bytes_generated, checksum, peak_rate) = if let Some(file) = &args.file {
        generate_to_file(args, file, stream_size, chunk_size, buffer_size, &mut pb, &cancel)?
    } else {
        generate_to_stdout(args, stream_size, chunk_size, &mut pb)?
    };

    // Check if operation was cancelled
    if cancel.load(Ordering::Relaxed) {
        log_summary(start, bytes_generated, peak_rate);
        return Ok(130);
    }

    info!("checksum: {checksum:08x}");
    log_summary(start, bytes_generated, peak_rate);
    Ok(0)
}

fn resolve_stream_size(args: &GenerateArgs) -> anyhow::Result<u64> {
    if let Some(size) = &args.common.size {
        return direct_aligned_stream_size(size.bytes, size.has_unit, args.common.direct);
    }
    if let Some(file) = &args.file
        && file.exists()
    {
        let size = read_file_size(file)?;
        if args.position > size {
            return Err(anyhow!(
                "The position {} is greater than the file size {size}",
                args.position
            ));
        }
        return direct_aligned_stream_size(size - args.position, false, args.common.direct);
    }
    Err(anyhow!("Size can't be determined. Use --size to provide a stream size."))
}

fn generate_to_file(
    args: &GenerateArgs,
    file: &PathBuf,
    stream_size: u64,
    chunk_size: usize,
    buffer_size: usize,
    pb: &mut Option<Progress>,
    cancel: &Arc<AtomicBool>,
) -> anyhow::Result<(u64, u32, f64)> {
    if args.common.direct {
        check_direct_alignment(chunk_size as u64, args.position)?;
    }

    // make sure the output file exists, before opening it in the threads
    let f = OpenOptions::new().create(true).truncate(false).write(true).open(file)?;
    // and that the file size matches the requested size
    if file.is_file() {
        let end_position = stream_size + args.position;
        if end_position > f.metadata()?.len() || !args.no_truncate {
            f.set_len(end_position)?;
        }
    }

    let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
    debug!("number of threads: {num_threads}");
    let num_chunks = stream_size.div_ceil(chunk_size as u64);
    let chunks_per_thread = num_chunks.div_ceil(num_threads as u64);
    let (tx, rx) = mpsc::channel::<u64>();

    let stream = StreamParams {
        seed: args.seed,
        position: args.position,
        stream_size,
        chunk_size,
        buffer_size,
        direct: args.common.direct,
    };

    let handles: Vec<_> = (0..num_threads as u64)
        .map(|i| {
            let file = file.clone();
            let tx = tx.clone();
            let cancel = cancel.clone();
            let stream = stream.clone();
            thread::spawn(move || -> anyhow::Result<_> {
                // One aligned buffer per thread, reused for every chunk.
                let mut buffer = AVec::<u8, RuntimeAlign>::from_iter(
                    DIRECT_ALIGN,
                    std::iter::repeat_n(0u8, stream.buffer_size),
                );
                let work = ThreadWork { thread_index: i, chunks_per_thread, num_chunks };
                let result =
                    write_chunk_range(&file, &stream, &work, buffer.as_mut_slice(), &tx, &cancel);
                if result.is_err() {
                    // tell the other threads to stop
                    cancel.store(true, Ordering::Relaxed);
                }
                result
            })
        })
        .collect();

    let peak_rate = receive_progress(pb, &rx, tx);
    let thread_data: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).try_collect()?;

    let write_bytes = thread_data.iter().map(|(b, _)| b).sum();
    let thread_hashers: Vec<_> = thread_data.iter().map(|(_, h)| h).collect();
    let mut hasher = thread_hashers[0].clone();
    for partial_hasher in thread_hashers[1..].iter() {
        hasher.combine(partial_hasher);
    }

    Ok((write_bytes, hasher.finalize(), peak_rate))
}

/// Generate the thread's/range's chunks into `buffer`, which must be exactly
/// `stream.buffer_size` long and is reused across chunks (and, in `stress`,
/// across jobs) so no allocation happens here.
pub(crate) fn write_chunk_range(
    file: &Path,
    stream: &StreamParams,
    work: &ThreadWork,
    buffer: &mut [u8],
    tx: &mpsc::Sender<u64>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, Hasher)> {
    debug_assert_eq!(buffer.len(), stream.buffer_size);
    let mut options = OpenOptions::new();
    options.write(true);
    if stream.direct {
        options.custom_flags(nix::libc::O_DIRECT);
    }
    let mut writer = options.open(file)?;
    let mut thread_hasher = Hasher::new();
    let mut local_hasher = Hasher::new();
    let mut rng = Pcg64Mcg::seed_from_u64(stream.seed);
    let start_chunk = work.thread_index * work.chunks_per_thread;
    let end_chunk = ((work.thread_index + 1) * work.chunks_per_thread).min(work.num_chunks);
    writer.seek(io::SeekFrom::Start(stream.position + start_chunk * stream.chunk_size as u64))?;
    let advance_amount = start_chunk
        .checked_mul(stream.buffer_size as u64)
        .ok_or_else(|| anyhow!("arithmetic overflow: start_chunk * buffer_size exceeds u64 max"))?
        / 8;
    rng.advance(advance_amount.into());
    let mut total_write_size: u64 = 0;
    let mut progress_bytes: u64 = 0;
    for chunk in start_chunk..end_chunk {
        let write_size = (stream.stream_size - chunk * stream.chunk_size as u64)
            .min(stream.chunk_size as u64) as usize;
        generate_chunk(&mut rng, buffer, write_size, &mut thread_hasher, &mut local_hasher);
        writer.write_all(&buffer[..write_size])?;
        total_write_size += write_size as u64;
        progress_bytes += write_size as u64;
        if chunk % 100 == 0 {
            tx.send(progress_bytes)?;
            progress_bytes = 0;
        }
        if cancel.load(Ordering::Relaxed) {
            if progress_bytes > 0 {
                tx.send(progress_bytes)?;
            }
            return Ok((total_write_size, thread_hasher));
        }
    }
    // Flush the bytes accumulated since the last `% 100` checkpoint. Ranges
    // shorter than the sampling stride (e.g. `stress`'s small ranges) would
    // otherwise report little or no progress at all.
    if progress_bytes > 0 {
        tx.send(progress_bytes)?;
    }
    Ok((total_write_size, thread_hasher))
}

fn generate_to_stdout(
    args: &GenerateArgs,
    stream_size: u64,
    chunk_size: usize,
    pb: &mut Option<Progress>,
) -> anyhow::Result<(u64, u32, f64)> {
    debug!("number of threads: 1");
    let mut writer = io::stdout();
    let mut rng = Pcg64Mcg::seed_from_u64(args.seed);
    let mut buffer = vec![0u8; chunk_size];
    let mut bytes_generated: u64 = 0;
    let mut hasher = Hasher::new();
    let mut local_hasher = Hasher::new();
    let mut peak = PeakRate::new();
    while bytes_generated < stream_size {
        let write_size = (stream_size - bytes_generated).min(chunk_size as u64) as usize;
        generate_chunk(&mut rng, &mut buffer, write_size, &mut hasher, &mut local_hasher);
        writer.write_all(&buffer[..write_size])?;
        bytes_generated += write_size as u64;
        peak.sample(bytes_generated);
        if let Some(p) = pb {
            p.tick(bytes_generated);
        }
    }
    Ok((bytes_generated, hasher.finalize(), peak.peak()))
}

pub fn generate_chunk(
    rng: &mut Pcg64Mcg,
    buffer: &mut [u8],
    write_size: usize,
    global_hasher: &mut Hasher,
    local_hasher: &mut Hasher,
) {
    if write_size >= 4 {
        rng.fill_bytes(&mut buffer[..]);
        local_hasher.reset();
        local_hasher.update(&buffer[..write_size - 4]);
        global_hasher.combine(local_hasher);
        let checksum_bytes = local_hasher.clone().finalize().to_le_bytes();
        let end_slice = &mut buffer[write_size - 4..write_size];
        end_slice.copy_from_slice(&checksum_bytes);
        // global_hasher.update(&checksum_bytes);
    } else {
        // not enough room to fit the checksum, just push some zeros in there
        buffer[..write_size].fill(0);
        global_hasher.update(&buffer[..write_size]);
    }
}
