use anyhow::anyhow;
use clap::Args;
use crc32fast::Hasher;
use log::{debug, info};
use parse_size::parse_size;
use rand::Rng as _;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;
use std::fs::OpenOptions;
use std::io::{self, IsTerminal as _, Seek as _, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::cli::CommonArgs;
use crate::{Layout, Progress, ThreadProgress, log_metrics, process_in_parallel, read_file_size};

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
    if args.file.is_none() && io::stdout().is_terminal() {
        return Err(anyhow!(
            "Refusing to write binary data to a terminal. Redirect the output or give an output file."
        ));
    }
    let mut pb = Progress::new(Some(stream_size), args.common.no_progress)?;

    debug!("position: {}", args.position);
    debug!("stream size: {stream_size}");
    debug!("chunk size: {chunk_size}");
    debug!("seed: {}", args.seed);

    let (bytes_generated, checksum) = if let Some(file) = &args.file {
        generate_to_file(args, file, stream_size, buffer_size, &mut pb, &cancel)?
    } else {
        generate_to_stdout(args, stream_size, chunk_size, &mut pb, &cancel)?
    };

    // Check if operation was cancelled
    if cancel.load(Ordering::Relaxed) {
        log_metrics(start, bytes_generated, "written bytes");
        return Ok(130);
    }

    info!("checksum: {checksum:08x}");
    log_metrics(start, bytes_generated, "written bytes");
    Ok(0)
}

fn resolve_stream_size(args: &GenerateArgs) -> anyhow::Result<u64> {
    if let Some(size) = args.common.size {
        if args.position.checked_add(size).is_none() {
            return Err(anyhow!(
                "The position {} plus the size {size} is too large",
                args.position
            ));
        }
        return Ok(size);
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
        return Ok(size - args.position);
    }
    Err(anyhow!("Size can't be determined. Use --size to provide a stream size."))
}

fn generate_to_file(
    args: &GenerateArgs,
    file: &Path,
    stream_size: u64,
    buffer_size: usize,
    pb: &mut Option<Progress>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, u32)> {
    // make sure the output file exists, before opening it in the threads
    let f = OpenOptions::new().create(true).truncate(false).write(true).open(file)?;
    // and that the file size matches the requested size
    if file.is_file() {
        let end_position = stream_size + args.position;
        if end_position > f.metadata()?.len() || !args.no_truncate {
            f.set_len(end_position)?;
        }
    }

    let layout =
        Layout { position: args.position, stream_size, chunk_size: args.common.chunk_size };
    let result = process_in_parallel(
        layout.num_chunks(),
        args.common.num_threads(),
        pb,
        cancel,
        |chunks, progress| {
            write_chunk_range(file, &layout, args.seed, buffer_size, chunks, progress, cancel)
        },
    )?;

    // make sure the data reached the device, and report the errors happening
    // while writing it back. Character devices can't be synced.
    if !cancel.load(Ordering::Relaxed)
        && let Err(err) = f.sync_data()
        && err.kind() != io::ErrorKind::InvalidInput
    {
        return Err(err.into());
    }
    Ok(result)
}

fn write_chunk_range(
    file: &Path,
    layout: &Layout,
    seed: u64,
    buffer_size: usize,
    chunks: Range<u64>,
    progress: &mut ThreadProgress,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, Hasher)> {
    let mut writer = OpenOptions::new().write(true).open(file)?;
    let mut thread_hasher = Hasher::new();
    let mut local_hasher = Hasher::new();
    let mut rng = Pcg64Mcg::seed_from_u64(seed);
    let mut buffer = vec![0; buffer_size];
    writer.seek(io::SeekFrom::Start(layout.offset(chunks.start)))?;
    let advance_amount =
        chunks.start.checked_mul(buffer_size as u64).ok_or_else(|| {
            anyhow!("arithmetic overflow: start_chunk * buffer_size exceeds u64 max")
        })? / 8;
    rng.advance(advance_amount.into());
    let mut total_write_size: u64 = 0;
    for chunk in chunks {
        let write_size = layout.chunk_len(chunk);
        generate_chunk(&mut rng, &mut buffer, write_size, &mut thread_hasher, &mut local_hasher);
        writer.write_all(&buffer[..write_size])?;
        total_write_size += write_size as u64;
        progress.add(write_size);
        if cancel.load(Ordering::Relaxed) {
            break;
        }
    }
    Ok((total_write_size, thread_hasher))
}

fn generate_to_stdout(
    args: &GenerateArgs,
    stream_size: u64,
    chunk_size: usize,
    pb: &mut Option<Progress>,
    cancel: &AtomicBool,
) -> anyhow::Result<(u64, u32)> {
    debug!("number of threads: 1");
    let mut writer = io::stdout();
    let mut rng = Pcg64Mcg::seed_from_u64(args.seed);
    let mut buffer = vec![0u8; chunk_size];
    let mut bytes_generated: u64 = 0;
    let mut hasher = Hasher::new();
    let mut local_hasher = Hasher::new();
    while bytes_generated < stream_size {
        let write_size = (stream_size - bytes_generated).min(chunk_size as u64) as usize;
        generate_chunk(&mut rng, &mut buffer, write_size, &mut hasher, &mut local_hasher);
        writer.write_all(&buffer[..write_size])?;
        bytes_generated += write_size as u64;
        if let Some(p) = pb {
            p.tick(bytes_generated);
        }
        if cancel.load(Ordering::Relaxed) {
            break;
        }
    }
    Ok((bytes_generated, hasher.finalize()))
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
    } else {
        // not enough room to fit the checksum, just push some zeros in there
        buffer[..write_size].fill(0);
        global_hasher.update(&buffer[..write_size]);
    }
}
