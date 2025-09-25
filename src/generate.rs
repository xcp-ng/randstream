use anyhow::anyhow;
use clap::{Args, arg, command};
use crc32fast::Hasher;
use log::debug;
use parse_size::parse_size;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::read_file_size;

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
    /// An hexidecimal notation is expected. The size can't exceed 32 bytes
    #[clap(short = 'S', long)]
    pub seed: Option<String>,

    /// The chunk size
    #[clap(short, long, default_value = "32ki")]
    pub chunk_size: String,
}

pub fn generate(args: &GenerateArgs) -> anyhow::Result<i32> {
    let chunk_size = parse_size(&args.chunk_size)? as usize;
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
    let mut writer: Box<dyn Write> = if let Some(path) = &args.file {
        Box::new(File::create(path)?)
    } else {
        Box::new(io::stdout())
    };

    let mut seed = [0u8; 32];
    if let Some(seed_hex) = &args.seed {
        hex::decode_to_slice(format!("{:0>64}", seed_hex), &mut seed)?;
    }
    debug!("seed: {}", hex::encode(seed));
    let mut rng = SmallRng::from_seed(seed);
    let mut buffer = vec![0u8; chunk_size];
    let mut bytes_generated: u64 = 0;

    while bytes_generated < stream_size {
        let remaining_bytes = stream_size - bytes_generated;
        let bytes_to_generate = remaining_bytes.min(chunk_size as u64) as usize;
        if bytes_to_generate >= 4 {
            rng.fill_bytes(&mut buffer[..bytes_to_generate - 4]);
            let mut hasher = Hasher::new();
            hasher.update(&buffer[..bytes_to_generate - 4]);
            let checksum_bytes = hasher.finalize().to_le_bytes();
            let end_slice = &mut buffer[bytes_to_generate - 4..bytes_to_generate];
            end_slice.copy_from_slice(&checksum_bytes);
        } else {
            // not enough room to fit the checksum, just push some zeros in there
            buffer[..bytes_to_generate].fill(0);
        }
        writer.write_all(&buffer[..bytes_to_generate])?;
        bytes_generated += bytes_to_generate as u64;
    }
    debug!("written bytes: {bytes_generated}");

    Ok(0)
}
