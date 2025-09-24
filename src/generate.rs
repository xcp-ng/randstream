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
}

pub fn generate(args: &GenerateArgs) -> anyhow::Result<i32> {
    let stream_size = if let Some(size) = &args.size {
        Some(parse_size(size)?)
    } else if let Some(file) = &args.file
        && file.exists()
    {
        Some(read_file_size(file)?)
    } else {
        None
    };
    debug!(
        "write size: {}",
        match stream_size {
            Some(size) => size.to_string(),
            None => "∞".to_string(),
        }
    );
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
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 65536];
    let mut bytes_generated: u64 = 0;

    if let Some(stream_size) = stream_size {
        while bytes_generated < stream_size {
            let remaining_bytes = stream_size - bytes_generated;
            let bytes_to_generate = remaining_bytes.min(buffer.len() as u64) as usize;
            rng.fill_bytes(&mut buffer[..bytes_to_generate]);
            writer.write_all(&buffer[..bytes_to_generate])?;
            hasher.update(&buffer[..bytes_to_generate]);
            bytes_generated += bytes_to_generate as u64;
        }
    } else {
        loop {
            rng.fill_bytes(&mut buffer);
            if let Ok(write_size) = writer.write(&buffer) {
                hasher.update(&buffer[..write_size]);
                bytes_generated += write_size as u64;
            } else {
                break;
            }
        }
    }
    debug!("written bytes: {bytes_generated}");
    eprintln!("{:08x}", hasher.finalize());

    Ok(0)
}
