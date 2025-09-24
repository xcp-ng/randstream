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
    #[clap(short, long)]
    pub size: Option<String>,
}

pub fn generate(args: &GenerateArgs) -> anyhow::Result<i32> {
    let total_size_to_generate: u64 = if let Some(size) = &args.size {
        parse_size(size)?
    } else if let Some(file) = &args.file {
        read_file_size(file)?
    } else {
        return Err(anyhow!("A file or a size is required"));
    };
    debug!("stream size: {total_size_to_generate}");
    let mut writer: Box<dyn Write> = if let Some(path) = &args.file {
        Box::new(File::create(path)?)
    } else {
        Box::new(io::stdout())
    };

    let seed = [0u8; 32];
    let mut rng = SmallRng::from_seed(seed);
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 65536];
    let mut bytes_generated: u64 = 0;

    while bytes_generated < total_size_to_generate {
        let remaining_bytes = total_size_to_generate - bytes_generated;
        let bytes_to_generate = remaining_bytes.min(buffer.len() as u64) as usize;

        rng.fill_bytes(&mut buffer[..bytes_to_generate]);
        hasher.update(&buffer[..bytes_to_generate]);

        if writer.write_all(&buffer[..bytes_to_generate]).is_err() {
            break;
        }

        bytes_generated += bytes_to_generate as u64;
    }

    eprintln!("{:x}", hasher.finalize());

    Ok(0)
}
