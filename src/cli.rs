use std::num::NonZeroUsize;

use clap::{Args, Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use parse_size::parse_size;

use crate::{generate::GenerateArgs, validate::ValidateArgs};

/// This utility creates and validate a random stream of data with built-in validation.
///
/// By including a checksum within each data chunk, it enables independent
/// validation and simplifies the process of locating errors within a specific
/// segment of the stream.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[command(flatten)]
    pub verbose: Verbosity<InfoLevel>,
}

#[derive(Args, Debug)]
pub struct CommonArgs {
    /// The stream size
    ///
    /// Defaults to the provided file size
    #[clap(short, long, value_parser=|s: &str| parse_size(s))]
    pub size: Option<u64>,

    /// The number of parallel jobs
    ///
    /// Defaults to the number of physical cores on the host
    #[clap(short, long)]
    pub jobs: Option<NonZeroUsize>,

    /// The chunk size
    ///
    /// Each chunk ends with a 4 bytes checksum, so it must be at least 5 bytes long
    #[clap(short, long, default_value = "32ki", value_parser=parse_chunk_size)]
    pub chunk_size: u64,

    /// Hide the progress bar
    #[clap(short = 'P', long)]
    pub no_progress: bool,
}

/// The smallest chunk with some random data in addition to its checksum
const MIN_CHUNK_SIZE: u64 = 5;

fn parse_chunk_size(s: &str) -> Result<u64, String> {
    let size = parse_size(s).map_err(|e| e.to_string())?;
    if size < MIN_CHUNK_SIZE {
        return Err(format!("must be at least {MIN_CHUNK_SIZE} bytes"));
    }
    Ok(size)
}

impl CommonArgs {
    pub fn num_threads(&self) -> usize {
        self.jobs.map_or(num_cpus::get_physical(), NonZeroUsize::get)
    }
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Generate(GenerateArgs),
    Validate(ValidateArgs),
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
