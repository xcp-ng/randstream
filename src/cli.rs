use clap::{Args, Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use parse_size::parse_size;

use crate::{generate::GenerateArgs, stress::StressArgs, validate::ValidateArgs};

/// This utility creates and validate a random stream of data with built-in validation.
///
/// By including a checksum within each data chunk, it enables independent
/// validation and simplifies the process of locating errors within a specific
/// segment of the stream.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[command(flatten)]
    pub verbose: Verbosity<InfoLevel>,
}

/// A `--size` value, remembering whether the user wrote it with a unit suffix
/// (`5G`, `4Mi`, …) or as a plain byte count. With `--direct` the two are
/// treated differently: a unit size must already be 4096-aligned, while a raw
/// byte count is rounded down automatically.
#[derive(Clone, Copy, Debug)]
pub struct Size {
    pub bytes: u64,
    pub has_unit: bool,
}

fn parse_size_arg(s: &str) -> Result<Size, parse_size::Error> {
    Ok(Size { bytes: parse_size(s)?, has_unit: s.chars().any(|c| c.is_ascii_alphabetic()) })
}

#[derive(Args, Debug)]
pub struct CommonArgs {
    /// The stream size
    ///
    /// Defaults to the provided file size
    #[clap(short, long, value_parser = parse_size_arg)]
    pub size: Option<Size>,

    /// The number of parallel jobs
    ///
    /// Defaults to the number of physical cores on the host
    #[clap(short, long)]
    pub jobs: Option<usize>,

    /// The chunk size
    #[clap(short, long, default_value = "32ki", value_parser=|s: &str| parse_size(s))]
    pub chunk_size: u64,

    /// The size of each read or write
    ///
    /// Several consecutive chunks are read or written at once: each thread
    /// waits for its I/O, so on a device with a high latency (e.g. a virtual
    /// disk) small I/Os leave it mostly idle. Rounded down to a multiple of
    /// the chunk size (one chunk at least). The stream doesn't depend on it.
    #[clap(short, long, default_value = "1Mi", value_parser=|s: &str| parse_size(s))]
    pub io_size: u64,

    /// Hide the progress bar
    #[clap(short = 'P', long)]
    pub no_progress: bool,

    /// Open the file or device with O_DIRECT (bypass the page cache)
    ///
    /// Requires the chunk size, position and stream size to all be multiples
    /// of 4096. Ignored when reading from stdin or writing to stdout.
    #[clap(short = 'D', long)]
    pub direct: bool,
}

impl CommonArgs {
    /// The number of chunks read or written at once
    pub fn io_chunks(&self) -> usize {
        (self.io_size / self.chunk_size).max(1) as usize
    }
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Generate(GenerateArgs),
    Validate(ValidateArgs),
    Stress(StressArgs),
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
