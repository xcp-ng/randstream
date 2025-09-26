use clap::{Args, Parser, Subcommand, command};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use human_units::Size;

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
    pub command: Option<Commands>,

    #[command(flatten)]
    pub verbose: Verbosity<InfoLevel>,
}

#[derive(Args, Debug)]
pub struct CommonArgs {
    /// The stream size
    ///
    /// Defaults to the provided file size
    #[clap(short, long, value_parser=clap::value_parser!(Size))]
    pub size: Option<Size>,

    /// The number of parallel jobs
    ///
    /// Defaults to the number of physical cores on the host
    #[clap(short, long)]
    pub jobs: Option<usize>,

    /// The chunk size
    #[clap(short, long, default_value = "32k", value_parser=clap::value_parser!(Size))]
    pub chunk_size: Size,

    /// Hide the progress bar
    #[clap(short, long)]
    pub no_progress: bool,
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
