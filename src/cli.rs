use clap::{Parser, Subcommand, command};
use clap_verbosity_flag::{InfoLevel, Verbosity};

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
