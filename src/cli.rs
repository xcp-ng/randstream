use clap::{Parser, Subcommand, command};
use clap_verbosity_flag::{InfoLevel, Verbosity};

use crate::{generate::GenerateArgs, validate::ValidateArgs};

/// A simple tool to generate a random stream and validate it
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
