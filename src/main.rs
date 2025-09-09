#[macro_use]
extern crate log;

use clap::Parser;

use randstream::cli;

use randstream::generate::generate;
use randstream::validate::validate;

fn run() -> anyhow::Result<i32> {
    let cli = cli::Cli::parse();
    if let Some(level) = cli.verbose.log_level() {
        ocli::init(level).unwrap();
    }

    match &cli.command.unwrap() {
        cli::Commands::Generate(args) => generate(args),
        cli::Commands::Validate(args) => validate(args),
    }
}

fn main() {
    match run() {
        Ok(exit_code) => std::process::exit(exit_code),
        Err(err) => {
            error!("{err}");
            std::process::exit(1);
        }
    }
}
