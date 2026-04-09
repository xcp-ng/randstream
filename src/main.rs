#[macro_use]
extern crate log;

use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use randstream::cli;

use randstream::generate::generate;
use randstream::validate::validate;

fn run() -> anyhow::Result<i32> {
    let cli = cli::Cli::parse();
    if let Some(level) = cli.verbose.log_level() {
        ocli::init(level).unwrap();
    }

    // Initialize the cancel flag
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Install signal handler
    let cancel_clone = cancel.clone();
    ctrlc::set_handler(move || {
        cancel_clone.store(true, Ordering::Relaxed);
    })?;

    match &cli.command.unwrap() {
        cli::Commands::Generate(args) => generate(args, cancel),
        cli::Commands::Validate(args) => validate(args, cancel),
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
