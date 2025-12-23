use std::io::Write;

use clap::{Args, Parser};
use toml_edit::{DocumentMut, value};
use xshell::{Shell, cmd};

/// Utility commands
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
#[command(name = "cargo xtask")]
#[command(bin_name = "cargo xtask")]
enum Command {
    /// Bump the version and create a new draft release on github
    Release(Release),
}

#[derive(Args, Debug)]
struct Release {
    /// The new version number
    version: String,
}

fn main() -> anyhow::Result<()> {
    let args = Command::parse();
    match args {
        Command::Release(args) => release(&args)?,
    };
    Ok(())
}

fn release(args: &Release) -> anyhow::Result<()> {
    let sh = Shell::new()?;
    // update the workspace Cargo.toml
    let toml = std::fs::read_to_string("Cargo.toml")?;
    let mut doc = toml.parse::<DocumentMut>()?;
    doc["workspace"]["package"]["version"] = value(&args.version);
    std::fs::File::create("Cargo.toml")?.write_all(doc.to_string().as_bytes())?;
    cmd!(sh, "cargo test").run()?;
    let version = &args.version;
    // commit, tag and push
    let message = format!("Bump version to {version}");
    cmd!(sh, "git commit -am {message}").run()?;
    cmd!(sh, "git tag -am {version} {version}").run()?;
    cmd!(sh, "git push").run()?;
    cmd!(sh, "git push --tags").run()?;
    eprintln!("Now wait for the release worflow to complete and publish the release");
    Ok(())
}
