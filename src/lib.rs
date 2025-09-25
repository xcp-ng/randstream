use std::io;
use std::sync::mpsc::{Receiver, Sender};
use std::{io::Read, os::unix::fs::FileTypeExt, path::Path};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

extern crate log;

pub mod cli;
pub mod generate;
pub mod validate;

pub fn read_file_size(file: &Path) -> anyhow::Result<u64> {
    let file_type = std::fs::metadata(file)?.file_type();
    if file_type.is_block_device() {
        let basename = file.file_name().unwrap().display();
        let size: u64 =
            std::fs::read_to_string(format!("/sys/block/{basename}/size"))?.trim().parse()?;
        let block_size: u64 =
            std::fs::read_to_string(format!("/sys/block/{basename}/queue/physical_block_size"))?
                .trim()
                .parse()?;
        Ok(size * block_size)
    } else {
        Ok(file.metadata()?.len())
    }
}

fn read_exact_or_eof(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut bytes_read = 0;
    while bytes_read < buffer.len() {
        let n = reader.read(&mut buffer[bytes_read..])?;
        if n == 0 {
            break;
        }
        bytes_read += n;
    }
    Ok(bytes_read)
}

fn set_up_progress_bar(stream_size: Option<u64>) -> anyhow::Result<ProgressBar> {
    let pb = ProgressBar::with_draw_target(stream_size, ProgressDrawTarget::stderr_with_hz(10));
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )?
        .progress_chars("=> "),
    );
    Ok(pb)
}

fn receive_progress(pb: &Option<ProgressBar>, rx: &Receiver<u64>, tx: Sender<u64>) {
    drop(tx);
    let mut total_bytes = 0;
    if let Some(pb) = pb {
        while let Ok(bytes) = rx.recv() {
            total_bytes += bytes;
            pb.set_position(total_bytes);
        }
        pb.finish_and_clear();
    } else {
        while rx.recv().is_ok() {}
    }
}
