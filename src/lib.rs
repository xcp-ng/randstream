use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;
use std::sync::mpsc::{Receiver, Sender};
use std::{io::Read, os::unix::fs::FileTypeExt, path::Path};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

extern crate log;

pub mod cli;
pub mod generate;
pub mod validate;

#[cfg(target_os = "linux")]
mod blk {
    use nix::ioctl_read;
    ioctl_read!(blkgetsize64, 0x12, 114, u64);
}

#[cfg(target_os = "freebsd")]
mod blk {
    use nix::ioctl_read;
    ioctl_read!(diocgmediasize, b'd', 129, u64);
}

pub fn read_file_size(path: &Path) -> anyhow::Result<u64> {
    let file_type = std::fs::metadata(path)?.file_type();
    if file_type.is_block_device() || file_type.is_char_device() {
        let file = File::open(path)?;
        let fd = file.as_raw_fd();

        #[cfg(target_os = "linux")]
        unsafe {
            let mut size: u64 = 0;
            blk::blkgetsize64(fd, &mut size).map_err(|e| io::Error::from_raw_os_error(e as i32))?;
            Ok(size)
        }

        #[cfg(target_os = "freebsd")]
        unsafe {
            let mut size: u64 = 0;
            blk::diocgmediasize(fd, &mut size)
                .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
            Ok(size)
        }
    } else {
        Ok(path.metadata()?.len())
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
        .progress_chars(if supports_unicode::on(supports_unicode::Stream::Stdout) {
            "█▉▊▋▌▍▎▏  "
        } else {
            "=> "
        }),
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
