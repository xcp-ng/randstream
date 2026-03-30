use std::fs::File;
use std::io;
use std::io::IsTerminal as _;
use std::os::fd::AsRawFd as _;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};
use std::{io::Read, os::unix::fs::FileTypeExt, path::Path};

use human_units::FormatSize as _;
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

/// Progress tracking for TTY (animated bar) or non-TTY (periodic log lines)
#[derive(Debug)]
pub struct LogProgress {
    stream_size: u64,
    last_print: Instant,
    last_pct: u8,
    prev_bytes: u64,
    interval_bytes: u64,
    interval_start: Instant,
}

pub enum Progress {
    Bar(ProgressBar),
    Log(LogProgress),
}

impl Progress {
    /// Create a new progress tracker. Returns `None` if progress is disabled or cannot be tracked.
    pub fn new(stream_size: Option<u64>, no_progress: bool) -> anyhow::Result<Option<Self>> {
        if no_progress {
            return Ok(None);
        }
        if std::io::stderr().is_terminal() {
            Ok(Some(Progress::Bar(set_up_progress_bar(stream_size)?)))
        } else if let Some(size) = stream_size {
            // Non-TTY with known size: use log-based progress
            Ok(Some(Progress::Log(LogProgress {
                stream_size: size,
                last_print: Instant::now(),
                last_pct: 0,
                prev_bytes: 0,
                interval_bytes: 0,
                interval_start: Instant::now(),
            })))
        } else {
            // Non-TTY with unknown size (stdin, no --size): no progress
            Ok(None)
        }
    }

    /// Update progress with cumulative bytes processed
    pub fn tick(&mut self, bytes_done: u64) {
        match self {
            Progress::Bar(pb) => pb.set_position(bytes_done),
            Progress::Log(lp) => lp.tick(bytes_done),
        }
    }

    /// Finish progress tracking
    pub fn finish(&mut self) {
        if let Progress::Bar(pb) = self {
            pb.finish_and_clear();
        }
    }
}

impl LogProgress {
    fn tick(&mut self, bytes_done: u64) {
        // Compute current percentage milestone (floored to 10%)
        let pct = ((bytes_done * 100 / self.stream_size) / 10 * 10) as u8;
        let now = Instant::now();

        // Check dual gate: milestone crossed AND 60s elapsed
        if pct >= self.last_pct + 10
            && now.duration_since(self.last_print) >= Duration::from_secs(60)
        {
            let delta = bytes_done - self.prev_bytes;
            self.interval_bytes += delta;

            let elapsed = now.duration_since(self.interval_start).as_micros() as f32;
            let throughput = (self.interval_bytes as f32 / elapsed * 1_000_000.0) as usize;

            eprintln!("progress: {pct}% - {}/s", throughput.format_size());

            self.last_pct = pct;
            self.last_print = now;
            self.interval_bytes = 0;
            self.interval_start = now;
        } else {
            // Always accumulate delta for throughput computation
            let delta = bytes_done - self.prev_bytes;
            self.interval_bytes += delta;
        }
        self.prev_bytes = bytes_done;
    }
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

pub fn receive_progress(pb: &mut Option<Progress>, rx: &Receiver<u64>, tx: Sender<u64>) {
    drop(tx);
    let mut total_bytes = 0;
    if let Some(p) = pb {
        while let Ok(bytes) = rx.recv() {
            total_bytes += bytes;
            p.tick(total_bytes);
        }
        p.finish();
    } else {
        while rx.recv().is_ok() {}
    }
}
