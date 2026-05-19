use std::fs::File;
use std::io;
use std::io::IsTerminal as _;
use std::os::fd::AsRawFd as _;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};
use std::{io::Read, os::unix::fs::FileTypeExt, path::Path};

use human_units::{FormatDuration, FormatSize as _};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

extern crate log;
use log::{debug, info};

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

/// Alignment (in bytes) required for O_DIRECT I/O. 4096 is a safe superset of
/// the common 512- and 4096-byte logical sector sizes.
pub const DIRECT_ALIGN: usize = 4096;

/// Round `size` down to the previous multiple of [`DIRECT_ALIGN`] so it can be
/// used with O_DIRECT. The stream size is adjusted automatically (rather than
/// rejected) because it is often derived from a non-aligned `--size` value.
pub fn align_size_for_direct(size: u64) -> u64 {
    size - size % DIRECT_ALIGN as u64
}

/// The stream size aligned for O_DIRECT. A size the user wrote with a unit
/// suffix (`has_unit`) must already be a multiple of [`DIRECT_ALIGN`] — it is
/// an error otherwise, so the chosen human value is not silently changed. A
/// raw byte count (no unit, including a file-derived size) is rounded down to
/// the nearest multiple instead — unless it is smaller than [`DIRECT_ALIGN`],
/// which rounds to 0 and is reported as an error rather than a silent no-op.
/// With `direct` off the size is returned as-is.
pub fn direct_aligned_stream_size(size: u64, has_unit: bool, direct: bool) -> anyhow::Result<u64> {
    if !direct {
        return Ok(size);
    }
    let aligned = align_size_for_direct(size);
    if aligned == size {
        return Ok(size);
    }
    if has_unit {
        return Err(anyhow::anyhow!(
            "--direct requires the size ({size}) to be a multiple of {DIRECT_ALIGN}; \
             use an aligned value (e.g. a `Ki`/`Mi` multiple) or pass a raw byte \
             count to have it rounded down automatically"
        ));
    }
    if aligned == 0 {
        return Err(anyhow::anyhow!(
            "--direct requires a size of at least {DIRECT_ALIGN} bytes, but {size} \
             rounds down to 0"
        ));
    }
    debug!("stream size {size} rounded down to {aligned} for O_DIRECT");
    Ok(aligned)
}

/// Ensure the user-provided knobs satisfy the O_DIRECT alignment requirements,
/// returning a helpful error otherwise (the kernel would only return a bare
/// `EINVAL`). The stream size is handled separately by [`align_size_for_direct`].
pub fn check_direct_alignment(chunk_size: u64, position: u64) -> anyhow::Result<()> {
    let align = DIRECT_ALIGN as u64;
    for (name, value) in [("chunk size", chunk_size), ("position", position)] {
        if value % align != 0 {
            return Err(anyhow::anyhow!(
                "--direct requires the {name} ({value}) to be a multiple of {align}"
            ));
        }
    }
    Ok(())
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

/// Metrics wrapper for tracking elapsed time, bytes processed, and throughput
pub struct Metrics {
    pub progress: Option<Progress>,
    pub start_time: Instant,
    pub bytes_processed: u64,
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

impl Metrics {
    /// Create a new metrics tracker
    pub fn new(stream_size: Option<u64>, no_progress: bool) -> anyhow::Result<Self> {
        Ok(Metrics {
            progress: Progress::new(stream_size, no_progress)?,
            start_time: Instant::now(),
            bytes_processed: 0,
        })
    }

    /// Format and log interrupt summary at DEBUG level
    pub fn log_interrupt_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            (self.bytes_processed as f64 / elapsed.as_secs_f64()) as usize
        } else {
            0
        };

        debug!("Total time: {}", elapsed.format_duration());
        debug!("Throughput: {}/s", throughput.format_size());
        debug!("Total bytes written/read: {}", self.bytes_processed.format_size());
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

/// Tracks the peak transfer rate, computed over ~1 second windows, from a
/// stream of cumulative byte counts.
pub struct PeakRate {
    window_start: Instant,
    window_start_bytes: u64,
    peak: f64,
}

impl Default for PeakRate {
    fn default() -> Self {
        PeakRate { window_start: Instant::now(), window_start_bytes: 0, peak: 0.0 }
    }
}

impl PeakRate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the latest cumulative byte count; the peak is updated at most once
    /// per window.
    pub fn sample(&mut self, total_bytes: u64) {
        let elapsed = self.window_start.elapsed();
        if elapsed >= Duration::from_secs(1) {
            let rate = (total_bytes - self.window_start_bytes) as f64 / elapsed.as_secs_f64();
            self.peak = self.peak.max(rate);
            self.window_start = Instant::now();
            self.window_start_bytes = total_bytes;
        }
    }

    /// The highest windowed rate observed so far, in bytes per second.
    pub fn peak(&self) -> f64 {
        self.peak
    }
}

/// Drain the progress channel, update the progress display, and return the
/// peak transfer rate (bytes/s) observed during the run.
pub fn receive_progress(pb: &mut Option<Progress>, rx: &Receiver<u64>, tx: Sender<u64>) -> f64 {
    drop(tx);
    let mut total_bytes = 0;
    let mut peak = PeakRate::new();
    if let Some(p) = pb {
        while let Ok(bytes) = rx.recv() {
            total_bytes += bytes;
            peak.sample(total_bytes);
            p.tick(total_bytes);
        }
        p.finish();
    } else {
        while let Ok(bytes) = rx.recv() {
            total_bytes += bytes;
            peak.sample(total_bytes);
        }
    }
    peak.peak()
}

/// Log debug metrics: elapsed time, throughput, and bytes processed
pub fn log_metrics(start: Instant, bytes: u64, label: &str) {
    let elapsed = start.elapsed();
    let throughput = if elapsed.as_micros() > 0 {
        (bytes as f32 / elapsed.as_micros() as f32 * 1_000_000.0) as usize
    } else {
        0
    };

    debug!("{label}: {bytes}");
    debug!("throughput: {}/s", throughput.format_size());
    debug!("run in {}", elapsed.format_duration());
}

/// Log an end-of-run summary at INFO level: total bytes, elapsed time, and the
/// average and peak transfer rates.
pub fn log_summary(start: Instant, bytes: u64, peak_rate: f64) {
    let elapsed = start.elapsed();
    let average =
        if elapsed.as_secs_f64() > 0.0 { (bytes as f64 / elapsed.as_secs_f64()) as u64 } else { 0 };
    // For runs shorter than one peak window no windowed rate is recorded;
    // fall back to the average so `max` is never reported below it.
    let max = (peak_rate as u64).max(average);
    info!(
        "summary: {} in {} - average {}/s, max {}/s",
        bytes.format_size(),
        elapsed.format_duration(),
        average.format_size(),
        max.format_size(),
    );
}
