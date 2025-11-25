use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender};

#[cfg(unix)]
use std::os::fd::AsRawFd as _;
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

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

#[cfg(target_os = "windows")]
mod blk {
    use std::io::Error;
    use std::mem::size_of;
    use std::os::windows::prelude::AsRawHandle;
    use windows_sys::Win32::Foundation::{FALSE, HANDLE};
    use windows_sys::Win32::System::IO::DeviceIoControl;

    pub fn get_size(file: &std::fs::File) -> Result<u64, Error> {
        let handle = file.as_raw_handle();
        let mut size: u64 = 0;
        let mut bytes_returned: u32 = 0;

        let result = unsafe {
            DeviceIoControl(
                handle as HANDLE,
                0x70000, // IOCTL_DISK_GET_LENGTH_INFO
                std::ptr::null_mut(),
                0,
                &mut size as *mut _ as *mut std::ffi::c_void,
                size_of::<u64>() as u32,
                &mut bytes_returned as *mut _,
                std::ptr::null_mut(),
            )
        };

        if result == FALSE { Err(Error::last_os_error()) } else { Ok(size) }
    }
}

pub fn read_file_size(path: &Path) -> anyhow::Result<u64> {
    let file_type = std::fs::metadata(path)?.file_type();
    #[cfg(unix)]
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
    } else if file_type.is_file() {
        Ok(path.metadata()?.len())
    } else {
        Err(anyhow::anyhow!("Unsupported file type"))
    }
    #[cfg(target_os = "windows")]
    {
        // On Windows, we try to open the file and get its size via DeviceIoControl
        // if it's not a regular file, it might be a block device.
        let file = File::open(path)?;
        if file_type.is_file() {
            Ok(path.metadata()?.len())
        } else {
            blk::get_size(&file)
                .map_err(|e| anyhow::anyhow!("Failed to get block device size: {}", e)) // Convert to anyhow::Result
        }
    }
    #[cfg(not(any(unix, target_os = "windows")))]
    {
        if file_type.is_file() {
            Ok(path.metadata()?.len())
        } else {
            Err(anyhow::anyhow!("Block device support not implemented for this OS"))
        }
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
