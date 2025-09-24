use std::{os::unix::fs::FileTypeExt, path::Path};

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
