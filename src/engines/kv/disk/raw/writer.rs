use crate::Result;
use super::{Storage, FILE_SIZE_LIMIT};

use std::fs::{OpenOptions, remove_file};
use std::io::Write;
use std::path::PathBuf;

/// A self-defined buffer.
/// Avoid using BufWriter, because it doesn't help on large block writing.
pub struct BlockWriter {
    writer: std::fs::File,
    path: PathBuf,
    buf: Vec<u8>,
}

impl Storage for BlockWriter {
    fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        let writer = OpenOptions::new()
            .create(true)
            .read(false)
            .write(true)
            .open(&path)?;
        let mut buf = Vec::with_capacity(FILE_SIZE_LIMIT);
        buf.resize(FILE_SIZE_LIMIT, 0);
        Ok( Self{ writer, path, buf } )
    }

    fn open(_: impl Into<PathBuf>) -> Result<Self> {
        unimplemented!("BlockWriter doesn't support this method")
    }

    fn read(&self, _: usize, _: usize) -> Result<&[u8]> {
        unimplemented!("BlockWriter doesn't support this method")
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        self.buf[offset .. offset+data.len()].copy_from_slice(data);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.writer.write_all(&self.buf)?;
        Ok(self.writer.flush()?)
    }

    fn close(mut self) -> Result<()> {
        self.sync()
    }

    fn remove(self) -> Result<()> {
        Ok(remove_file(self.path)?)
    }
}
