use crate::Result;

use std::path::PathBuf;

mod writer;
mod mmapfile;

const PAGE_SIZE: usize = 1 << 12;
const PAGES_PER_FILE: usize = 1 << 8;

pub trait Storage: Sync + Send {
    fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
    fn read(&self, offset: usize, length: usize) -> Result<&[u8]>;
    fn write(&mut self, offset: usize, data: &[u8]) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn close(self) -> Result<()>;
    fn remove(self) -> Result<()>;
}

pub use mmapfile::MmapFile;
pub use writer::BlockWriter;

pub const FILE_SIZE_LIMIT: usize = PAGE_SIZE * PAGES_PER_FILE;