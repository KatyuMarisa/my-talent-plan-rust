use crate::Result;
use super::{Storage, FILE_SIZE_LIMIT};

use memmap::MmapMut;

use std::fs::{create_dir_all, OpenOptions, remove_file};
use std::path::PathBuf;

pub struct MmapFile {
    mmap: MmapMut,
    path: PathBuf,
}

impl Storage for MmapFile {
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
        let pb: PathBuf = path.into();
        let f = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .open(&pb)?;

        Ok (Self{
            mmap: unsafe { MmapMut::map_mut(&f)? },
            path: pb
        })
    }

    fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
        let pb: PathBuf = path.into();
        create_dir_all(pb.parent().expect("invalid path"))?;
        let f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&pb)?;

        f.set_len(FILE_SIZE_LIMIT as u64)?;
        Ok(Self{
            mmap: unsafe { MmapMut::map_mut(&f)? },
            path: pb
        })
        
    }

    fn read(&self, offset: usize, length: usize) -> Result<&[u8]> {
        Ok (
            &self.mmap[offset..offset + length]
        )
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        (&mut self.mmap[offset..offset + data.len()]).copy_from_slice(data);
        Ok (())
    }

    fn sync(&mut self) -> Result<()> {
        Ok (
            self.mmap.flush()?
        )
    }

    fn remove(self) -> Result<()> {
        Ok (
            remove_file(self.path)?
        )
    }

    fn close(self) -> Result<()> {
        drop(self.mmap);
        Ok(())
    }
}

