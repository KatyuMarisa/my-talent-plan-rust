// use std::sync::{Mutex, Arc};
// use std::{path::PathBuf};
// use std::fs::{OpenOptions, create_dir_all, remove_file};
// 
// use lockfree::map::Map as ThreadSafeMap;
// use memmap::MmapMut;
// 
// use crate::Result;
// use super::dbfile::Pos;
// use super::manifest2::FileId;
// use super::memtable::Memtable;
// 
// type RID = (FileId, Pos);
// 
// pub struct KvStore {
//     manifest: Arc<Mutex<Manifest>>,
//     memtable: Arc<Memtable>,
//     sstables: Arc<ThreadSafeMap<file_id, i32>>,
// }
// 
// pub trait ImmFile {
//     fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
//     fn read(&self, offset: usize, length: usize) -> Result<&[u8]>;
//     fn remove(self) -> Result<()>;
// }
// 
// pub trait WritableFile {
//     fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
//     fn write(&mut self, offset: usize, data: &[u8]) -> Result<()>;
//     fn sync(&mut self) -> Result<()>;
//     fn close(self) -> Result<()>;
//     fn remove(self) -> Result<()>;
// }
// 
// pub struct MmapFile {
//     mmap: memmap::MmapMut,
//     path: PathBuf,
// }
// 
// impl ImmFile for MmapFile {
//     fn open(path: impl Into<PathBuf>) -> Result<Self> {
//         let pb: PathBuf = path.into();
//         let f = OpenOptions::new()
//             .create(false)
//             .read(true)
//             .write(true)
//             .open(&pb)?;
// 
//         Ok (Self{
//             mmap: unsafe { MmapMut::map_mut(&f)? },
//             path: pb
//         })
//     }
// 
//     fn read(&self, offset: usize, length: usize) -> Result<&[u8]> {
//         Ok (
//             &self.mmap[offset..offset + length]
//         )
//     }
// 
//     fn remove(self) -> Result<()> {
//         Ok (
//             remove_file(self.path)?
//         )
//     }
// }
// 
// impl WritableFile for MmapFile {
//     fn new(path: impl Into<PathBuf>) -> Result<Self> {
//         let pb: PathBuf = path.into();
//         create_dir_all(pb.parent().expect("invalid path"))?;
//         let f = OpenOptions::new()
//             .create_new(true)
//             .read(true)
//             .write(true)
//             .open(&pb)?;
// 
//         f.set_len(FILE_SIZE_LIMIT as u64)?;
//         Ok(Self{
//             mmap: unsafe { MmapMut::map_mut(&f)? },
//             path: pb
//         })
//     }
// 
//     fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
//         Ok (
//             (&mut self.mmap[offset..offset + data.len()]).copy_from_slice(data)
//         )
//     }
// 
//     fn sync(&mut self) -> Result<()> {
//         Ok (
//             self.mmap.flush()?
//         )
//     }
// 
//     fn close(self) -> Result<()> {
//         drop(self.mmap);
//         Ok(())
//     }
// 
//     fn remove(self) -> Result<()> {
//         Ok (
//             remove_file(self.path)?
//         )
//     }
// }
// 
// 