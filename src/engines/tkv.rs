// use std::fs::{OpenOptions, create_dir_all, remove_file};
// use std::{collections::HashMap, path::PathBuf};
// use std::sync::{Mutex, Arc};

// use lockfree::map::Map as MemTable;
// use memmap::MmapMut;

// use crate::errors::{Result};
// use crate::dbfile::{Pos, FILE_SIZE_LIMIT};
// use crate::manifest::{file_id};

// type RID = (file_id, Pos);

// pub trait ThreadSafeStorate: Send {
//     fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
//     fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
//     fn read(&self, offset: usize, length: usize) -> Result<&[u8]>;
//     fn write(&mut self, offset: usize, data: &[u8]) -> Result<()>; 
//     fn sync(&mut self) -> Result<()>;
//     fn close(self) -> Result<()>;
//     fn remove(self) -> Result<()>;
// }

// pub struct SSTable {
//     mmap: memmap::MmapMut,
//     path: PathBuf,
// }

// impl ThreadSafeStorate for SSTable {
//     fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
//         let pb: PathBuf = path.into();
//         let f = OpenOptions::new()
//             .create(false)
//             .read(true)
//             .write(true)
//             .open(&pb)?;

//         Ok (Self{
//             mmap: unsafe { MmapMut::map_mut(&f)? },
//             path: pb
//         })
//     }

//     fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
//         let pb: PathBuf = path.into();
//         create_dir_all(pb.parent().expect("invalid path"))?;
//         let f = OpenOptions::new()
//             .create_new(true)
//             .read(true)
//             .write(true)
//             .open(&pb)?;

//         f.set_len(FILE_SIZE_LIMIT as u64)?;
//         Ok(Self{
//             mmap: unsafe { MmapMut::map_mut(&f)? },
//             path: pb
//         })
        
//     }

//     fn read(&self, offset: usize, length: usize) -> Result<&[u8]> {
//         Ok (
//             &self.mmap[offset..offset + length]
//         )
//     }

//     fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
//         Ok (
//             (&mut self.mmap[offset..offset + data.len()]).copy_from_slice(data)
//         )
//     }

//     fn sync(&mut self) -> Result<()> {
//         Ok (
//             self.mmap.flush()?
//         )
//     }

//     fn remove(self) -> Result<()> {
//         Ok (
//             remove_file(self.path)?
//         )
//     }

//     fn close(self) -> Result<()> {
//         drop(self.mmap);
//         Ok(())
//     }
// }


// #[derive(Clone)]
// pub struct ThreadSafeKVStore {
//     manifest: Arc<Mutex<SSTable>>,
//     offsets: Arc<HashMap<String, RID>>,
//     sstables: Arc<HashMap<file_id, Arc<SSTable>>>,
//     memtable: Arc<MemTable<String, Vec<u8>>>,
//     uncompact: u64,
//     compact_limit: u64,
// }

// pub trait SafeKvsEngine: Clone + Send + 'static {
//     fn set(&self, key: String, value: String) -> Result<()>;

//     fn get(&self, key: String) -> Result<Option<String>>;

//     fn remove(&self, key: String) -> Result<()>;
// }


// impl SafeKvsEngine for ThreadSafeKVStore {
//     fn set(&self, key: String, value: String) -> Result<()> {
//         // self.offsets.insert(key, (0, (0, 0)));
//         // Ok(())
//         Ok(())
//     }

//     fn get(&self, key: String) -> Result<Option<String>> {
//         // bincode::deserialize(&self.get(key))?;
//         if let Some((fid, pos)) = self.offsets.get(&key) {
//             if 0 == *fid {

//             }
//             // let data = match *fid {
//             //     0 => { self.memtable.get(&key).unwrap().val().as_slice() }

//             //     _ => { self.sstables.get(fid).unwrap().read(0, 24)? }
//             // };
//             // let value = bincode::deserialize(&data)?;
//             return Ok(Some(value))
//         }

//         return Ok(None)

//         // match self.offsets.get(&key) {
//         //     Some((fid, pos)) => {
//         //         if fid == 0 {

//         //         } else {
//         //             match self.memtable.get(&key)

//         //         }
//         //     }

//         //     None => {
//         //         Ok(None)
//         //     }
//         // }
//     }

//     fn remove(&self, key: String) -> Result<()> {
//         todo!()
//     }
// }
