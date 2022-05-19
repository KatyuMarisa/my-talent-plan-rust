// use std::{collections::HashMap, path::PathBuf};

// use bincode::deserialize;

// use crate::engines::KvsEngine;
// use crate::errors::{Result, KvError};
// use crate::dbfile::{Pos, Storage, DefaultHeader, FixSizedHeader, self, FILE_SIZE_LIMIT, MmapFile};
// use crate::kvfile::{KVSSTable, MAGIC_KV, KVRecordKind, KVRecord};
// use crate::manifest::{file_id, Manifest, INVALID_FILE_ID};

// type RID = (file_id, Pos);

// pub struct KvStore {
//     manifest: Manifest,
//     kv: HashMap<String, RID>,
//     handles: HashMap<file_id, KVSSTable>,
//     writer_handle: file_id,
//     compact_limit: u64,
//     tot_size: u64,
// }

// impl KvStore {
//     pub fn open(root_dir: impl Into<PathBuf>) -> Result<Self> {
//         let (manifest, files) = Manifest::open(root_dir)?;
//         let mut store = Self {
//             manifest,
//             kv: HashMap::new(),
//             handles: HashMap::new(),
//             writer_handle: INVALID_FILE_ID,
//             compact_limit: 3 * FILE_SIZE_LIMIT as u64,
//             tot_size: 0
//         };
    
//         store.init(files)?;
//         Ok(store)
//     }

//     pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self> {
//         let mut root_dir: PathBuf = root_dir.into();
//         root_dir.push("MANIFEST");
//         if std::path::Path::new(&root_dir).exists() {
//             root_dir.pop();
//             KvStore::open(root_dir)
//         } else {
//             root_dir.pop();
//             let (manifest, files) = Manifest::new(root_dir)?;
//             let mut store = Self {
//                 manifest,
//                 kv: HashMap::new(),
//                 handles: HashMap::new(),
//                 writer_handle: INVALID_FILE_ID,
//                 compact_limit: 3 * FILE_SIZE_LIMIT as u64,
//                 tot_size: 0
//             };
//             store.init(files)?;
//             Ok(store)
//         }

//     }

//     fn init(&mut self, mut files: Vec<(file_id, Box<dyn Storage>)>) -> Result<()> {
//         // sorted by file_id
//         files.sort_by(|a, b| {
//             a.0.cmp(&b.0)
//         });
//         for (fid, file) in files {
//             self.handle_opened_file(fid, file)?;  
//         }
//         // the handle with max file_id is the writable handle. if there is no handle in use, create
//         // a new handle.
//         if self.handles.is_empty() {
//             self.new_writable_kv_file()?;
//         } else {
//             self.writer_handle = self.handles.keys().into_iter().max().unwrap().to_owned();
//         }

//         self.compact_limit = self.tot_size * 3 / 2;

//         Ok(())
//     }

//     fn handle_opened_file(&mut self, fid: file_id, file: Box<dyn Storage>) -> Result<()> {
//         const MAGIC_INVALID: u8 = 255;
//         let h: DefaultHeader<MAGIC_INVALID> = deserialize(file.read(0, 24)?)?;
//         match h.magic() {
//             MAGIC_KV => {
//                 let kv_file = KVSSTable::open(file, false)?;
//                 for (idx, record) in kv_file.all_records()? {
//                     if record.kind == KVRecordKind::TOMB {
//                         self.kv.remove(&record.key);
//                     } else {
//                         self.kv.insert(record.key, (fid, idx));
//                     }
//                 }
//                 self.tot_size += dbfile::FILE_SIZE_LIMIT as u64;
//                 self.handles.insert(fid, kv_file);
//                 Ok(())
//             }
//             _ => {
//                 Err(KvError::MaybeCorrput.into())
//             }
//         }
//     }

//     fn write_kv_file(&mut self, record: &KVRecord) -> Result<RID> {
//         let writer = self.handles.get_mut(&self.writer_handle).unwrap();
//         match writer.append_record(record) {
//             Ok(pos) => {
//                 return Ok((self.writer_handle, pos))
//             }

//             Err(err) => {
//                 // TODO:
//                 if let Some(KvError::FileSizeExceed) = err.downcast_ref::<KvError>() {
//                     if self.tot_size <= self.compact_limit {
//                         self.new_writable_kv_file()?;
//                     } else {
//                         self.maybe_compact()?;
//                     }
//                     self.write_kv_file(record)
//                 } else {
//                     return Err(err)
//                 }
//             }
//         }
//     }

//     fn new_writable_kv_file(&mut self) -> Result<()> {
//         let (fid, handle) = self.manifest.create_file()?;
//         self.manifest.validate_file(fid)?;
//         self.handles.insert(fid, KVSSTable::open(handle, true)?);
//         self.writer_handle = fid;
//         self.tot_size += FILE_SIZE_LIMIT as u64;
//         Ok(())
//     }

//     fn maybe_compact(&mut self) -> Result<()> {
//         if self.tot_size < self.compact_limit {
//             return Ok(())
//         }

//         let v: Vec<(String, RID)> = self.kv.clone().into_iter().collect();
//         let mut all = v.into_iter().map(|r| {
//             let (key, rid) = r;
//             let value = self.get(key.clone()).unwrap().unwrap();
//             ((key, value), rid)
//         }).collect::<Vec<_>>();

//         // sort by fild_id, so that we can fully use cache
//         all.sort_by(|a, b| {
//             a.1.0.cmp(&b.1.0)
//         });

//         // file_id that should be validate/removed after compaction
//         let mut added = Vec::<file_id>::new();
//         let (mut current_fid, mut temp_file) = self.manifest.create_file()?;
//         let mut current_file = KVSSTable::open(temp_file, true)?;
//         added.push(current_fid);
//         let removed = self.handles.keys().cloned().collect();
//         // new kv map
//         let mut kv = HashMap::new();
//         // total file size
//         let mut size_after = FILE_SIZE_LIMIT as u64;

//         let mut iter = all.iter();
//         // TODO: try use `move` instead of copy
//         while let Some(((key, value), _)) = iter.next() {
//             let record = KVRecord {
//                 kind: KVRecordKind::KV, 
//                 key: key.to_string(), 
//                 value: value.to_string()
//             };

//             match current_file.append_record(&record)
//             {
//                 Ok(pos) => {
//                     kv.insert(key.to_owned(), (current_fid, pos));
//                 }

//                 Err(err) => {
//                     if let Some(KvError::FileSizeExceed) = err.downcast_ref::<KvError>() {
//                         current_file.sync()?;
//                         (current_fid, temp_file) = self.manifest.create_file()?;
//                         current_file = KVSSTable::open(temp_file, true)?;
//                         size_after += FILE_SIZE_LIMIT as u64;
//                         added.push(current_fid);
//                         let final_pos = current_file.append_record(&record)?;
//                         kv.insert(record.key, (current_fid, final_pos));
//                     } else {
//                         return Err(err)
//                     }
//                 }
//             }
//         }

//         drop(current_file);
//         let size_before = self.tot_size;
//         assert!(size_after <= size_before);
//         // assert!(size_after <= self.compact_limit);
//         // dynamiclly change compact trigger
//         if 12 * size_after / 10 >= self.compact_limit {
//             self.compact_limit *= 2;
//         } else if size_after < self.compact_limit / 4 && self.compact_limit > 10 * FILE_SIZE_LIMIT as u64 {
//             self.compact_limit /= 2;
//         }

//         // persist
//         // TODO: 
//         self.manifest.atomic_add_remove(added.clone(), removed)?;
//         let mut handles = HashMap::new();
//         for fid in added {
//             let handle = KVSSTable::open(
//                 Box::new(MmapFile::open(self.manifest.filename(fid))?)
//                 , false)?;
//             handles.insert(fid, handle);
//         }

//         self.kv = kv;
//         self.writer_handle = current_fid;
//         self.tot_size = size_after;
//         self.handles = handles;

//         Ok(())
//     }
// }

// impl KvsEngine for KvStore {
//     fn set(&mut self, key: String, value: String) -> Result<()> {
//         let record = KVRecord {
//             key,
//             value,
//             kind: KVRecordKind::KV
//         };
//         let rid = self.write_kv_file(&record)?;
//         self.kv.insert(record.key, rid);
//         Ok(())
//     }

//     fn get(&mut self, key: String) -> Result<Option<String>> {
//         if let Some(rid) = self.kv.get(&key) {
//             let (fid, pos) = rid;
//             let reader = self.handles.get(fid).unwrap();
//             let record = reader.read_record_at(pos.0, pos.1)?;
//             if record.kind == KVRecordKind::TOMB {
//                 return Ok(None)
//             } else {
//                 return Ok(Some(record.value))
//             }            
//         } else {
//             return Ok(None)
//         }
//     }

//     fn remove(&mut self, key: String) -> Result<()> {
//         if !self.kv.contains_key(&key) {
//             return Err(KvError::KeyNotFoundError{key}.into())
//         }

//         let record = KVRecord {
//             key,
//             value: "".to_owned(),
//             kind: KVRecordKind::TOMB
//         };
//         self.write_kv_file(&record)?;
//         self.kv.remove(&record.key);
//         Ok(())
//     }

// }

// #[cfg(test)]
// mod kvstore_unit_test {
//     use crate::{Result, KvStore, dbfile::FILE_SIZE_LIMIT};
//     use crate::engines::KvsEngine;

//     // TODO:
//     // 1) create_file要删除可能重复的文件
//     // 2) 把泛型和life time specifier搞懂

//     #[test]
//     fn basic_test() -> Result<()> {
//         let root = tempfile::tempdir()?;
//         let root_dir = root.path();
//         let mut store = KvStore::new(root_dir)?;
//         assert_eq!(store.tot_size, FILE_SIZE_LIMIT as u64);

//         for i in 0..20 {
//             store.set(i.to_string(), i.to_string())?;
//         }
//         for i in 0..20 {
//             assert_eq!(store.get(i.to_string())?.unwrap(), i.to_string());
//         }
//         for i in 20..40 {
//             assert_eq!(store.get(i.to_string())?, None);
//         }
//         for i in 0..10 {
//             store.remove(i.to_string())?;
//         }
//         for i in 0..10 {
//             assert_eq!(store.get(i.to_string())?, None);
//         }
//         for i in 10..20 {
//             assert_eq!(store.get(i.to_string())?.unwrap(), i.to_string());
//         }
//         assert_eq!(store.tot_size, FILE_SIZE_LIMIT as u64);

//         drop(store);

//         store = KvStore::open(root_dir)?;
//         for i in 0..10 {
//             assert_eq!(store.get(i.to_string())?, None);
//         }
//         for i in 10..20 {
//             assert_eq!(store.get(i.to_string())?.unwrap(), i.to_string());
//         }
//         assert_eq!(store.tot_size, FILE_SIZE_LIMIT as u64);

//         store.compact_limit = 1;    // force_compact
//         store.maybe_compact()?;
//         for i in 0..10 {
//             assert_eq!(store.get(i.to_string())?, None);
//         }
//         for i in 10..20 {
//             assert_eq!(store.get(i.to_string())?.unwrap(), i.to_string());
//         }

//         drop(store);

//         store = KvStore::open(root_dir)?;
//         for i in 10..20 {
//             assert_eq!(store.get(i.to_string())?.unwrap(), i.to_string());
//         }

//         Ok(())
//     }

//     #[test]
//     fn basic_compact_test() -> Result<()> {
//         let root = tempfile::tempdir()?;
//         let root_dir = root.path();
//         let mut store = KvStore::new(root_dir)?;
//         assert_eq!(store.tot_size, FILE_SIZE_LIMIT as u64);

//         for i in 0..10000 {
//             let key = format!("key{}", i);
//             let value = format!("value{}", i);
//             store.set(key, value)?;
//         }
//         for i in 0..10000 {
//             assert_eq!(store.get(format!("key{}", i))? , Some(format!("value{}", i)));
//         }
//         for i in 10000..20000 {
//             assert_eq!(store.get(format!("key{}", i))? , None);
//         }

//         drop(store);
//         store = KvStore::open(root_dir)?;
//         for i in 0..10000 {
//             assert_eq!(store.get(format!("key{}", i))? , Some(format!("value{}", i)));
//         }
//         for i in 10000..20000 {
//             assert_eq!(store.get(format!("key{}", i))? , None);
//         }

//         for i in 0..5000 {
//             store.remove(format!("key{}", i))?;
//         }
//         for i in 0..5000 {
//             assert_eq!(store.get(format!("key{}", i))?, None);
//         }
//         for i in 5000..10000 {
//             assert_eq!(store.get(format!("key{}", i))?, Some(format!("value{}", i)));
//         }

//         drop(store);
//         store = KvStore::open(root_dir)?;
//         for i in 0..5000 {
//             assert_eq!(store.get(format!("key{}", i))?, None);
//         }
//         for i in 5000..10000 {
//             assert_eq!(store.get(format!("key{}", i))?, Some(format!("value{}", i)));
//         }

//         // drop all values
//         for i in 5000..10000 {
//             store.remove(format!("key{}", i))?;
//         }
//         for i in 5000..10000 {
//             assert_eq!(store.get(format!("key{}", i))?, None);
//         }
//         store.compact_limit = 1;    // force compact
//         store.maybe_compact()?;
//         assert_eq!(store.tot_size, FILE_SIZE_LIMIT as u64);
//         Ok(())
//     }
// }