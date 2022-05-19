// use std::collections::{HashSet};
// use std::fs::{create_dir_all};
// use std::path::{PathBuf};

// use manifest_file::{ManifestFile, ManifestRecord};
// use crate::Result;
// use crate::dbfile::{Storage, MmapFile};
// use crate::errors::KvError;

// pub struct Manifest {
//     next_fid: file_id,
//     manifest_file: ManifestFile,
//     files: HashSet<file_id>,
//     root: PathBuf,
// }

// impl Manifest {
//     /// TODO: process should set current_dir to database dir
//     pub fn open(root_dir: impl Into<PathBuf>) -> Result<(Self, Vec<(file_id, Box<dyn Storage>)>)> {
//         let mut pb: PathBuf = root_dir.into();
//         pb.push("MANIFEST");
//         let manifest_file = ManifestFile::open(
//             Box::new( MmapFile::open(&pb)? )
//             , false
//         )?;
//         pb.pop();
//         let mut manifest = Manifest {
//             next_fid: 1,
//             manifest_file,
//             files: HashSet::new(),
//             root: pb,
//         };

//         let files = manifest.init()?;
//         Ok((manifest, files))
//     }

//     pub fn new(root_dir: impl Into<PathBuf>) -> Result<(Self, Vec<(file_id, Box<dyn Storage>)>)> {
//         let mut pb: PathBuf = root_dir.into();
//         create_dir_all(&pb)?;
//         pb.push("MANIFEST");
//         let manifest_file = ManifestFile::open(
//             Box::new(MmapFile::new(&pb)?), 
//             true
//         )?;
//         pb.pop();
//         let mut manifest = Manifest {
//             next_fid: 1,
//             manifest_file,
//             files: HashSet::new(),
//             root: pb,
//         };

//         let files = manifest.init()?;

//         Ok((manifest, files))
//     }

//     pub fn create_file(&mut self) -> Result<(file_id, Box<dyn Storage>)> {
//         let name = self.filename(self.next_fid);
//         let fid = self.next_fid;

//         let storage = Box::new(
//             MmapFile::new(PathBuf::from(name))?
//         );
//         self.files.insert(fid);
//         self.next_fid += 1;        
//         Ok((fid, storage))
//     }

//     pub fn remove_file(&mut self, fid: file_id) -> Result<()> {
//         self.atomic_add_remove(vec![], vec![fid])
//     }

//     pub fn validate_file(&mut self, fid: file_id) -> Result<()> {
//         self.atomic_add_remove(vec![fid], vec![])
//     }

//     pub fn atomic_add_remove(&mut self, added: Vec<file_id>, removed: Vec<file_id>) -> Result<()> {
//         for fid in added.iter() {
//             if !self.files.contains(&fid) {
//                 return Err(KvError::MaybeCorrput.into())
//             }
//         }
//         for fid in removed.iter() {
//             if !self.files.contains(&fid) {
//                 return Err(KvError::MaybeCorrput.into());
//             }
//         }

//         self.manifest_file.append_record(
//             & ManifestRecord {
//                 added,
//                 removed: removed.clone()
//             } 
//         )?;

//         for fid in removed {
//             self.files.remove(&fid);
//             self.remove_file_by_fid(fid).unwrap_or_default();
//         }

//         self.manifest_file.sync()
//     }

//     fn init(&mut self) -> Result<Vec<(file_id, Box<dyn Storage>)>> {
//         let (mut valid_fids, mut invalid_fids) = (HashSet::<file_id>::new(), HashSet::<file_id>::new());

//         let mut max_fid = 0 as file_id;

//         for (_, record) in self.manifest_file.all_records()? {
//             for fid in record.added {
//                 max_fid = std::cmp::max(max_fid, fid);
//                 valid_fids.insert(fid);
//             }
//             for fid in record.removed {
//                 max_fid = std::cmp::max(max_fid, fid);
//                 invalid_fids.insert(fid);
//             }
//         }        
//         for fid in invalid_fids.iter() {
//             self.remove_file_by_fid(*fid)?;
//             valid_fids.remove(&fid);
//         }

//         let mut files: Vec<(file_id, Box<dyn Storage>)> = Vec::new();
//         for fid in valid_fids.iter() {
//             files.push((*fid,
//                 Box::new(MmapFile::open(self.filename(*fid))?
//             )))
//         }

//         self.next_fid = max_fid + 1;
//         self.files = valid_fids;
//         Ok(files)
//     }

//     pub fn filename(&self, fid: file_id) -> String {
//         format!("{}/DBFile00{}.dbf", self.root.to_str().unwrap(), fid)
//     }

//     pub fn remove_file_by_fid(&mut self, fid: file_id) -> Result<()> {
//         if let Err(err) = std::fs::remove_file(PathBuf::from(self.filename(fid))) {
//             if err.kind() != std::io::ErrorKind::NotFound {
//                 return Err(KvError::from(err).into())
//             }
//         }
//         Ok(())
//     }

// }

// mod manifest_file {
//     use serde::{Serialize, Deserialize};

//     use crate::dbfile::{OrdinaryRecord, DataBaseFile, DefaultHeader};
//     use crate::manifest::file_id;

//     #[derive(Serialize, Deserialize, Debug)]
//     pub struct ManifestRecord {
//         pub added: Vec<file_id>,
//         pub removed: Vec<file_id>,
//     }

//     impl OrdinaryRecord for ManifestRecord {  }

//     pub type ManifestFile = DataBaseFile<DefaultHeader<MAGIC_MANIFEST>, ManifestRecord>;
//     pub const MAGIC_MANIFEST: u8 = 0;
// }


// pub type file_id = u32;
// pub const INVALID_FILE_ID: file_id = 0;

// // TODO: chdir or other solutions
// #[cfg(test)]
// mod manifest_unit_tests {
//     use crate::{Result, kvfile::{KVSSTable, KVRecord}, manifest::file_id};

//     use super::Manifest;

//     #[test]
//     fn simple_test() -> Result<()> {
//         let dir = tempfile::tempdir()?;
//         let (mut manifest, mut files) = Manifest::new(dir.path())?;
//         assert_eq!(files.len(), 0);
//         assert_eq!(manifest.next_fid, 1);

//         let (fid_1, file_1) = manifest.create_file()?;
//         let (fid_2, file_2) = manifest.create_file()?;
//         let (fid_3, file_3) = manifest.create_file()?;
//         assert_eq!(fid_1, 1);
//         assert_eq!(fid_2, 2);
//         assert_eq!(fid_3, 3);
//         assert_eq!(manifest.next_fid, 4);

//         let mut kv_file_1 = KVSSTable::open(file_1, true)?;
//         let mut kv_file_2 = KVSSTable::open(file_2, true)?;
//         let mut kv_file_3 = KVSSTable::open(file_3, true)?;

//         for i in 0..80 {
//             kv_file_1.append_record(&KVRecord{
//                 kind: crate::kvfile::KVRecordKind::KV,
//                 key: format!("KEY_{}_FROM_{}", i, 1),
//                 value: format!("VALUE_{}_FROM_{}", i, 1),
//             })?;

//             kv_file_2.append_record(&KVRecord {
//                 kind: crate::kvfile::KVRecordKind::KV,
//                 key: format!("KEY_{}_FROM_{}", i, 2),
//                 value: format!("VALUE_{}_FROM_{}", i, 2),
//             })?;
//         }

//         let mut records = kv_file_1.all_records()?;
//         assert_eq!(records.len(), 80);
//         for (i, (_, record)) in records.iter().enumerate() {
//             assert_eq!(record.key, format!("KEY_{}_FROM_{}", i, 1));
//             assert_eq!(record.value, format!("VALUE_{}_FROM_{}", i, 1));
//         }
//         records = kv_file_2.all_records()?;
//         assert_eq!(records.len(), 80);
//         for (i, (_, record)) in records.iter().enumerate() {
//             assert_eq!(record.key, format!("KEY_{}_FROM_{}", i, 2));
//             assert_eq!(record.value, format!("VALUE_{}_FROM_{}", i, 2));
//         }
//         records = kv_file_3.all_records()?;
//         assert_eq!(records.len(), 0);

//         kv_file_1.close()?;
//         kv_file_2.close()?;
//         kv_file_3.close()?;

//         manifest.validate_file(fid_1)?;
//         manifest.validate_file(fid_3)?;

//         drop(manifest);

//         (manifest, files) = Manifest::open(dir.path())?;
//         assert_eq!(files.len(), 2);
//         assert_eq!(manifest.next_fid, 4);
//         let mut fids: Vec<file_id> = manifest.files.clone().into_iter().collect();
//         fids.sort();
//         assert_eq!(fids, vec![1, 3]);
//         fids.clear();
//         for (fid, _) in files.iter() {
//             fids.push(*fid);
//         }
//         fids.sort();
//         assert_eq!(fids, vec![1, 3]);

//         for (fid, file) in files.drain(..) {
//             if fid == 1 {
//                 kv_file_1 = KVSSTable::open(file, false)?;
//             } else {
//                 kv_file_3 = KVSSTable::open(file, false)?;
//             }
//         }

//         let mut records = kv_file_3.all_records()?;
//         assert_eq!(records.len(), 0);
//         records = kv_file_1.all_records()?;
//         assert_eq!(records.len(), 80);
//         for (i, (_, record)) in records.iter().enumerate() {
//             assert_eq!(record.key, format!("KEY_{}_FROM_{}", i, 1));
//             assert_eq!(record.value, format!("VALUE_{}_FROM_{}", i, 1));
//         }

//         let (fid_4, file_4) = manifest.create_file()?;
//         assert_eq!(fid_4, 4);
//         assert_eq!(manifest.next_fid, 5);
//         let mut kv_file_4 = KVSSTable::open(file_4, true)?;
        
//         for i in 0..80 {
//             kv_file_4.append_record(&KVRecord{
//                 kind: crate::kvfile::KVRecordKind::KV,
//                 key: format!("KEY_{}_FROM_{}", i, 4),
//                 value: format!("VALUE_{}_FROM_{}", i, 4),
//             })?;
//         }
//         kv_file_4.close()?;
//         manifest.atomic_add_remove(vec![fid_4], vec![fid_1, fid_3])?;
//         drop(manifest);

//         (_, files) = Manifest::open(dir.path())?;
//         assert_eq!(files.len(), 1);
//         let (fid, f) = files.remove(0);
//         assert_eq!(fid, 4);
//         kv_file_4 = KVSSTable::open(f, false)?;
//         records = kv_file_4.all_records()?;
//         assert_eq!(records.len(), 80);
//         for (i, (_, record)) in records.iter().enumerate() {
//             assert_eq!(record.key, format!("KEY_{}_FROM_{}", i, 4));
//             assert_eq!(record.value, format!("VALUE_{}_FROM_{}", i, 4));
//         }

//         Ok(())
//     }
// }