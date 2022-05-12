use std::collections::{HashMap};
use std::fs::remove_file;
use std::path::{PathBuf, Path};
use serde::{Serialize, Deserialize};

use crate::dbfile::DBFile;
use crate::errors::KvError;
use crate::kvfile::{KVRecord, KVRecordKind};
use crate::Result;

type KVFile = DBFile<KVRecord>;
type ManifestFile = DBFile<ManifestRecord>;
pub type file_id = u32;

#[derive(Debug, Serialize, Deserialize)]
struct ManifestRecord {
    added: Vec<file_id>,
    removed: Vec<file_id>,
}

pub struct Manifest {
    next_fid: file_id,
    manifest_file: ManifestFile,
    kv_files: HashMap<file_id, KVFile>,
}

impl Manifest {
    pub fn open(db: impl Into<PathBuf>) -> Result<Self> {
        let mut path: PathBuf = db.into();
        path.push("MANIFEST");

        let mut manifest = Manifest {
            next_fid: 0,
            manifest_file: DBFile::<ManifestRecord>::open(path, true)?,
            kv_files: HashMap::new(),
        };

        let mut max_fid = 0;
        let mut valid_fids = std::collections::HashSet::new();

        for record in manifest.manifest_file.all_records()? {
            for fid in record.added {
                valid_fids.insert(fid);
                max_fid = std::cmp::max(max_fid, fid);
            }
            for fid in record.removed {
                valid_fids.remove(&fid);
                manifest.os_remove_file(fid)?;
                max_fid = std::cmp::max(max_fid, fid);
            }
        }

        for fid in valid_fids {
            let kv_file = KVFile::open(Manifest::fid_to_file_name(fid), false)?;
            manifest.kv_files.insert(fid, kv_file);
        }

        Ok(manifest)
    }

    pub fn create_kv_file(&mut self) -> Result<file_id> {
        match self.os_create_kv_file() {
            Ok((fid, file)) => {
                self.kv_files.insert(fid, file);
                Ok(fid)
            }

            Err(err) => {
                Err(err)
            }
        }
    }

    pub fn use_file(&mut self, fid: file_id) -> Result<()> {
        self._use_kv_file(fid)
    }

    pub fn atomic_add_remove(&mut self, added: &Vec<file_id>, removed: &Vec<file_id>) -> Result<()> {
        self._atomic_add_remove(added, removed)
    }

    fn _use_kv_file(&mut self, fid: file_id) -> Result<()> {
        assert!(self.kv_files.contains_key(&fid));
        self.atomic_add_remove(&vec![fid], &vec![])
    }

    // TODO: error handling
    // Does BufReader and BufWriter still usable after enc/dec failed?
    // How to handle Database corruption?
    fn _atomic_add_remove(&mut self, added: &Vec<file_id>, removed: &Vec<file_id>) -> Result<()> {
        self.manifest_file.append_record(ManifestRecord {
            added: added.to_owned(), 
            removed: removed.to_owned()}
        )?;

        for fid in removed {
            self.os_remove_file(*fid)?
        }
        Ok(())
    }

    fn fid_to_file_name(fid: file_id) -> String {
        format!("DBFile00{}.dbf", fid)
    }

    fn os_remove_file(&self, fid: file_id) -> Result<()> {
        match remove_file(Manifest::fid_to_file_name(fid)) {
            Ok(()) => { Ok(()) }
            Err(err) => { 
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(())
                }
                Err(KvError::IOError(err))
            }
        }
    }

    fn os_create_kv_file(&mut self) -> Result<(file_id, KVFile)> {
        let fid = self.next_fid;
        match DBFile::<KVRecord>::open(Manifest::fid_to_file_name(fid), true) {
            Ok(file) => {
                self.next_fid += 1;
                Ok((fid, file))
            }

            Err(err) => {
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod manifest_unit_tests {
    // use std::{path::Path, fs::{remove_file, remove_dir_all}};
    // use crate::kvfile::{KVRecord, KVRecordKind};
    // use crate::manifest::Manifest;

    // #[test]
    // fn create_open_test() {
    //     let db_path =  Path::new("testDB");
    //     remove_dir_all(db_path).unwrap_or(());
    //     let mut manifest = Manifest::open(db_path).unwrap();

    //     let f1 = manifest.create_kv_file().unwrap();
    //     let f2 = manifest.create_kv_file().unwrap();
    //     let f3 = manifest.create_kv_file().unwrap();
    //     assert!(f1 == 1 && f2 == 2 && f3 == 3);

    //     for i in 0..10 {
    //         manifest.kv_files.get_mut(&f1).unwrap().append_record(KVRecord{ 
    //             kind: KVRecordKind::KV,
    //             key: format!(),
    //             value: String::from("213")
    //         }).unwrap();
    //     }


    // }
}