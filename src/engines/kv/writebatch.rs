use std::{sync::{Mutex, Arc}, collections::{HashSet, HashMap}};

use crate::{Result, KvError};

use super::{manifest::{FileId, Manifest, RID}, kvfile::{KVFile, KVSSTable, KVRecord}, dbfile::{Appendable, Pos, FILE_SIZE_LIMIT, Readable, DefaultHeader}, kvstore::Statuts};

pub struct WriteBatch {
    manifest: Arc<Mutex<Manifest>>,
    current_fid: FileId,
    writers: HashMap<FileId, Box<KVFile>>,
    invalid_fids: HashSet<FileId>,
    disk_usage: usize,
}

impl WriteBatch {
    pub fn new(manifest: Arc<Mutex<Manifest>>) -> Result<Self> {
        let (current_fid, file) = manifest.lock().unwrap().create_file()?;
        let mut valid_fids = HashSet::new();
        valid_fids.insert(current_fid);
        let writer = Box::new(KVFile::init(file)?);
        let mut writers = HashMap::new();
        writers.insert(current_fid, writer);

        Ok(Self {
            manifest,
            current_fid,
            writers,
            invalid_fids: HashSet::new(),
            disk_usage: FILE_SIZE_LIMIT,
        })
    }

    pub fn append_bytes(&mut self, data: &[u8]) -> Result<RID> {   
        match self.writers.get_mut(&self.current_fid).unwrap().raw_append(data) {
            Ok(pos) => {
                return Ok((self.current_fid, pos))
            }

            Err(err) => {
                if let Some(KvError::FileSizeExceed) = err.downcast_ref::<KvError>() {
                    let (current_fid, file) = self.manifest.lock().unwrap().create_file()?;
                    let current_writer = Box::new(KVFile::init(file)?);
                    self.writers.insert(current_fid, current_writer);
                    self.current_fid = current_fid;
                    self.disk_usage += FILE_SIZE_LIMIT;
                    return Ok((self.current_fid, self.writers.get_mut(&current_fid).unwrap().raw_append(data)?))
                } else {
                    return Err(err)
                }
            }
        }
    }

    pub fn disk_usage(&self) -> usize {
        return self.disk_usage;
    }

    pub fn mark_fid_invalid(&mut self, fids: Vec<FileId>) {
        self.invalid_fids.extend(fids.iter())
    }

    pub fn commit(self, stat: &mut Statuts) -> Result<HashMap<FileId, Box<KVSSTable>>> {
        let start = std::time::Instant::now();
        let invalid_fids = self.invalid_fids.into_iter().collect::<Vec<_>>();
        let valid_fids = self.writers.iter().map(|x| {*x.0}).collect::<Vec<_>>();
        let mut filemap: HashMap<FileId, Box<KVSSTable>> = HashMap::new();

        for (fid, mut kvf) in self.writers {
            // flush all writes to disk.
            kvf.sync()?;
            filemap.insert(fid, Box::new(*kvf));
        }

        self.manifest.lock().unwrap().atomic_add_remove(
            valid_fids,
            invalid_fids)?;

        stat.timecost_commit += start.elapsed().as_millis();
        Ok(filemap)
    }

    // TODO: It's useless to remove file to avoid data flush because data flush is guarented
    // to happen when `Mmap` is droped.
    // TODO: Maybe use anaonymous Mmap instead?
    pub fn abort(self) -> Result<()> {
        let mut manifest_guard = self.manifest.lock().unwrap();
        let remove_fids = self.writers.keys();
        for fid in remove_fids {
            manifest_guard.remove_file_by_fid(*fid)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod writebatch_unit_test {
    #[test]
    fn basic_test() -> Result<()> {
        let mut cost = Statuts::default();
        let db_path = tempfile::tempdir()?;
        let root_dir = db_path.path();
        let (mut manifest, _) = Manifest::new(root_dir)?;
        let mut p_manifest = Arc::new(Mutex::new(manifest));
        let mut batch = WriteBatch::new(Arc::clone(&p_manifest))?;
        // use WriteBatch write many many records.
        let mut rids = Vec::new();
        for i in 0..50000 {
            let record = KVRecord {
                key: format!("key-{}", i),
                value: format!("value-{}", i),
                kind: KVRecordKind::KV,
            };

            let rid = batch.append_bytes(&bincode::serialize(&record)?)?;
            rids.push(rid);
        }
        // create a new kvfile and write some value.
        let (usable_fid, file) = p_manifest.lock().unwrap().create_file()?;
        let mut kvfile = KVFile::init(file)?;
        let mut rids2 = Vec::new();
        for i in 0..10 {
            let record = KVRecord {
                key: format!("key-{}", i),
                value: format!("value-{}", i),
                kind: KVRecordKind::KV,
            };
            
            rids2.push(kvfile.append_record(&record)?);
        }
        p_manifest.lock().unwrap().validate_file(usable_fid)?;
        // Abort the WriteBatch. Any records in WriteBatch is droped.
        // Close the manifest.
        batch.abort()?;
        drop(p_manifest);
        // Reopen Manifest. The only valid file's FileId is usable_fid.
        let mut files;
        (manifest, files) = Manifest::open(root_dir)?;
        assert_eq!(files.len(), 1);
        assert_eq!(files.get(0).unwrap().0, usable_fid);
        // Check if any fid create by aborted WriteBatch is removed.
        for fid in 0..usable_fid {
            assert_eq!(
                MmapFile::open(
                    PathBuf::from(format!("{}/DBFile00{}.dbf",root_dir.to_str().unwrap(), fid))
                ).is_err(),
                true
            );
        }
        // Records writen by usable_fid should not be affected.
        kvfile = KVFile::open(files.remove(0).1)?;
        let records = kvfile.all_records()?;
        assert_eq!(records.len(), 10);
        for (i, record) in records.iter().enumerate() {
            let (_, record) = record;
            assert!(
                record.key == format!("key-{}", i) &&
                record.value == format!("value-{}", i) &&
                record.kind == KVRecordKind::KV
            );
        }
        // Create another WriteBatch. Write many records
        p_manifest = Arc::new(Mutex::new(manifest));
        batch = WriteBatch::new(p_manifest.clone())?;
        rids.clear();
        for i in 0..50000 {            
            let record = KVRecord {
                key: format!("key-{}", i),
                value: format!("value-{}", i),
                kind: KVRecordKind::KV,
            };

            let rid = batch.append_bytes(&bincode::serialize(&record)?)?;
            rids.push(rid);
        }
        // Make usable_fid invalid.
        // After commitment, all data from WriteBatch should be persist, and usable_fid should not exist.
        batch.mark_fid_invalid(vec![usable_fid]);
        batch.commit(&mut cost)?;
        drop(p_manifest);
        // Reopen Manifest. Check records writen by WriteBatch.
        let (_, files) = Manifest::open(root_dir)?;
        let files_map = files.into_iter().collect::<HashMap<FileId, Box<dyn Storage>>>();
        for (i, rid) in rids.iter().enumerate() {
            let record: KVRecord = bincode::deserialize(
                files_map.get(&rid.0).unwrap().read(rid.1.0, rid.1.1)?
            )?;
            assert!(
                record.key == format!("key-{}", i) &&
                record.value == format!("value-{}", i) &&
                record.kind == KVRecordKind::KV
            );
        }
        // Records writen by usable_fid is also removed.
        assert_ne!(files_map.contains_key(&usable_fid), true);
        // Congratulations!
        Ok(())
    }

    use std::{sync::{Arc, Mutex}, path::PathBuf, collections::HashMap};

    use tempfile;

    use crate::{Result, engines::kv::{manifest::{Manifest, FileId}, kvfile::{KVRecordKind, KVRecord, KVFile}, dbfile::{Appendable, Readable, MmapFile, Storage}, kvstore::Statuts}};
    use super::WriteBatch;
}