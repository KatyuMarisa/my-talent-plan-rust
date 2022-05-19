use std::sync::{Arc, Mutex, Condvar};
use std::path::PathBuf;
use std::cmp;

use lockfree::map::Map as ThreadSafeMap;

use crate::engines::kv::dbfile::{FixSizedHeader};
use crate::engines::kv::kvfile::{MAGIC_KV, KVFile, KVRecordKind};
use crate::engines::kv::memtable::Value;
use crate::{KvError, Result, KvsEngine};

use super::dbfile::{DefaultHeader, Appendable};
use super::kvfile::{KVWritable, KVRecord};
use super::manifest::RID;
use super::memtable::MemtableState;
use super::writebatch::WriteBatch;
use super::{manifest::{Manifest, FileId}, memtable::Memtable, dbfile::{Readable, FILE_SIZE_LIMIT}, kvfile::KVSSTable};

pub struct KvStore {
    manifest: Arc<Mutex<Manifest>>,
    memtable: Arc<Memtable>,
    sstables: Arc<ThreadSafeMap<FileId, Box<KVSSTable>>>,
    bg_cond: Arc<Condvar>,
    init_compaction_limit: usize,
}

impl KvStore {
    pub fn open(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let (manifest, mut files) = Manifest::open(root_dir)?;
        let cond = Arc::new(Condvar::new());
        let memtable = Memtable::new(std::usize::MAX, cond.clone());
        let sstables = ThreadSafeMap::new();
        let mut compaction_limit: usize = 0;

        files.sort_by(|a, b| {
            a.0.cmp(&b.0)
        });
        for (fid, file) in files {
            const MAGIC_INVALID: u8 = 255;
            let h: DefaultHeader<MAGIC_INVALID> = bincode::deserialize(
                file.read(0, DefaultHeader::<MAGIC_INVALID>::header_length())?
            )?;

            match h.magic() {
                MAGIC_KV => {
                    let reader: Box<KVSSTable> = Box::new(
                        KVFile::open(file)?
                    );

                    for (pos, record) in reader.all_records()? {
                        if record.kind == KVRecordKind::TOMB {
                            memtable.unpin_remove(&record.key);
                        } else {
                            memtable.unpin_set(&record.key, Value::RID((fid, (pos.0, pos.1))))
                        }
                    }
                    sstables.insert(fid, reader);
                    compaction_limit += FILE_SIZE_LIMIT;
                }
 
                _ => {
                    return Err(KvError::MaybeCorrput.into())
                }
            }
        }

        compaction_limit = compaction_limit * 3 / 2;

        Ok(KvStore{
            manifest: Arc::new(Mutex::from(manifest)),
            memtable: Arc::new(memtable),
            sstables: Arc::new(sstables),
            bg_cond: cond,
            init_compaction_limit: compaction_limit,
        })
    }

    fn bg_flush_compaction(&self) {
        let mtx = Mutex::new(5);
        let mut guard = mtx.lock().unwrap();
        let mut compaction_limit = self.init_compaction_limit;
        let mut disk_usage = self.manifest.lock().unwrap().disk_usage();

        loop {
            guard = self.bg_cond.wait(guard).unwrap();

            if !self.memtable.should_flush() {
                continue;
            }

            if !self.memtable.pin_flush() {
                continue;
            }
            
            let mut batch = WriteBatch::new(self.manifest.clone()).expect("create writebatch failed");
            self.write_to_batch(&mut batch, self.memtable.remove_flushable()).expect("batch write error");
            // TODO: adjust compaction_limit
            if disk_usage + batch.disk_usage() <= compaction_limit {
                batch.commit().expect("batch commit error");
            } else {
                batch.abort().expect("batch abort error");
                batch = WriteBatch::new(self.manifest.clone()).expect("create writebatch failed");
            }
        }
    }

    // TODO: return reference to avoid deep copy.
    // TODO: maybe sort by FileId to make this function more cache friendly.
    fn write_to_batch(&self, batch: &mut WriteBatch, values: Vec<(String, Value)>) -> Result<Vec<(String, RID)>> {
        let mut key_rids = Vec::new();
        let mut rid;

        for (key, val) in values {
            match val {
                Value::ValueStr(valstr) => {
                    let record = KVRecord {
                        key: key.to_owned(),
                        value: valstr.to_owned(),
                        kind: KVRecordKind::KV
                    };
                    rid = batch.append_bytes(&bincode::serialize(&record)?)?;
                }

                Value::RID((fid, pos)) => {
                    rid = batch.append_bytes(
                        self.sstables.get(&fid).unwrap().val().raw_read(pos.0, pos.1)?
                    )?;
                }

                Value::TOMB => {
                    let record = KVRecord {
                        key: key.to_owned(),
                        value: "".to_owned(),
                        kind: KVRecordKind::TOMB
                    };
                    rid = batch.append_bytes(&bincode::serialize(&record)?)?;
                }
            }
            key_rids.push((key.to_owned(), rid));            
        }

        return Ok(key_rids);
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        // if let Some(either) = self.memtable.get(key)
        // let Some((state, )) = self.memtable.get(key)
        todo!()
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let mut retry = 3;
        loop {
            if 0 == retry {
                return Err(KvError::DataRace.into())
            }
            // TODO: maybe schedule background task.
            match self.memtable.get(key.to_owned()) {
                Ok((state, value)) => {
                    if MemtableState::Ok != state {
                        retry -= 1;
                        continue;
                    }

                    match value {
                        Some(Value::RID(rid)) => {
                            return Ok(Some(self.sstables.get(&rid.0)
                                .unwrap()
                                .val()
                                .read_record_at(rid.1.0, rid.1.1)?.value));
                        }

                        Some(Value::ValueStr(vstr)) => {
                            return Ok(Some(vstr))
                        }

                        None => {
                            return Ok(None)
                        }

                        Some(Value::TOMB) => {
                            return Ok(None)
                        }
                    }
                }

                Err(err) => {
                    return Err(err)
                }
            }
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        todo!()
    }
}