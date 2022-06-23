use crate::engines::kv::disk::{FixSizedHeader, Readable};
use crate::{KvError, Result};
use super::disk::{
    FILE_SIZE_LIMIT, MAGIC_KV,
    KVFile, KVSSTable, 
    DefaultHeader, KVRecordKind, KVRecord
};
use super::writebatch::WriteBatch;
use super::memtable::*;
use super::manifest::*;

use lockfree::map::Map as ThreadSafeMap;
use lockfree::map::ReadGuard;

use std::sync::{Arc, Mutex, Condvar};
use std::path::PathBuf;

pub struct KvStoreInner {
    memtable: Arc<Memtable>,
    manifest: Arc<Mutex<Manifest>>,
    sstables: Arc<ThreadSafeMap<FileId, Box<KVSSTable>>>,
    init_compaction_limit: usize,
    bg_flush_cond: Arc<Condvar>, // condvar for memtable to wakeup flush thread.
    state: Arc<Mutex<State>>,
}

impl KvStoreInner {
    const INIT_FLUSH_LIMIT: usize = FILE_SIZE_LIMIT * 2;
    const INIT_COMPACT_LIMIT: usize = 5 * FILE_SIZE_LIMIT * 20;

    pub fn open(root_dir: impl Into<PathBuf>, bg_cond: Arc<Condvar>, state: Arc<Mutex<State>>) -> Result<Self> {
        let (manifest, mut files) = Manifest::open(root_dir)?;
        let memtable = Memtable::new(Self::INIT_FLUSH_LIMIT, bg_cond.clone());
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
                        if record.kind == KVRecordKind::Tomb {
                            memtable.raw_remove(&record.key);
                        } else {
                            memtable.raw_set(&record.key, MemtableValue::Rid((fid, (pos.0, pos.1))))
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

        compaction_limit = std::cmp::max(compaction_limit * 3 / 2, Self::INIT_COMPACT_LIMIT);
        let res = KvStoreInner {
            manifest: Arc::new(Mutex::from(manifest)),
            memtable: Arc::new(memtable),
            sstables: Arc::new(sstables),
            init_compaction_limit: compaction_limit,
            bg_flush_cond: bg_cond,
            state,
        };
        Ok(res)
    }

    pub fn new(root_dir: impl Into<PathBuf>, bg_cond: Arc<Condvar>, state: Arc<Mutex<State>>) -> Result<Self> {
        let mut root_dir: PathBuf = root_dir.into();
        root_dir.push("MANIFEST");
        if std::path::Path::new(&root_dir).exists() {
            root_dir.pop();
            Self::open(root_dir, bg_cond, state)
        } else {
            root_dir.pop();
            let (manifest, files) = Manifest::new(root_dir)?;
            assert!(files.is_empty());
            Ok(Self {
                manifest: Arc::new(Mutex::new(manifest)),
                memtable: Arc::new(Memtable::new(Self::INIT_FLUSH_LIMIT, bg_cond.clone())),
                sstables: Arc::new(ThreadSafeMap::new()),
                init_compaction_limit: Self::INIT_COMPACT_LIMIT,
                bg_flush_cond: bg_cond,
                state
            })
        }
    }
}

impl KvStoreInner {
    pub fn set(&self, key: String, value: String) -> Result<()> {
        let func = || {
            (self.memtable.try_set(&key, (&value).to_owned()), ())
        };
        let cb = |()| {
            Ok(())
        };

        self.memtable_retry_loop(func, cb)
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let func = || {
            self.memtable.try_get(&key)
        };
        let cb = |v| -> Result<(bool, Option<String>)> {
            match v {
                Some(MemtableValue::Rid(rid)) => {
                    // Even though we have get rid, the corresponding file may be removed by compaction. If so, 
                    // we just retry for another time.
                    self.try_get_from_sstable(rid)
                }

                Some(MemtableValue::Value(val)) => {
                    Ok((true, val))
                }

                None => {
                    Ok((true, None))
                }
            }
        };

        loop {
            match self.memtable_retry_loop(func, cb) {
                Ok((success, res)) => {
                    if success {
                        return Ok(res)
                    }
                },
                Err(err) => {
                    return Err(err)
                },
            }
        }
    }

    pub fn remove(&self, key: String) -> Result<()> {
        let func = || {
            (self.memtable.try_remove(&key), ())
        };
        let cb = |()| {
            Ok(())
        };

        self.memtable_retry_loop(func, cb)
    }

    pub fn try_get_from_sstable(&self, rid: Rid) -> Result<(bool, Option<String>)> {
        let (fid, rid) = rid;
        if let Some(sstable) = self.sstables.get(&fid) {
            let record = sstable.val().read_record_at(rid.0, rid.1)?;
            if record.kind == KVRecordKind::Tomb {
                return Ok((true, None))
            } else {
                return Ok((true, Some(record.value.to_owned())))
            }
        }
        Ok((false, None))
    }

    /// A complex method to avoid duplicate code.
    /// F: function which must return a MemtableState
    /// V: value that return from F
    /// C: callback on Value which returned from func.
    /// R: return type for callback
    fn memtable_retry_loop<F, V, C, R>(&self, func: F, cb: C) -> Result<R>
    where
        F: Fn() -> (MemtableAccessState, V),
        C: FnOnce(V) -> Result<R>
    {
        let v: V;
        loop {
            let (state, vv) = func();
            match state {
                MemtableAccessState::Ok => {
                    v = vv;
                    break;
                },

                MemtableAccessState::Retry => {
                    continue;
                },

                MemtableAccessState::FlushReject => {
                    std::thread::sleep(std::time::Duration::from_millis(3));
                    continue;
                },

                MemtableAccessState::CompactionReject => {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    continue;
                },

                MemtableAccessState::CloseReject => {
                    return Err(KvError::Info(String::from("Memtable is closed")).into())
                },

                MemtableAccessState::ErrRemoveNonExist => {
                    unreachable!("unreachable branch")
                },
            }
        }
        cb(v)
    }
}

#[derive(Default, std::fmt::Debug)]
pub struct Statuts {
    pub kind: String,

    pub timecost_get_values: u128,
    pub num_records: usize,

    pub timecost_prepare_and_reset: u128,
    pub bytes_size: usize,

    pub timecost_commit: u128,
}

/// Methods abort flush and compaction.
impl KvStoreInner {
    pub fn bg_flush_compaction_loop(&self) {
        let mut compaction_limit = self.init_compaction_limit;
        let mut disk_usage = self.manifest.lock().unwrap().disk_usage();

        let mut state_guard = self.state.lock().unwrap();
        assert!(*state_guard != State::Exit);
        // handle compaction/flush
        while *state_guard != State::Closed {
            (disk_usage, compaction_limit) = self.maybe_flush_or_compact(disk_usage, compaction_limit);
            state_guard = self.bg_flush_cond.wait(state_guard).unwrap();
        }
        drop(state_guard);
        // now the state is State::CLOSED.
        self.force_flush();
        println!("background thread exit!");
    }

    pub fn force_flush(&self) {
        assert!(*self.state.lock().unwrap() == State::Closed);
        self.memtable.prepare_force_flush();
        self.maybe_flush_or_compact(0, 1);
        {
            let mut state_guard = self.state.lock().unwrap();
            *state_guard = State::Exit;
            self.bg_flush_cond.notify_one();
        }
    }

    fn maybe_flush_or_compact(&self, mut disk_usage: usize, mut compaction_limit: usize) -> (usize, usize) {
        if !self.memtable.should_flush() {
            return (disk_usage, compaction_limit);
        }
        // Guard for flush/compaction.
        let flush_compaction_guard = self.memtable.prepare_flush();
        let mut stat: Statuts = Statuts::default();
        let force_flush = compaction_limit == 1;
        stat.kind = "Flush".to_string();
        // try to flush first...
        // flush all flushable records to dist
        let mut batch = WriteBatch::new(self.manifest.clone()).expect("create WriteBatch error");
        self.flush_and_reset_rids(&mut batch,
            self.memtable.take_all_flushable(),
            &mut stat)
            .expect("write to WriteBatch error");
        let batch_disk_usage = batch.disk_usage();
        disk_usage += batch_disk_usage;
        // commit batch and create new fid->sstable mapping.
        let filemap = batch.commit(&mut stat).expect("batch commit error");
        self.sstables.extend(filemap);
        // println!("disk_usage: {}, batch_disk_usage: {}, compaction_limit: {}", disk_usage, batch_disk_usage, compaction_limit);
        if disk_usage <= compaction_limit || force_flush {
            // Nothing to do because disk_usage not reach compaction threshold.
        } else {
            // trigger compaction...
            // TODO: Multi Flush Buffer + Delay Delete to ensure read/write non-block.
            stat.kind = "Compaction".to_string();
            self.memtable.prepare_compaction();
            let before_fids = self.manifest.lock().unwrap().all_fids();
            batch = WriteBatch::new(self.manifest.clone()).expect("create WriteBatch error");
            self.flush_and_reset_rids(
                &mut batch,
                self.memtable.all(),
                &mut stat)
                .expect("write to WriteBatch error");
            // after compaction, all previous fids is invalid.
            batch.mark_fid_invalid(before_fids);
            stat.bytes_size += batch.disk_usage();
            // commit all changes.
            let filemap = batch.commit(&mut stat).expect("batch commit error");
            self.sstables.extend(filemap);
            // adjust compaction_limit
            let prev_disk_usage = disk_usage;
            disk_usage = self.manifest.lock().unwrap().disk_usage();
            assert!(disk_usage <= prev_disk_usage);
            // only small portion of records is duplicate, so we expand compaction_limit.
            if disk_usage * 12 / 10 >= prev_disk_usage {
                compaction_limit = 2 * prev_disk_usage;
            } else if disk_usage < prev_disk_usage / 2 && compaction_limit > 2 * Self::INIT_COMPACT_LIMIT  {
                // most of records is duplicate, so we shrink compaction_limit.
                compaction_limit = prev_disk_usage * 2 / 3;
            }
        }
        self.memtable.finish_flush(flush_compaction_guard);
        // println!("{:?}", stat);
        (disk_usage, compaction_limit)
    }

    // TODO: return reference to avoid deep copy.
    // TODO: maybe sort by FileId to make this function more cache friendly.
    fn flush_and_reset_rids<'a>(&self, batch: &mut WriteBatch, entries: impl Iterator<Item = ReadGuard<'a, String, MemtableValue>>,
            stat: &mut Statuts) -> Result<()> {
        // sort to make programm more cache-friendly.
        let mut records = entries.collect::<Vec::<_>>();
        records.sort_by(|a, b| {
            match (a.val(), b.val()) {
                (MemtableValue::Value(aa), MemtableValue::Value(bb)) => {
                    match (aa, bb) {
                        (None, None) => {
                            std::cmp::Ordering::Equal
                        }
                        (None, Some(_)) => {
                            std::cmp::Ordering::Less
                        },
                        (Some(_), None) => {
                            std::cmp::Ordering::Greater
                        }
                        (Some(aa), Some(bb)) => {
                            aa.cmp(bb)
                        },
                    }
                }

                (MemtableValue::Rid(_), MemtableValue::Value(_)) => {
                    std::cmp::Ordering::Greater
                },

                (MemtableValue::Value(_), MemtableValue::Rid(_)) => {
                    std::cmp::Ordering::Less
                },

                (MemtableValue::Rid(aa), MemtableValue::Rid(bb)) => {
                    let (a_fid, (a_offset, _)) = aa;
                    let (b_fid, (b_offset, _)) = bb;
                    if a_fid != b_fid {
                        a_fid.cmp(b_fid)
                    } else {
                        a_offset.cmp(b_offset)
                    }
                }
            }
        });

        let mut rid;
        let start = std::time::Instant::now();
        let mut num_records = 0;

        for guard in records {
            num_records += 1;

            match guard.val() {
                MemtableValue::Value(Some(val)) => {
                    let record = KVRecord {
                        key: guard.key().to_owned(),
                        value: val.to_owned(),
                        kind: KVRecordKind::KV
                    };
                    rid = batch.append_bytes(&bincode::serialize(&record)?)?;
                }

                MemtableValue::Rid((fid, pos)) => {
                    rid = batch.append_bytes(
                        self.sstables
                            .get(fid).unwrap()
                            .val()
                            .raw_read(pos.0, pos.1)?
                    )?;
                }

                MemtableValue::Value(None) => {
                    let record = KVRecord {
                        key: guard.key().to_owned(),
                        value: "".to_owned(),
                        kind: KVRecordKind::Tomb
                    };
                    rid = batch.append_bytes(&bincode::serialize(&record)?)?;
                }
            }

            self.memtable.raw_set(guard.key(), MemtableValue::Rid(rid));
        }
        stat.num_records += num_records;
        stat.timecost_prepare_and_reset += start.elapsed().as_millis();
        Ok(())
    }
}

impl Drop for KvStoreInner {
    fn drop(&mut self) {
        println!("KvStoreInner close");
    }
}

#[derive(PartialEq)]
pub enum State {
    Running,
    Closed,
    Exit,
}
