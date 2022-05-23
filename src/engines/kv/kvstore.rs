use std::sync::{Arc, Mutex, Condvar};
use std::path::PathBuf;

use lockfree::map::{Map as ThreadSafeMap, ReadGuard};

use crate::engines::kv::dbfile::{FixSizedHeader};
use crate::engines::kv::kvfile::{MAGIC_KV, KVFile, KVRecordKind};
use crate::engines::kv::memtable::MemtableValue;
use crate::{KvError, Result, KvsEngine};

use super::dbfile::DefaultHeader;
use super::kvfile::KVRecord;
use super::memtable::MemtableState;
use super::writebatch::WriteBatch;
use super::{manifest::{Manifest, FileId}, memtable::Memtable, dbfile::{Readable, FILE_SIZE_LIMIT}, kvfile::KVSSTable};

struct KvStoreInner {
    manifest: Arc<Mutex<Manifest>>,
    memtable: Arc<Memtable>,
    sstables: Arc<ThreadSafeMap<FileId, Box<KVSSTable>>>,
    init_compaction_limit: usize,
    bg_flush_cond: Arc<Condvar>,
    bg_pending_cond: Arc<Condvar>,
    bg_pending_mutex: Arc<Mutex<u8>>,
    running: Arc<Mutex<bool>>,
}

#[derive(Clone)]
pub struct KvStore {
    inner: Arc<KvStoreInner>,
    bg_cond: Arc<Condvar>,
    running: Arc<Mutex<bool>>,
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

impl KvStore {
    pub fn open(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let bg_cond = Arc::new(Condvar::new());
        let running = Arc::new(Mutex::new(true));
        let inner = Arc::new(KvStoreInner::open(root_dir, bg_cond.clone(), running.clone())?);
        let inner2 = inner.clone();
        std::thread::spawn(move || {
            inner2.bg_flush_compaction();
        });

        Ok(Self {
            inner,
            bg_cond,
            running
        })
    }

    pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let bg_cond = Arc::new(Condvar::new());
        let running = Arc::new(Mutex::new(true));
        let inner = Arc::new(KvStoreInner::new(root_dir, bg_cond.clone(), running.clone())?);
        let inner2 = inner.clone();
        std::thread::spawn(move || {
            inner2.bg_flush_compaction();
        });

        Ok(Self {
            inner,
            bg_cond,
            running
        })

    }

    pub fn stop(self) {
        // stop memtable.
        self.inner.memtable.stop_and_prepare_flush_memtable();
        // kill bg_flush_compaction task.
        {
            let mut running_guard = self.running.lock().unwrap();
            *running_guard = false;
            self.bg_cond.notify_one();
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        // it must be a flush
        self.inner.maybe_flush_or_compact(0, 1);
    }
}

impl KvStoreInner {
    const INIT_FLUSH_LIMIT: usize = FILE_SIZE_LIMIT * 2;
    const INIT_COMPACT_LIMIT: usize = FILE_SIZE_LIMIT * 4;

    pub fn open(root_dir: impl Into<PathBuf>, bg_cond: Arc<Condvar>, running: Arc<Mutex<bool>>) -> Result<Self> {
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
                        if record.kind == KVRecordKind::TOMB {
                            memtable.raw_remove(&record.key);
                        } else {
                            memtable.raw_set(&record.key, MemtableValue::RID((fid, (pos.0, pos.1))))
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
        Ok(KvStoreInner {
            manifest: Arc::new(Mutex::from(manifest)),
            memtable: Arc::new(memtable),
            sstables: Arc::new(sstables),
            init_compaction_limit: compaction_limit,
            bg_pending_mutex: Arc::new(Mutex::new(5)),
            bg_pending_cond: Arc::new(Condvar::new()),
            bg_flush_cond: bg_cond,
            running,
        })
    }

    fn new(root_dir: impl Into<PathBuf>, bg_cond: Arc<Condvar>, running: Arc<Mutex<bool>>) -> Result<Self> {
        let mut root_dir: PathBuf = root_dir.into();
        root_dir.push("MANIFEST");
        if std::path::Path::new(&root_dir).exists() {
            root_dir.pop();
            return Self::open(root_dir, bg_cond, running)
        } else {
            root_dir.pop();
            let (manifest, files) = Manifest::new(root_dir)?;
            assert!(files.len() == 0);
            return Ok(Self {
                manifest: Arc::new(Mutex::new(manifest)),
                memtable: Arc::new(Memtable::new(Self::INIT_FLUSH_LIMIT, bg_cond.clone())),
                sstables: Arc::new(ThreadSafeMap::new()),
                init_compaction_limit: Self::INIT_COMPACT_LIMIT,
                bg_pending_mutex: Arc::new(Mutex::new(5)),
                bg_pending_cond: Arc::new(Condvar::new()),
                bg_flush_cond: bg_cond,
                running
            })
        }
    }

    /// A complex method to avoid duplicate code. This function will retry
    /// F: function which must return a MemtableState
    /// V: value that return from F
    /// C: callback on Value return from F.
    /// R: return type for callback
    fn retry_loop<F, V, C, R>(&self, func: F, cb: C) -> Result<R>
    where
        F: Fn() -> (MemtableState, V),
        C: Fn(V) -> Result<R>,
    {
        let mut max_retry = 10;
        let mut data_race_retry = 0;
        let mut compaction_race_retry = 0;
        let mut flush_race_retry = 0;
        let v: V;
        loop {
            if max_retry == 0 {
                println!("failed to access memtable");
                println!("data_race_retry: {}", data_race_retry);
                println!("compaction_race_retry: {}", compaction_race_retry);
                println!("flush_race_retry: {}", flush_race_retry);
                return Err(KvError::DataRace.into())
            }

            match func() {
                (MemtableState::Ok, vv) => {
                    v = vv;
                    break;
                }

                (MemtableState::AccessRace, _) => {
                    data_race_retry += 1;
                    // data race is too heavy, but no flush or compaction is happending, so juse
                    // retry for another time.
                }

                (MemtableState::FlushRace, _) => {
                    // std::thread::sleep(std::time::Duration::from_millis(100));
                    _ = self.bg_pending_cond.wait(self.bg_pending_mutex.lock().unwrap());
                    flush_race_retry += 1;
                }

                (MemtableState::CompactionRace, _) => {
                    // std::thread::sleep(std::time::Duration::from_millis(200));
                    _ = self.bg_pending_cond.wait(self.bg_pending_mutex.lock().unwrap());
                    compaction_race_retry += 1;
                }

                (MemtableState::KeyNotExist(key), _) => {
                    return Err(KvError::KeyNotFoundError { key }.into())
                }
            }

            max_retry -= 1;
        }
        cb(v)
    }

    fn bg_flush_compaction(&self) {
        let mut compaction_limit = self.init_compaction_limit;
        let mut disk_usage = self.manifest.lock().unwrap().disk_usage();
        loop {
            // A wakeup may indicate flush/compaction, or stop.
            {
                let guard = self.running.lock().unwrap();
                // stop
                if !(*guard) {
                    break;
                }
                _ = self.bg_flush_cond.wait(guard).unwrap();
            }

            if !self.memtable.should_flush() {
                continue;
            }

            if !self.memtable.pin_flush() {
                continue;
            }

            (disk_usage, compaction_limit) = self.maybe_flush_or_compact(disk_usage, compaction_limit);

            // because most of read/write operations is lockfree, so we can wakeup all pengding requests without
            // worrying about intense access.
            self.bg_pending_cond.notify_all();
        }
    }

    fn maybe_flush_or_compact(&self, mut disk_usage: usize, mut compaction_limit: usize) -> (usize, usize) {
        let mut stat: Statuts = Statuts::default();
        let force_flush = compaction_limit == 1;
        stat.kind = "Flush".to_string();
        // try to flush first...
        let mut batch = WriteBatch::new(self.manifest.clone()).expect("create WriteBatch error");
        let start_travel_records = std::time::Instant::now();
        let records = self.memtable.remove_flushable();
        stat.timecost_get_values += start_travel_records.elapsed().as_millis();
        stat.bytes_size += batch.disk_usage();
        self.write_to_batch(&mut batch, records, &mut stat).expect("write to WriteBatch error");
        let batch_disk_usage = batch.disk_usage();
        disk_usage += batch_disk_usage;
        let filemap = batch.commit(&mut stat).expect("batch commit error");
        self.sstables.extend(filemap);
        println!("disk_usage: {}, batch_disk_usage: {}, compaction_limit: {}", disk_usage, batch_disk_usage, compaction_limit);
        if disk_usage <= compaction_limit || force_flush {
            self.memtable.unpin_flush();
        } else {
            // compaction is a very heavy operation, so we block spin-read/write operations to save cpu cycles.
            stat.kind = "Compaction".to_string();
            self.memtable.pin_compaction();
            // trigger compaction
            let before_fids = self.manifest.lock().unwrap().all_fids();
            batch = WriteBatch::new(self.manifest.clone()).expect("create WriteBatch error");
            let start_travel_records = std::time::Instant::now();
            let records = self.memtable.remove_all();
            stat.timecost_get_values += start_travel_records.elapsed().as_millis();
            self.write_to_batch(&mut batch, records, &mut stat).expect("write to WriteBatch error");
            batch.mark_fid_invalid(before_fids);
            stat.bytes_size += batch.disk_usage();
            // 
            let filemap = batch.commit(&mut stat).expect("batch commit error");
            self.sstables.extend(filemap);
            // adjust compaction_limit
            let prev_disk_usage = disk_usage;
            disk_usage = self.manifest.lock().unwrap().disk_usage();
            assert!(disk_usage <= prev_disk_usage);
            // only small portion of records is duplicate, so we expand compaction_limit.
            if disk_usage * 12 / 10 >= prev_disk_usage {
                compaction_limit = 2 * prev_disk_usage;
            } else if disk_usage < prev_disk_usage / 4 && compaction_limit > 20 * FILE_SIZE_LIMIT {
                // most of records is duplicate, so we shrink compaction_limit.
                compaction_limit = prev_disk_usage / 2;
                // println!("adjust compaction_limit to {}", compaction_limit);
            }
            self.memtable.unpin_compaction();
            self.memtable.unpin_flush();
        }
        println!("{:?}", stat);
        (disk_usage, compaction_limit)
    }

    // TODO: return reference to avoid deep copy.
    // TODO: maybe sort by FileId to make this function more cache friendly.
    fn write_to_batch<'a>(&self, batch: &mut WriteBatch, entries: impl Iterator<Item = ReadGuard<'a, String, MemtableValue>>,
            stat: &mut Statuts) -> Result<()> {

        let mut rid;
        let start = std::time::Instant::now();
        let mut num_records = 0;

        for guard in entries {
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

                MemtableValue::RID((fid, pos)) => {
                    rid = batch.append_bytes(
                        self.sstables.get(&fid).unwrap().val().raw_read(pos.0, pos.1)?
                    )?;
                }

                MemtableValue::Value(None) => {
                    let record = KVRecord {
                        key: guard.key().to_owned(),
                        value: "".to_owned(),
                        kind: KVRecordKind::TOMB
                    };
                    rid = batch.append_bytes(&bincode::serialize(&record)?)?;
                }
            }

            self.memtable.raw_set(guard.key(), MemtableValue::RID(rid));;
        }
        stat.num_records += num_records;
        stat.timecost_prepare_and_reset += start.elapsed().as_millis();
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let func = || {
            (self.inner.memtable.set((&key).to_owned(), (&value).to_owned()), ())
        };
        let cb = |()| { Ok(()) };

        self.inner.retry_loop(func, cb)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let func = || {
            self.inner.memtable.get((&key).to_owned())
        };

        let cb = |v: Option<MemtableValue>| -> Result<Option<String>> {
            match v {
                Some(MemtableValue::RID(rid)) => {
                    let (fid, pos) = rid;
                    let record = self.inner.sstables.get(&fid)
                        .unwrap().val()
                        .read_record_at(pos.0, pos.1)?;
                    if record.kind == KVRecordKind::TOMB {
                        return Ok(None)
                    } else {
                        return Ok(Some(record.value))
                    }
                }

                Some(MemtableValue::Value(valstr)) => {
                    return Ok(valstr)
                }

                None => {
                    return Ok(None)
                }
            }
        };

        self.inner.retry_loop(func, cb)
    }

    fn remove(&self, key: String) -> Result<()> {
        let func = || {
            (self.inner.memtable.remove((&key).to_owned()), ())
        };

        let cb = |()| { Ok(()) };

        self.inner.retry_loop(func, cb)
    }
}

#[cfg(test)]
mod kvstore_unit_test {
    use crate::{Result, KvsEngine};

    use super::KvStore;

    #[test]
    fn test_concurrency() -> Result<()> {
        let db_path = tempfile::tempdir()?;
        let root_dir = db_path.path();
        let kvs = KvStore::new(root_dir)?;

        // thread num set.
        let num_set_threads = 20;
        let num_get_threads = 10;
        let num_remove_threads = 10;
        let num_records_per_thread = 100000;
        assert!(num_set_threads >= num_get_threads && num_set_threads >= num_remove_threads);
        assert!(num_set_threads/num_get_threads*num_get_threads == num_set_threads);
        assert!(num_set_threads/num_remove_threads*num_remove_threads == num_set_threads);

        // concurrent set
        let mut write_threads = Vec::new();
        for i in 0..num_set_threads {
            let kvs_handle = kvs.clone();
            let thread_handle = std::thread::spawn(move || {
                for j in 0..num_records_per_thread {
                    let key = format!("key-{}-{}", i, j);
                    let value = format!("value-{}-{}", i, j);
                    kvs_handle.set(key, value).unwrap();
                }
            });
            write_threads.push(thread_handle);
        }
        for h in write_threads {
            h.join().unwrap();
        }

        // concurrent get
        let mut get_threads = Vec::new();
        for i in 0..num_get_threads {
            let kvs_handle = kvs.clone();
            let thread_handle = std::thread::spawn(move || {
                for j in 0..num_set_threads/num_get_threads {
                    for k in 0..num_records_per_thread {
                        let key = format!("key-{}-{}", num_set_threads/num_get_threads*i + j, k);
                        let value = format!("value-{}-{}", num_set_threads/num_get_threads*i + j, k);
                        assert!(value == kvs_handle.get(key).unwrap().unwrap());
                    }
                }
            });
            get_threads.push(thread_handle);
        }
        for h in get_threads {
            h.join().unwrap();
        }

        // drop and then reopen.
        kvs.stop();
        let kvs = KvStore::open(root_dir)?;
        // concurrent remove and get
        let num_remove_threads = 10;
        let mut remove_threads = Vec::new();
        for i in 0..num_remove_threads {
            let kvs_handle = kvs.clone();
            let thread_handle = std::thread::spawn(move || {
                for j in 0..num_set_threads/num_remove_threads {
                    for k in 0..num_records_per_thread/2 {
                        let key = format!("key-{}-{}", num_set_threads/num_remove_threads*i + j, k);
                        kvs_handle.remove(key).unwrap();
                    }
                }
            });
            remove_threads.push(thread_handle);
        }

        let mut get_threads = Vec::new();
        for i in 0..num_get_threads {
            let kvs_handle = kvs.clone();
            let thread_handle = std::thread::spawn(move || {
                for j in 0..num_set_threads/num_get_threads {
                    for k in num_records_per_thread/2..num_records_per_thread {
                        let key = format!("key-{}-{}", num_set_threads/num_get_threads*i + j, k);
                        let value = format!("value-{}-{}", num_set_threads/num_get_threads*i + j, k);
                        assert_eq!(value, kvs_handle.get(key).unwrap().unwrap())
                    }
                }
            });
            get_threads.push(thread_handle);
        }
        for h in remove_threads {
            h.join().unwrap();
        }
        for h in get_threads {
            h.join().unwrap();
        }

        // drop and then reopen
        kvs.stop();
        // let kvs = KvStore::open(root_dir)?;
        // // final concurrent get
        // let mut get_threads = Vec::new();
        // for i in 0..num_get_threads {
        //     let kvs_handle = kvs.clone();
        //     let thread_handle = std::thread::spawn(move || {
        //         for j in 0..num_set_threads/num_get_threads {
        //             for k in num_records_per_thread/2..num_records_per_thread {
        //                 let key = format!("key-{}-{}", num_set_threads/num_get_threads*i + j, k);
        //                 let value = format!("value-{}-{}", num_set_threads/num_get_threads*i + j, k);
        //                 assert_eq!(value, kvs_handle.get(key).unwrap().unwrap())
        //             }
        //         }
        //     });
        //     get_threads.push(thread_handle);
        // }
        // for h in get_threads {
        //     h.join().unwrap();
        // }

        // congratulations!
        Ok(())
    }
}