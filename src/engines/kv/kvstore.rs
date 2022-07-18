use crate::{Result, KvsEngine};
use super::bufring::create_buf_ring;
use super::compaction::Compactor;
use super::inner::KvStoreInner;

use std::sync::{Arc, Condvar};
use std::path::PathBuf;

#[derive(Clone)]
pub struct KvStore {
    inner: Arc<KvStoreInner>,
    _compactor: Arc<Compactor>,
}

impl KvStore {
    pub fn open(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let bg_cond = Arc::new(Condvar::new());
        let ring = Arc::new(create_buf_ring());
        let inner = KvStoreInner::open(root_dir, bg_cond.clone(), ring)?;
        let inner = Arc::new(inner);

        let compactor = Arc::new(Compactor::new(inner.clone(), bg_cond.clone()));
        let compactor2 = compactor.clone();
        let compactor3 = compactor.clone();

        std::thread::spawn(move || compactor2.monitor_loop());
        std::thread::spawn(move || compactor3.flush_compaction_loop());
        Ok( KvStore { inner, _compactor: compactor } )
    }

    pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let bg_cond = Arc::new(Condvar::new());
        let ring = Arc::new(create_buf_ring());
        let inner = KvStoreInner::new(root_dir, bg_cond.clone(), ring)?;
        let inner = Arc::new(inner);

        let compactor = Arc::new(Compactor::new(inner.clone(), bg_cond.clone()));
        let compactor2 = compactor.clone();
        let compactor3 = compactor.clone();

        std::thread::spawn(move || compactor2.monitor_loop());
        std::thread::spawn(move || compactor3.flush_compaction_loop());
        Ok( Self { inner, _compactor: compactor })
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner.set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.inner.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.inner.remove(key)
    }
}

/// The drop of KvStore will force memtable flush, it is triggered by
/// the drop of Compactor.
impl Drop for KvStore {
    fn drop(&mut self) { }
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
        drop(kvs);
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
        drop(kvs);
        let kvs = KvStore::open(root_dir)?;
        // final concurrent get
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
        for h in get_threads {
            h.join().unwrap();
        }

        // congratulations!
        Ok(())
    }
}