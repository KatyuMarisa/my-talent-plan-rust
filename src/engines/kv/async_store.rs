use crate::Result;
use crate::engines::AsyncKvsEngine;
use crate::thread_pool::ThreadPool;
use super::drop_guard::DropGuard;
use super::inner::{KvStoreInner, State};

use async_trait::async_trait;
use tokio::sync::oneshot;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, Condvar};

#[derive(Clone)]
pub struct AsyncKvStore<P: ThreadPool> {
    pool: P,
    inner: Arc<KvStoreInner>,
    #[allow(dead_code)]
    drop_guard: Arc<DropGuard>,
}

impl<P: ThreadPool> AsyncKvStore<P> {
    pub fn open(root_dir: impl Into<PathBuf>, nthread: u32) -> Result<Self> {
        let pool = P::new(nthread)?;
        let state = Arc::new(Mutex::new(State::RUNNING));
        let bg_cond = Arc::new(Condvar::new());
        let inner = Arc::new(KvStoreInner::open(root_dir, bg_cond.clone(), state.clone())?);
        let drop_guard = Arc::new(DropGuard::new(state, bg_cond));

        let inner2 = inner.clone();
        std::thread::spawn(move || {
            inner2.bg_flush_compaction_loop();
        });
        Ok( Self{ pool, inner, drop_guard } )
    }

    pub fn new(root_dir: impl Into<PathBuf>, nthread: u32) -> Result<Self> {
        let pool = P::new(nthread)?;
        let state = Arc::new(Mutex::new(State::RUNNING));
        let bg_cond = Arc::new(Condvar::new());
        let inner = Arc::new(KvStoreInner::new(root_dir, bg_cond.clone(), state.clone())?);
        let drop_guard = Arc::new(DropGuard::new(state, bg_cond));
        let inner2 = inner.clone();
        std::thread::spawn(move || {
            inner2.bg_flush_compaction_loop();
        });
  
        Ok(Self{ pool, inner, drop_guard })
    }

    async fn schedule<F, R>(&self, func: F) -> R
    where
        F: Send + 'static + FnOnce() -> R,
        R: Send + 'static
    {
        let (tx, rx) = oneshot::channel();
        self.pool.spawn(move || {
            if let Err(_) = tx.send(func()) {
                eprintln!("send error")
            }
        });
        rx.await
            .expect("receive error")
    }
}

/// Because all operations are directly memory access, async is meaningless.
#[async_trait]
impl<P: ThreadPool> AsyncKvsEngine for AsyncKvStore<P> {
    async fn async_set(&self, key: String, value: String) -> Result<()> {
        let inner = self.inner.clone();
        let func = move || {
            inner.set(key, value)
        };
        self.schedule(func).await
    }

    async fn async_get(&self, key: String) -> Result<Option<String>> {
        let inner = self.inner.clone();
        let func = move || {
            inner.get(key)
        };
        self.schedule(func).await
    }

    async fn async_remove(&self, key: String) -> Result<()> {
        let inner = self.inner.clone();
        let func = move || {
            inner.remove(key)
        };
        self.schedule(func).await
    }
}

#[cfg(test)]
mod async_kvstore_unit_test {
    use futures::future::join_all;

    use crate::{Result, thread_pool::{self}, engines::{kv::AsyncKvStore, AsyncKvsEngine}};
    #[test]
    fn test_concurrency() -> Result<()> {
        let db_path = tempfile::tempdir()?;
        let root_dir = db_path.path();
        // thread num set.
        let num_set_threads = 20;
        let num_get_threads = 10;
        let num_remove_threads = 10;
        let num_records_per_thread = 100000;
        assert!(num_set_threads >= num_get_threads && num_set_threads >= num_remove_threads);
        assert!(num_set_threads/num_get_threads*num_get_threads == num_set_threads);
        assert!(num_set_threads/num_remove_threads*num_remove_threads == num_set_threads);

        let store = AsyncKvStore
            ::<thread_pool::SharedQueueThreadPool>::new(root_dir, num_set_threads)?;
        
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .worker_threads(num_set_threads as usize)
            .build()
            .unwrap();

        let mut futures = Vec::new();
        for i in 0..num_set_threads {
            let store2 = store.clone();
            let fu = async move {
                for j in 0..num_records_per_thread {
                    store2.async_set(format!("key-{}-{}", i, j), format!("value-{}-{}", i, j))
                        .await.unwrap();
                }
            };
            futures.push(fu);
        }

        rt.block_on( async {
            join_all(futures).await;
        });
        Ok(())
    }
}