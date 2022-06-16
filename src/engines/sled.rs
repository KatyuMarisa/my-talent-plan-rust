use super::{KvsEngine, AsyncKvsEngine};
use crate::{KvError, Result};

use async_trait::async_trait;
use sled::{Db, Tree};

/// Wrapper of `sled::Db`
#[derive(Clone)]
pub struct SledKvStore(Db);

impl SledKvStore {
    /// Creates a `SledKvsEngine` from `sled::Db`.
    pub fn new(db: Db) -> Self {
        SledKvStore(db)
    }
}

impl KvsEngine for SledKvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&self, key: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.remove(&key)?.ok_or(KvError::KeyNotFoundError {key})?;
        tree.flush()?;
        Ok(())
    }
}

#[async_trait]
impl AsyncKvsEngine for SledKvStore {
    async fn async_set(&self, key: String, value: String) -> Result<()> {
        self.set(key, value)
    }

    async fn async_get(&self, key: String) -> Result<Option<String>> {
        self.get(key)
    }

    async fn async_remove(&self, key: String) -> Result<()> {
        self.remove(key)
    }
}