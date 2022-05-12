use serde::{Serialize, Deserialize};

use crate::dbfile::{DefaultHeader, OrdinaryRecord, DataBaseFile};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum KVRecordKind {
    KV,
    TOMB,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KVRecord {
    pub kind: KVRecordKind,
    pub key: String,
    pub value: String
}

impl OrdinaryRecord for KVRecord {  }

pub const MAGIC_KV: u8 = 1;
pub type KVFile = DataBaseFile<DefaultHeader<MAGIC_KV>, KVRecord>;
