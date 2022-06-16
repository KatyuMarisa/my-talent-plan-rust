use serde::{Serialize, Deserialize};

use super::dbfile::{DefaultHeader, OrdinaryRecord, DataBaseFile, Readable, Appendable};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum KVRecordKind {
    KV,
    Tomb,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KVRecord {
    pub kind: KVRecordKind,
    pub key: String,
    pub value: String
}

impl OrdinaryRecord for KVRecord { }

pub const MAGIC_KV: u8 = 1;

pub type KVSSTable = dyn Readable<DefaultHeader<MAGIC_KV>, KVRecord>;
#[allow(dead_code)]
pub type KVWritable = dyn Appendable<DefaultHeader<MAGIC_KV>, KVRecord>;
pub type KVFile = DataBaseFile<DefaultHeader<MAGIC_KV>, KVRecord>; 
