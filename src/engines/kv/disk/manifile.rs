use serde::{Serialize, Deserialize};

use crate::engines::kv::manifest::FileId;

use super::{
    files_truct::OrdinaryRecord,
    dbfile::DataBaseFile,
    DefaultHeader
};

#[derive(Serialize, Deserialize, Debug)]
pub struct ManifestRecord {
    pub added: Vec<FileId>,
    pub removed: Vec<FileId>,
}

impl OrdinaryRecord for ManifestRecord {  }
pub type ManifestFile = DataBaseFile<DefaultHeader<MAGIC_MANIFEST>, ManifestRecord>;
pub const MAGIC_MANIFEST: u8 = 0;
