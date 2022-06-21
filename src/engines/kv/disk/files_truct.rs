use crate::Result;
use super::Storage;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use std::fmt::Debug;

pub type Pos = (usize, usize);

impl<const MAGIC: u8> OrdinaryHeader for DefaultHeader<MAGIC> { }
pub trait OrdinaryHeader: Serialize + DeserializeOwned + Debug + FixSizedHeader + Default + Send + Sync{ }
pub trait OrdinaryRecord: Serialize + DeserializeOwned + Debug + Send + Sync { }

pub trait Readable<Header, Record>: Sync + Send
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    fn open(file: Box<dyn Storage>) -> Result<Self> where Self: Sized;
    fn read_record_at(&self, offset: usize, length: usize) -> Result<Record>;
    fn all_records(&self) -> Result<Vec<(Pos, Record)>>;
    fn raw_read(&self, offset: usize, length: usize) -> Result<&[u8]>;
}

pub trait Appendable<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    fn init(file: Box<dyn Storage>) -> Result<Self> where Self: Sized;
    fn append_record(&mut self, r: &Record) -> Result<Pos>;
    fn raw_append(&mut self, data: &[u8]) -> Result<Pos>;
    fn sync(&mut self) -> Result<()>;
}

pub trait ReadableAppendable<H, R>: Readable<H, R> + Appendable<H, R>
where
    H: OrdinaryHeader,
    R: OrdinaryRecord
{ }

pub trait FixSizedHeader {
    fn header_length() -> usize;
    fn default_serialize() -> Result<Vec<u8>>;
    fn magic(&self) -> u8;
    fn get_file_length(&self) -> usize;
    fn set_file_length(&mut self, len: usize);
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DefaultHeader<const MAGIC: u8>
{
    magic: u8,
    file_length: usize,
}

impl<const MAGIC: u8> FixSizedHeader for DefaultHeader<MAGIC> {
    fn magic(&self) -> u8 {
        self.magic
    }

    fn header_length() -> usize {
        HEADER_LENGTH as usize
    }

    fn default_serialize() -> Result<Vec<u8>> {
        let mut data = bincode::serialize(&Self::default())?;
        assert!(data.len() <= Self::header_length());
        data.resize(Self::header_length(), 0);
        Ok(data)
    }

    fn get_file_length(&self) -> usize {
        self.file_length
    }

    fn set_file_length(&mut self, len: usize) {
        self.file_length = len;
    }
}

impl<const MAGIC: u8> Default for DefaultHeader<MAGIC> {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            file_length: Self::header_length()
        }
    }
}

const HEADER_LENGTH: u8 = 24;
