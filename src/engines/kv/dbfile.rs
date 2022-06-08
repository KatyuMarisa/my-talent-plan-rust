use std::fs::{create_dir_all, OpenOptions, remove_file};
use std::path::PathBuf;
use std::marker::PhantomData;
use std::fmt::Debug;

use memmap::MmapMut;
use serde::Deserialize;
use serde::{Serialize, de::DeserializeOwned};
use bincode::{serialize, deserialize, deserialize_from as deserialize_stream};

use crate::errors::{Result, KvError};

pub trait Storage: Sync + Send {
    fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
    fn read(&self, offset: usize, length: usize) -> Result<&[u8]>;
    fn write(&mut self, offset: usize, data: &[u8]) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn close(self) -> Result<()>;
    fn remove(self) -> Result<()>;
}

pub struct MmapFile {
    mmap: memmap::MmapMut,
    path: PathBuf,
}

impl Storage for MmapFile {
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
        let pb: PathBuf = path.into();
        let f = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .open(&pb)?;

        Ok (Self{
            mmap: unsafe { MmapMut::map_mut(&f)? },
            path: pb
        })
    }

    fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
        let pb: PathBuf = path.into();
        create_dir_all(pb.parent().expect("invalid path"))?;
        let f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&pb)?;

        f.set_len(FILE_SIZE_LIMIT as u64)?;
        Ok(Self{
            mmap: unsafe { MmapMut::map_mut(&f)? },
            path: pb
        })
        
    }

    fn read(&self, offset: usize, length: usize) -> Result<&[u8]> {
        Ok (
            &self.mmap[offset..offset + length]
        )
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        Ok (
            (&mut self.mmap[offset..offset + data.len()]).copy_from_slice(data)
        )
    }

    fn sync(&mut self) -> Result<()> {
        Ok (
            self.mmap.flush()?
        )
    }

    fn remove(self) -> Result<()> {
        Ok (
            remove_file(self.path)?
        )
    }

    fn close(self) -> Result<()> {
        drop(self.mmap);
        Ok(())
    }
}

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

pub type Pos = (usize, usize);

pub struct DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    file: Box<dyn Storage>,
    header: Header,
    _phantom: PhantomData<Record>
}

impl<Header, Record> Readable<Header, Record> for DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    fn open(file: Box<dyn Storage>) -> Result<Self> where Self: Sized {
        let header = deserialize(file.read(0, Header::header_length())?)?;
        let result = Self{
            file,
            header,
            _phantom: PhantomData
        };
        Ok(result)
    }

    fn read_record_at(&self, offset: usize, length: usize) -> Result<Record> {
        Ok (
            deserialize(self.file.read(offset, length)?)?
        )
    }

    fn all_records(&self) -> Result<Vec<(Pos, Record)>> {
        let mut records = Vec::<(Pos, Record)>::new();
        let mut cursor = std::io::Cursor::new(
            self.file.read(Header::header_length(),
                self.header.get_file_length() - Header::header_length())?
        );
        let mut offset = 0;
        loop {
            match deserialize_stream(&mut cursor) {
                Ok(record) => {
                    let prev = offset;
                    offset = cursor.position() as usize;
                    records.push((
                        (prev + Header::header_length(), offset - prev),
                        record
                    ));
                }
                Err(err) => {
                    let mut is_error = true;
                    if let bincode::ErrorKind::Io(io_err) = err.as_ref() {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            is_error = false;
                        }
                    }
                    if is_error {
                        return Err(KvError::from(err).into())
                    } else {
                        break;
                    }
                }
            }
        }
        return Ok(records)
    }

    fn raw_read(&self, offset: usize, length: usize) -> Result<&[u8]> {
        self.file.read(offset, length)
    }
}


impl<Header, Record> Appendable<Header, Record> for DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    fn init(file: Box<dyn Storage>) -> Result<Self> where Self: Sized {
        let header = Header::default();
        let mut res = Self {
            file,
            header,
            _phantom: PhantomData
        };

        res.reset_file_length(Header::header_length())?;
        res.sync()?;
        Ok(res)
    }

    fn append_record(&mut self, r: &Record) -> Result<Pos> {
        let start = self.header.get_file_length();
        let data = bincode::serialize(r)?;
        if start + data.len() > FILE_SIZE_LIMIT {
            return Err(KvError::FileSizeExceed.into())
        } else {
            self.file.write(start, &data)?;
            self.reset_file_length(start + data.len())?;
            Ok((start, data.len()))
        }
    }

    fn raw_append(&mut self, data: &[u8]) -> Result<Pos> {
        let start = self.header.get_file_length();
        if start + data.len() > FILE_SIZE_LIMIT {
            return Err(KvError::FileSizeExceed.into());
        } else {
            self.file.write(start, data)?;
            self.reset_file_length(start + data.len())?;
            Ok((start, data.len()))
        }
    }

    fn sync(&mut self) -> Result<()> {
        let mut data = bincode::serialize(&self.header)?;
        data.resize(Header::header_length(), 0);
        self.file.write(0, &data)?;
        self.file.sync()
    }
}

impl<Header, Record> DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    fn reset_file_length(&mut self, length: usize) -> Result<()> {
        self.header.set_file_length(length);
        Ok(())
    }

    fn close(self) -> Result<()> {
        drop(self.file);
        Ok(())
    }
}


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
        let mut data = serialize(&Self::default())?;
        assert!(data.len() <= Self::header_length());
        data.resize(Self::header_length(), 0);
        Ok(data)
    }

    fn get_file_length(&self) -> usize {
        return self.file_length
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

impl<const MAGIC: u8> OrdinaryHeader for DefaultHeader<MAGIC> { }
pub trait OrdinaryHeader: Serialize + DeserializeOwned + Debug + FixSizedHeader + Default + Send + Sync{ }
pub trait OrdinaryRecord: Serialize + DeserializeOwned + Debug + Send + Sync { }

pub const HEADER_LENGTH: u8 = 24;
// pub const FILE_SIZE_LIMIT: usize = 1 << 20;
pub const FILE_SIZE_LIMIT: usize = 1 << 10; // For Test Only
const PAGE_SIZE: usize = 1 << 12;
const PAGES_PER_FILE: usize = FILE_SIZE_LIMIT / PAGE_SIZE;

#[cfg(test)]
mod dbfile_unit_tests {
    #[test]
    fn simple_encdec_test() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut file_path: PathBuf = temp_dir.path().into();
        file_path.push("test.dbf");

        let mut dbf = TestFile::open(Box::new(
            MmapFile::new(file_path)?
        ))?;

        let (off, len) = dbf.append_record( &TestRecord {
            x: 1,
            y: 2,
            s: "Hello Bincode".to_string()
        } )?;

        let r = dbf.read_record_at(off, len)?;
        assert!(r.x == 1 && r.y == 2 && r.s == "Hello Bincode".to_string());
        Ok(())
    }

    #[test]
    fn multi_encdec_test() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut file_path: PathBuf = temp_dir.path().into();
        file_path.push("test.dbf");

        let mut dbf = TestFile::open(Box::new(
            MmapFile::new(&file_path)?
        ))?;

        let records = dbf.all_records()?;
        assert_eq!(records.len(), 0);

        let mut rids = Vec::with_capacity(80);
        for i in 0..80 {
            let rid = dbf.append_record( &TestRecord{
                x: i,
                y: 2*i,
                s: format!("Hello Bincode, {}", i)
            }).unwrap();
            rids.push(rid);
        }

        for (i, (offset, len)) in rids.iter().enumerate() {
            let record = dbf.read_record_at(*offset, *len)?;
            assert!(record.x == i && record.y == 2*i && record.s == format!("Hello Bincode, {}", i));
        }

        let records = dbf.all_records()?;
        assert_eq!(records.len(), 80);
        for (i, (rid, record)) in records.iter().enumerate() {
            assert!(record.x == i && record.y == 2*i && record.s == format!("Hello Bincode, {}", i));
            assert_eq!(rid.0, rids.get(i).unwrap().0);
            assert_eq!(rid.1, rids.get(i).unwrap().1);
        }

        dbf.close()?;

        dbf = TestFile::open(Box::new(
            MmapFile::open(&file_path)?
        ))?;

        for (i, (offset, len)) in rids.iter().enumerate() {
            let record = dbf.read_record_at(*offset, *len)?;
            assert!(record.x == i && record.y == 2*i && record.s == format!("Hello Bincode, {}", i));
        }

        let records = dbf.all_records()?;
        for (i, (_, record)) in records.iter().enumerate() {
            assert!(record.x == i && record.y == 2*i && record.s == format!("Hello Bincode, {}", i));
        }

        Ok(())
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestRecord {
        x: usize,
        y: usize,
        s: String,
    }

    impl OrdinaryRecord for TestRecord { }

    const TEST_HEADER_MAGIC: u8 = 254;
    type TestFile = DataBaseFile<DefaultHeader<TEST_HEADER_MAGIC>, TestRecord>;

    use serde::{Serialize, Deserialize};
    use tempfile::tempdir;

    use crate::{Result, engines::kv::dbfile::{MmapFile, Storage}};
    use std::path::PathBuf;
    use super::{DataBaseFile, DefaultHeader, OrdinaryRecord, Readable, Appendable};
}
