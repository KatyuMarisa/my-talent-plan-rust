use std::fs::{create_dir_all, OpenOptions, remove_file};
use std::{fmt::Debug, path::PathBuf};
use std::marker::PhantomData;

use memmap::{MmapMut};
use serde::Deserialize;
use serde::{Serialize, de::DeserializeOwned};
use bincode::{serialize, deserialize, deserialize_from as deserialize_stream};

use crate::errors::{Result, KvError};

pub trait Storage {
    fn new(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized;
    fn read(&self, offset: usize, length: usize) -> Result<&[u8]>;  // TODO: Shallow Copy
    fn write(&mut self, offset: usize, data: &[u8]) -> Result<()>; 
    fn sync(&mut self) -> Result<()>;
    fn close(self) -> Result<()>;
    fn remove(self) -> Result<()>;  // TODO: is move better than borrow?
}

pub type Pos = (usize, usize);

pub struct MmapFile {
    mmap: MmapMut,
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

pub struct DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    file: Box<dyn Storage>,
    header: Header,
    phantom: (PhantomData<Header>, PhantomData<Record>),
}

impl<Header, Record> DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord,
{
    pub fn open(file: Box<dyn Storage>, init: bool) -> Result<Self> {
        let header = {
            if init {
                Header::default()
            } else {
                deserialize(
                    file.read(0, Header::header_length())?
                )?
            }
        };

        let mut result = Self{
            file,
            header,
            phantom: (PhantomData, PhantomData),
        };

        if init {
            result.reset_file_length(Header::header_length())?;
            result.sync()?;
        }

        Ok(result)
    }

    pub fn read_record_at(&self, offset: usize, length: usize) -> Result<Record> {
        Ok (deserialize(self.file.read(offset, length)?)? )
    }

    pub fn append_record(&mut self, record: &Record) -> Result<Pos> {
        let start = self.header.get_file_length();
        let data = serialize(&record)?;
        if start + data.len() > FILE_SIZE_LIMIT {
            return Err(KvError::FileSizeExceed.into())
        } else {
            self.file.write(start, &data)?;
            self.reset_file_length(start + data.len())?;
            Ok((start, data.len()))
        }
    }

    pub fn reset_file_length(&mut self, length: usize) -> Result<()> {
        self.header.set_file_length(length);
        let mut data = serialize(&self.header)?;
        data.resize(Header::header_length(), 0);
        self.file.write(0, &data)
    }

    // TODO: split block into meta_block and data_block for quick recover
    pub fn all_records(&self) -> Result<Vec<(Pos, Record)>> {
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

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync()
    }

    pub fn close(&mut self) -> Result<()> {
        self.file.sync()?;
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
        HEADER_LENGTH
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

pub trait OrdinaryHeader: Serialize + DeserializeOwned + Debug + FixSizedHeader + Default { }
pub trait OrdinaryRecord: Serialize + DeserializeOwned + Debug { }

const HEADER_LENGTH: usize = 24;
const PAGES_PER_FILE: usize = 3;
pub const FILE_SIZE_LIMIT: usize = 4096 * PAGES_PER_FILE;

#[cfg(test)]
mod dbfile_unit_tests {
    #[test]
    fn simple_encdec_test() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut file_path: PathBuf = temp_dir.path().into();
        file_path.push("test.dbf");

        let mut dbf = TestFile::open(Box::new(
            MmapFile::new(file_path)?
        ), true)?;

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
        ), true)?;

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
        ), false)?;

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

    impl crate::dbfile::OrdinaryRecord for TestRecord { }

    const TEST_HEADER_MAGIC: u8 = 254;
    type TestFile = DataBaseFile<DefaultHeader<TEST_HEADER_MAGIC>, TestRecord>;

    use serde::{Serialize, Deserialize};
    use tempfile::tempdir;

    use crate::{Result, dbfile::{Storage, MmapFile}};
    use std::{path::PathBuf};
    use super::{DataBaseFile, DefaultHeader};
}
