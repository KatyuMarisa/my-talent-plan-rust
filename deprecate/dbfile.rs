use std::error::Error;
use std::fs::{File, OpenOptions, create_dir_all};
use std::path::PathBuf;
use std::io::{BufReader, BufWriter, SeekFrom, Seek, Write};
use std::fmt::Debug;
use std::marker::PhantomData;

use bincode::ErrorKind;
use serde::{Serialize, de::DeserializeOwned};

use crate::Result;
use crate::errors::KvError;

/// A very poor data base file implemention.
/// The `writer` and `reader` use different user space buffer, which causes newly writen data may not visiable by reader, so a 
/// `flush` after each write is essential.
/// Besides, such implemention is not suitable for multi-thread condition. 
///
/// TODO: implement DBFile with mmap, or other better solutions.
///
/// Assumed that all files used by database has the unique format:
/// [Header, Record, Record, Record...] 
/// 

pub struct DBFile<Record>
where
    Record: Serialize + DeserializeOwned + Debug
{
    writer: BufWriter<File>,
    reader: BufReader<File>,
    phantom: PhantomData<Record>
}

impl<Record> DBFile<Record>
where
    Record: Serialize + DeserializeOwned + Debug
{
    pub fn open(path: impl Into<PathBuf>, create: bool) -> Result<Self> {
        let pb: PathBuf = path.into();

        if create {
            match create_dir_all(pb.parent().expect("invalid path")) {
                Ok(()) => { }
                Err(err) => {
                    if err.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(err)
                    }
                }
            }
        }

        Ok (
            Self {
                writer: BufWriter::new(
                    OpenOptions::new()
                    .create(create)
                    .read(false)
                    .write(true)
                    .append(true)
                    .open(pb.as_path())?
                ),
                reader: BufReader::new(
                    OpenOptions::new()
                        .read(true)
                        .open(pb.as_path())?
                ),
                phantom: PhantomData
            }
        )
    }

    pub fn read_record_at(&mut self, offset: u64) -> Result<Option<Record>> {
        self.reader.seek(SeekFrom::Start(offset))?;
        match bincode::deserialize_from::<&mut std::io::BufReader<File>, Record>(&mut self.reader) {
            Ok(record) => { Ok(Some(record)) }
            Err(err) => { Err(KvError::EncodeDecodeError(err.to_string())) }
        }
    }

    pub fn append_record(&mut self, r: Record) -> Result<u64> {
        let off = self.writer.seek(SeekFrom::Current(0))?;

        // TODO:maybe sync
        match bincode::serialize_into(&mut self.writer, &r) {
            Err(err) => { Err(KvError::EncodeDecodeError(err.to_string())) }
            Ok(()) => {
                // A flush is ensential.
                self.writer.flush()?;
                Ok(off)
            }
        }
    }

    pub fn all_records(&mut self) -> Result<Vec<Record>> {
        self.reader.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();

        loop {
            match bincode::deserialize_from::<_, Record>(&mut self.reader) {
                Ok(record) => {
                    records.push(record);
                }

                Err(err) => {
                    let mut is_error = true;
                    let reason_string = err.to_string();

                    if let ErrorKind::Io(io_err) = *err {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            is_error = false;   
                        }
                    }

                    if is_error {
                        return Err(KvError::EncodeDecodeError(reason_string))
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(records)
    }
}

#[cfg(test)]
mod dbfile_unit_tests {
    use std::{path::Path, fs::remove_file};

    use serde::{Serialize, Deserialize};

    use super::DBFile;
    #[derive(Debug, Serialize, Deserialize)]
    struct TestRecord {
        x: u32,
        y: i32,
        s: String,
    }

    #[test]
    fn simple_encdec_test() {
        let path = Path::new("/home/yukari/kvnew/kvs/testDB/tdb.dbf");
        remove_file(path).unwrap_or_default();

        let mut dbf = DBFile::<TestRecord>::open(path, true).unwrap();
        let off = dbf.append_record(TestRecord {
            x: 1,
            y: 2,
            s: "Hello Bincode".to_string(),
        }).unwrap();

        let r = dbf.read_record_at(off).unwrap().unwrap();
        assert!(r.x == 1 && r.y == 2 && r.s == "Hello Bincode".to_string());
        remove_file(path).unwrap();
    }

    #[test]
    fn multi_encdec_test() {
        let path = Path::new("/home/yukari/kvnew/kvs/testDB/tdb.dbf");
        remove_file(path).unwrap_or_default();
        let mut dbf = DBFile::<TestRecord>::open(path, true).unwrap();
        
        let mut offsets = Vec::new();
        for i in 0..10 {
            let off = dbf.append_record(TestRecord{
                x: i,
                y: 2*i as i32,
                s: format!("Hello Bincode, {}", i),
            }).unwrap();
            offsets.push(off);
        }

        for (i, off) in offsets.iter().enumerate() {
            let r = dbf.read_record_at(*off).unwrap().unwrap();
            assert!(
                r.x == i as u32 &&
                r.y == 2*i as i32 &&
                r.s == format!("Hello Bincode, {}", i)
            )
        }

        let records = dbf.all_records().unwrap();
        assert_eq!(records.len(), 10);

        for (i, record) in records.iter().enumerate() {
            assert!(
                record.x == i as u32 &&
                record.y == 2*i as i32 &&
                record.s == format!("Hello Bincode, {}", i)
            )
        }

        remove_file(path).unwrap();
    }
}