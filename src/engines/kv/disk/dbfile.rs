use crate::{KvError, Result};

use super::{files_truct::{OrdinaryHeader, OrdinaryRecord, Readable, Pos, Appendable}, Storage, FILE_SIZE_LIMIT};

use std::marker::PhantomData;

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
        let header = bincode::deserialize(file.read(0, Header::header_length())?)?;
        let result = Self{
            file,
            header,
            _phantom: PhantomData
        };
        Ok(result)
    }

    fn read_record_at(&self, offset: usize, length: usize) -> Result<Record> {
        Ok (bincode::deserialize(self.file.read(offset, length)?)?)
    }

    fn all_records(&self) -> Result<Vec<(Pos, Record)>> {
        let mut records = Vec::<(Pos, Record)>::new();
        let mut cursor = std::io::Cursor::new(
            self.file.read(Header::header_length(),
                self.header.get_file_length() - Header::header_length())?
        );
        let mut offset = 0;
        loop {
            match bincode::deserialize_from(&mut cursor) {
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
        Ok(records)
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
        // TODO: Avoid needless sync.
        Ok(res)
    }

    fn append_record(&mut self, r: &Record) -> Result<Pos> {
        let start = self.header.get_file_length();
        let data = bincode::serialize(r)?;
        if start + data.len() > FILE_SIZE_LIMIT {
            Err(KvError::FileSizeExceed.into())
        } else {
            self.file.write(start, &data)?;
            self.reset_file_length(start + data.len())?;
            Ok((start, data.len()))
        }
    }

    fn raw_append(&mut self, data: &[u8]) -> Result<Pos> {
        let start = self.header.get_file_length();
        if start + data.len() > FILE_SIZE_LIMIT {
            Err(KvError::FileSizeExceed.into())
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
}

impl<Header, Record> DataBaseFile<Header, Record>
where
    Header: OrdinaryHeader,
    Record: OrdinaryRecord
{
    #[allow(dead_code)]
    fn close(mut self) -> Result<()> {
        self.sync()
    }
}

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

    use crate::{Result, engines::kv::disk::{raw::MmapFile, dbfile::DataBaseFile, Storage, DefaultHeader, files_truct::{Readable, Appendable}}};
    use std::path::PathBuf;

    use super::{OrdinaryRecord};
}
