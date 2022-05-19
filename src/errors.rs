use std::io;

use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "IOError: {}", err)]
    IOError{err: io::Error},
    #[fail(display = "EncodeDecodeError: {}", err)]
    EncodeDecodeError{err: bincode::Error},
    #[fail(display = "Key not found")]
    KeyNotFoundError{key: String},
    #[fail(display = "File size exceed.")]
    FileSizeExceed,
    #[fail(display = "DataBase corrpution")]
    MaybeCorrput,
    #[fail(display = "Race")]
    DataRace,
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> Self {
        KvError::IOError{err}
    }
}

impl From<bincode::Error> for KvError {
    fn from(err: bincode::Error) -> Self {
        KvError::EncodeDecodeError{err}
    }
}

impl From<Box<bincode::Error>> for KvError {
    fn from(err: Box<bincode::Error>) -> Self {
        KvError::EncodeDecodeError{err: *err}
    }
}

pub type Result<T> = std::result::Result<T, failure::Error>;
