use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SET{key: String, value: String},
    GET{key: String},
    REMOVE{key: String}
}

pub type ErrMsg = String;
pub type Reply = std::result::Result<Option<String>, ErrMsg>;

pub mod kv_server_error {
    use crate::common::ErrMsg;

    #[derive(Debug)]
    pub enum KvsError {
        ErrMsg(String)
    }
    pub type Result<T> = std::result::Result<T, KvsError>;

    impl From<std::io::Error> for KvsError {
        fn from(err: std::io::Error) -> Self {
            KvsError::ErrMsg(err.to_string())
        }
    }

    impl From<ErrMsg> for KvsError {
        fn from(err: ErrMsg) -> Self {
            KvsError::ErrMsg(err)
        }
    }

    impl From<bincode::Error> for KvsError {
        fn from(err: bincode::Error) -> Self {
            KvsError::ErrMsg(err.to_string())
        }
    }
}
