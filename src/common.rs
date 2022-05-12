use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SET{key: String, value: String},
    GET{key: String},
    REMOVE{key: String}
}


pub type ErrMsg = String;
pub type Reply = std::result::Result<Option<String>, ErrMsg>;
