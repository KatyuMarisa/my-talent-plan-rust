use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SET{key: String, value: String},
    GET{key: String, value: String},
    REMOVE{key: String}
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Reply {
    SetReply{value: RemoteKvResult},
    GetReply{value: RemoteKvResult},
    RemoveReply{value: RemoteKvResult},   
}

pub type RemoteKvResult = std::result::Result<String, String>;
