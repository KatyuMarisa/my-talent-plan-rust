extern crate failure_derive;

mod errors;
// mod dbfile;
// mod manifest;
// mod kvfile;
mod engines;
mod server;
mod common;

pub use engines::{KvsEngine, SledKvsEngine};
pub use errors::{Result, KvError};
pub use common::{Request, Reply};
pub use server::KvServer;