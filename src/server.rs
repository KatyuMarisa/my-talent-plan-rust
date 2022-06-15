use std::{net::{TcpListener, TcpStream}};

use bincode::deserialize_from;

use crate::{KvsEngine, common::{Request, Reply}, Result};

pub struct KvServer<Engine: KvsEngine> {
    engine: Engine,
    addr: String,
}

impl<Engine> KvServer<Engine>
where
    Engine: KvsEngine
{
    pub fn new(engine: Engine, ip_addr: String) -> Self {
        Self { engine, addr: ip_addr }
    }

    pub fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.addr.to_owned())?;
        for conn in listener.incoming() {
            self.serve(conn.unwrap())?;
        }
        Ok(())
    }

    pub fn serve(&mut self, conn: TcpStream) -> Result<()> {
        let req = deserialize_from::<_, Request>(&conn);
        
        let reply: Reply = match req {
            Ok(command) => {
                match command {
                    Request::SET { key, value } => {
                        match self.engine.set(key, value) {
                            Ok(()) => { Ok(None) }
                            Err(err) => { Err(format!("{}", err)) }
                        }
                    }

                    Request::GET { key } => {
                        match self.engine.get(key) {
                            Ok(value) => { Ok(value) }
                            Err(err) => { Err(format!("{}", err)) }
                        }
                    }

                    Request::REMOVE { key } => {
                        match self.engine.remove(key) {
                            Ok(()) => { Ok(None) }
                            Err(err) => { Err(format!("{}", err)) }
                        }
                    }
                }
            }

            Err(err) => {
                Err(format!("{}", err.to_string()))
            }
        };
        bincode::serialize_into(&conn, &reply)?;
        Ok(())
    }
}
