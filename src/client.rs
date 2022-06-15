use async_bincode::AsyncDestination;
use async_bincode::tokio::AsyncBincodeStream;
use futures::{StreamExt, SinkExt};
use tokio::net::TcpStream as AsyncTcpStream;

use crate::common::{Request, Reply, kv_server_error::*};

pub struct Client {
    stream: AsyncClientBincodeStream,
}

impl Client {
    pub async fn new(addr: &str) -> Result<Self> {
        let conn: AsyncTcpStream = AsyncTcpStream::connect(addr).await?;
        Ok(
            Self { stream: AsyncBincodeStream::from(conn).for_async() }
        )
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = Request::GET { key };
        let reply = self.ping_pong(req).await??;
        return Ok(reply)
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = Request::SET { key ,value };
        self.ping_pong(req).await??;
        Ok(())
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        let req = Request::REMOVE { key };
        self.ping_pong(req).await??;
        Ok(())
    }

    pub async fn ping_pong(&mut self, req: Request) -> Result<Reply> {
        let (mut read_half, mut write_half) = self.stream.tcp_split();
        write_half.send(req).await?;
        if let Some(reply) = read_half.next().await {
            return Ok(reply?)
        }
        unreachable!("cannot read from stream!");
    }
}

pub type AsyncClientBincodeStream
    = async_bincode::tokio::AsyncBincodeStream<AsyncTcpStream, Reply, Request, AsyncDestination>;

#[allow(dead_code)]
pub type AsyncServerBincodeStream
    = async_bincode::tokio::AsyncBincodeStream<AsyncTcpStream, Request, Reply, AsyncDestination>;

#[cfg(test)]
mod client_unit_test {
    use async_bincode::tokio::AsyncBincodeStream;
    use futures::{StreamExt, SinkExt};
    use tokio::net::TcpListener;

    use super::{AsyncServerBincodeStream, Client};

    use crate::{Reply, Request};

    #[test]
    fn simple_server_test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        static ADDR: &str = "127.0.0.1:9126";

        // an simple echo server
        rt.spawn(async {
            let listener = TcpListener::bind(ADDR).await.unwrap();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                println!("got new connection!");
                let mut stream: AsyncServerBincodeStream
                    = AsyncBincodeStream::from(stream).for_async();

                let (mut read_half, mut write_half)
                    = stream.tcp_split();

                loop {
                    if let Some(Ok(req)) = read_half.next().await {
                        println!("{:?}", req);
                        let key = match req {
                            Request::SET { key, value: _ } => key,
                            Request::GET { key } => key,
                            Request::REMOVE { key } => key,
                        };
                        write_half.send(Reply::Ok(Some(key))).await.unwrap();
                    } else {
                        println!("connection close");
                        break;
                    }
                }
            }
        });

        let rt2 = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        rt2.block_on(async {
            for i in 0..10 {
                let mut cli = Client::new(ADDR).await.unwrap();
                cli.set(format!("key-{}", i), format!("value-{}", i)).await.unwrap();
                cli.get(format!("key-{}", i)).await.unwrap();
                cli.remove(format!("key-{}", i)).await.unwrap();
            }
        });

        println!("success!");
    }
}
