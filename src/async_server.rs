use async_bincode::tokio::AsyncBincodeStream;
use futures::{StreamExt, SinkExt};

use crate::KvError;
use crate::client::AsyncServerBincodeStream;
use crate::engines::AsyncKvsEngine;
use crate::{Result, common::Request};

pub struct AsyncKvServer<E: AsyncKvsEngine> {
    engine: E,
    addr: String
}

impl<E: AsyncKvsEngine> AsyncKvServer<E> {
    pub fn new(engine: E, addr: String) -> Self {
        Self { engine, addr }
    }

    // runtime需要由外界来提供
    pub async fn run(&mut self) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(&self.addr).await?;
        loop {
            let stream_wrapper = listener.accept().await;
            match stream_wrapper {
                Ok((tcp_stream, _)) => {
                    let stream: AsyncServerBincodeStream
                        = AsyncBincodeStream::from(tcp_stream).for_async();

                    let engine = self.engine.clone();
                    tokio::spawn(async move {
                        if let Err(err) = serve(engine, stream).await {
                            eprintln!("{}", err.to_string());
                        }
                    });
                }

                Err(err) => {
                    eprintln!("accept error: {}", err.to_string());
                }
            }
        }
    }
}

async fn serve<E: AsyncKvsEngine> (engine: E, mut stream: AsyncServerBincodeStream) -> Result<()> {
    let (mut read_half, mut write_half) = stream.tcp_split();
    loop {
        if let Some(Ok(req)) = read_half.next().await {
            let res = match req {
                Request::SET { key, value } => {
                    if let Err(err) = engine.async_set(key, value).await {
                        return Err(err)
                    }
                    Ok(None)
                },

                Request::GET { key } => {
                    match engine.async_get(key).await {
                        Ok(get_result) => Ok(get_result),
                        Err(err) => Err(err)
                    }
                },

                Request::REMOVE { key } => {
                    if let Err(err) = engine.async_remove(key).await {
                        return Err(err)
                    }
                    Ok(None)
                },
            };

            // Even though remove a non-exist key returns KvError, it doesn't cause database corrpution.
            let mut corrupt = true;
            if let Err(err) = res.as_ref() {
                if let Some(KvError::KeyNotFoundError { key: _ }) = err.downcast_ref::<KvError>() {
                    corrupt = false;
                }
            }

            let reply = {
                if corrupt {
                    Ok(res.unwrap())
                } else {
                    Err(res.unwrap_err().to_string())
                }
            };
            write_half.send(reply).await?;
        } else {
            println!("connection close");
            break;
        }
    }
    Ok(())
}
