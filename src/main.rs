use bytes::{Bytes, BytesMut};
use mini_redis;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::io::*;
use tokio::net::{TcpListener, TcpStream};
mod connection;
mod parser;
mod utils;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::args()
        .skip(1)
        .next()
        .unwrap_or("127.0.0.1:7777".to_owned());

    let addr = addr.parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let res = read(stream).await?;
    }
    Ok(())
}

async fn read(stream: TcpStream) -> Result<usize> {
    let mut buf = BytesMut::new();
    let mut bufReader = BufWriter::new(stream);
    let ret = bufReader.read_buf(&mut buf).await?;
    Ok(ret)
}
