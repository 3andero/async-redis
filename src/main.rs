use bytes::{Bytes, BytesMut};
use mini_redis;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::io::*;
use tokio::net::{TcpListener, TcpStream};
mod cmd;
mod connection;
mod db;
mod protocol;
mod server;
mod shutdown;
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

    server::run(listener, tokio::signal::ctrl_c()).await;
    Ok(())
}
