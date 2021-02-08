use anyhow::Result;
use tracing::info;
use std::env;
use std::net::SocketAddr;
use tokio::net::TcpListener;
mod cmd;
mod connection;
mod db;
mod protocol;
mod server;
mod shutdown;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::args()
        .skip(1)
        .next()
        .unwrap_or("127.0.0.1:7777".to_owned());

    let loglevel = env::args()
        .skip(2)
        .next()
        .map_or(tracing::Level::INFO, |f| match &f.to_lowercase()[..] {
            "info" => tracing::Level::INFO,
            "debug" => tracing::Level::DEBUG,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            "trace" => tracing::Level::TRACE,
            _ => tracing::Level::INFO,
        });

    let collector = tracing_subscriber::fmt().with_max_level(loglevel).finish();

    let _ = tracing::subscriber::set_global_default(collector);

    info!("async-redis v0.2.0");
    let addr = addr.parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).await?;

    server::run(listener, tokio::signal::ctrl_c()).await;
    Ok(())
}
