// #[global_allocator]
// static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpListener;
mod cmd;
mod connection;
mod db;
mod protocol;
mod server;
mod shutdown;
mod utils;
use clap::{App, Arg};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("async-redis")
        .version("0.5.0")
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("thread")
                .short("t")
                .long("thread")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-level")
                .short("l")
                .long("log-level")
                .takes_value(true),
        )
        .get_matches();

    let addr = "127.0.0.1:".to_owned() + matches.value_of("port").unwrap_or("7777");

    let thread_num = matches
        .value_of("thread")
        .map(|v| match v.parse::<usize>() {
            Ok(v) => v,
            Err(_) => panic!("not a number"),
        })
        .unwrap_or(num_cpus::get());

    let loglevel = matches
        .value_of("log-level")
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

    let addr = addr.parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).await?;

    server::run(listener, tokio::signal::ctrl_c(), thread_num).await;
    Ok(())
}
