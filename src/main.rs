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

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() -> Result<()> {
    // tracing_subscriber::fmt::init();
    let collector = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(tracing::Level::ERROR)
        // build but do not install the subscriber.
        .finish();

    let _ = tracing::subscriber::set_global_default(collector);
    let addr = env::args()
        .skip(1)
        .next()
        .unwrap_or("127.0.0.1:7777".to_owned());

    let addr = addr.parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).await?;

    server::run(listener, tokio::signal::ctrl_c()).await;
    Ok(())
}
