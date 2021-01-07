use std::net::{TcpStream, TcpListener};
use tokio::sync::*;

pub struct Listener {
    listener: TcpListener,
    db: crate::db::DB,

    shutdown_signal: broadcast::Sender<()>
}
