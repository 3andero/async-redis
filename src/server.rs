use std::future::Future;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::*,
};

use crate::{cmd::*, connection::*, db::*, shutdown::Shutdown, Result};

pub struct Listener {
    listener: TcpListener,
    db: DB,

    shutdown_begin: broadcast::Sender<()>,

    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&self) -> Result<()> {
        println!("Server Started");
        loop {
            let (stream, _) = self.listener.accept().await?;
            // println!("stream accepted: {:?}", &stream);

            let conn = Connection::new(stream);

            let mut handler = Handler {
                connection: conn,
                db: self.db.clone(),
                shutdown_begin: Shutdown::new(self.shutdown_begin.subscribe()),
                shutdown_complete_tx: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                match handler.run().await {
                    Err(e) => {
                        // println!("error occur while handling: {}", e);
                    }
                    _ => (),
                }
            });
        }
    }
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    db: DB,
    shutdown_begin: Shutdown,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Handler {
    pub async fn run(&mut self) -> Result<()> {
        while !self.shutdown_begin.is_shutdown() {
            // println!("handling: {:?}", self);
            let opt_frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame() => res?
            };

            // println!("frame received: {:?}", opt_frame);
            let frame = match opt_frame {
                Some(f) => f,
                None => {
                    return Ok(());
                }
            };

            let command = Command::new(&frame)?;
            // println!("parsed command: {:?}", command);
            let ret_frame = command.exec(&self.db);
            // println!("ret_frame: {:?}", ret_frame);
            self.connection.write_frame(&ret_frame).await?;
        }
        Ok(())
    }
}

pub async fn run(listener: TcpListener, shutdown_signal: impl Future) {
    println!("Serving Entered");
    let (shutdown_begin_tx, shutdown_begin_rx) = broadcast::channel(1);

    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let server = Listener {
        listener: listener,
        db: DB::new(),
        shutdown_begin: shutdown_begin_tx,
        shutdown_complete_rx: shutdown_complete_rx,
        shutdown_complete_tx: shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            match res {
                Err(e) => {
                    // println!("{}", e);
                }
                _ => ()
            }
        }
        _ = shutdown_signal => {
            println!("Ctrl+C");
        }
    }

    let Listener {
        shutdown_begin,
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        ..
    } = server;

    drop(shutdown_begin);

    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
    println!("Shutdown Complete");
}
