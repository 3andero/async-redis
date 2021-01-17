use std::future::Future;
use tokio::{net::TcpListener, sync::*};
use tracing::*;

use crate::{cmd::*, connection::*, db::*, protocol::Frame, shutdown::Shutdown, Result};

pub struct Listener {
    listener: TcpListener,
    db: DB,

    shutdown_begin: broadcast::Sender<()>,

    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    #[instrument(skip(self))]
    async fn run(&self) -> Result<()> {
        debug!("Server Started");
        loop {
            let (stream, _) = self.listener.accept().await?;
            debug!("stream accepted: {:?}", &stream);

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
                        error!("error occur while handling: {}", e);
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
    #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        while !self.shutdown_begin.is_shutdown() {
            // debug!("handling: {:?}", self.db.shared.database);
            let opt_frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame() => res?
            };

            debug!("frame received: {:?}", opt_frame);
            let frame = match opt_frame {
                Some(f) => f,
                None => {
                    return Ok(());
                }
            };

            let command = Command::new(&frame);
            let ret_frame = match command {
                Ok(cmd) => {
                    debug!("parsed command: {:?}", cmd);
                    cmd.exec(&self.db)
                }
                Err(e) => match e.downcast_ref::<CommandError>() {
                    Some(e) => Frame::Errors(format!("{}", e).into()),
                    None => {
                        return Err(e);
                    }
                },
            };
            debug!("ret_frame: {:?}", ret_frame);
            self.connection.write_frame(&ret_frame).await?;
        }
        Ok(())
    }
}

#[instrument(skip(shutdown_signal))]
pub async fn run(listener: TcpListener, shutdown_signal: impl Future) {
    debug!("Serving Entered");
    let (shutdown_begin_tx, _) = broadcast::channel(1);

    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let server = Listener {
        listener,
        db: DB::new(),
        shutdown_begin: shutdown_begin_tx,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            match res {
                Err(e) => {
                    debug!("{}", e);
                }
                _ => ()
            }
        }
        _ = shutdown_signal => {
            debug!("Ctrl+C");
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
    debug!("Shutdown Complete");
}
