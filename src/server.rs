use std::{
    collections::hash_map::DefaultHasher,
    future::Future,
    hash::{Hash, Hasher},
    sync::atomic::AtomicU64,
    sync::Arc,
};

use core::mem;
use tokio::{net::TcpListener, spawn, sync::*};
use tracing::*;

use crate::{cmd::*, connection::*, db::*, protocol::Frame, shutdown::Shutdown, Result};

const BUFFERSIZE: usize = 10;

fn num_partitions() -> usize {
    (num_cpus::get() * 4).next_power_of_two()
}

fn calculate_hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}

pub fn determine_database(hash: usize) -> usize {
    // Leave the high 7 bits for the HashBrown SIMD tag.
    (hash << 7) >> (mem::size_of::<usize>() * 8 - (num_partitions().trailing_zeros() as usize))
}
#[derive(Debug)]
pub struct Shared {
    num_partition: usize,
    // state: State,
    counter: AtomicU64,
    tasks_tx: Vec<mpsc::Sender<TaskParam>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            num_partition: num_partitions(),
            counter: AtomicU64::new(0),
            tasks_tx: Vec::with_capacity(num_partitions()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Dispatcher {
    shared: Arc<Shared>,
    notify_background_task: Arc<broadcast::Sender<()>>,
}

impl Dispatcher {
    pub fn new() -> Self {
        let mut shared = Shared::new();
        let mut tasks_rx = Vec::with_capacity(num_partitions());
        for _ in 0..shared.num_partition {
            let (tx, rx) = mpsc::channel(BUFFERSIZE);
            shared.tasks_tx.push(tx);
            tasks_rx.push(rx);
        }

        let shared = Arc::new(shared);
        let (notify_tx, _) = broadcast::channel(1);
        for (id, rx) in tasks_rx.drain(..).enumerate() {
            let notify_copy = notify_tx.subscribe();
            spawn(async move {
                database_manager(rx, notify_copy, id).await;
            });
        }
        Self {
            shared,
            notify_background_task: Arc::new(notify_tx),
        }
    }

    pub fn determine_db(cmd: &Command) {}
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == 1 {
            let _ = self.notify_background_task.send(());
        }
    }
}
pub struct Listener {
    listener: TcpListener,
    dispatcher: Dispatcher,

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
                dispatcher: self.dispatcher.clone(),
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
    dispatcher: Dispatcher,
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
                Ok(mut cmd) => {
                    debug!("parsed command: {:?}", cmd);
                    let nounce = self
                        .dispatcher
                        .shared
                        .counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let (ret_tx, mut ret_rx) = oneshot::channel();
                    let notify = Arc::new(Notify::new());
                    cmd.set_nounce(nounce);
                    let db_id = determine_database(calculate_hash(cmd.get_key()));

                    self.dispatcher.shared.tasks_tx[db_id]
                        .clone()
                        .send((cmd, ret_tx, notify.clone()))
                        .await?;

                    notify.notified().await;
                    ret_rx.try_recv()?
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
        dispatcher: Dispatcher::new(),
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
