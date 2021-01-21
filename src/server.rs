use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    future::Future,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU32, AtomicU64},
    sync::Arc,
};

use core::mem;
use tokio::{net::TcpListener, select, spawn, sync::*};
use tracing::*;

use crate::{cmd::*, connection::*, db::*, protocol::Frame, shutdown::Shutdown, Result};

const BUFFERSIZE: usize = 150;

fn num_partitions() -> usize {
    (num_cpus::get() * 8).next_power_of_two()
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

async fn recycle_handler(mut handler: Handler, sender: mpsc::Sender<Handler>) {
    while let Some((&db_id, &num_used)) = handler.sent.iter().next() {
        if num_used == 0 {
            match handler.dispatcher.shared.tasks_tx[db_id]
                .send(TaskParam::Remove(handler.id))
                .await
            {
                Err(e) => {
                    error!(
                        "error occured while recycling handler[{}]: {}",
                        handler.id, e
                    );
                    return;
                }
                _ => (),
            }

            handler.sent.remove(&db_id);
        }
    }

    let _ = sender.send(handler).await;
}

impl Listener {
    #[instrument(skip(self))]
    async fn run(&self) -> Result<()> {
        debug!("Server Started");
        let mut float_num: u64 = 0;
        let (recycle_tx, recycle_rx) = mpsc::channel(1000);
        let channel_counter = AtomicU32::new(0);
        loop {
            // select! {}
            let (stream, _) = self.listener.accept().await?;
            debug!("stream accepted: {:?}", &stream);

            let conn = Connection::new(stream);

            if channel_counter > 0 {
            } else {
                float_num += 1;
                let (ret_tx, ret_rx) = mpsc::channel(1);
                let mut handler = Handler {
                    connection: conn,
                    dispatcher: self.dispatcher.clone(),
                    shutdown_begin: Shutdown::new(self.shutdown_begin.subscribe()),
                    sent: BTreeMap::new(),
                    ret_rx,
                    ret_tx,
                    shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                    id: float_num,
                };
            }

            let recycle_tx_copy = recycle_tx.clone();
            tokio::spawn(async move {
                match handler.run().await {
                    Err(e) => {
                        error!("error occured while handling: {}", e);
                    }
                    _ => (),
                }
                recycle_handler(handler, recycle_tx_copy).await;
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
    sent: BTreeMap<usize, u32>,
    ret_tx: mpsc::Sender<Frame>,
    ret_rx: mpsc::Receiver<Frame>,
    id: u64,
}

impl Handler {
    #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        let mut sent = vec![false; self.dispatcher.shared.tasks_tx.len()];
        let (ret_tx, mut ret_rx) = mpsc::channel(1);
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

                    cmd.set_nounce(nounce);
                    let db_id = determine_database(calculate_hash(cmd.get_key()));

                    let option_tx = if sent[db_id] {
                        None
                    } else {
                        Some(ret_tx.clone())
                    };

                    self.dispatcher.shared.tasks_tx[db_id]
                        .send(TaskParam::Task((cmd, self.id, option_tx)))
                        .await?;

                    sent[db_id] = true;

                    ret_rx.recv().await.unwrap()
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
