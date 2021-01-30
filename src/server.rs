use std::{
    collections::hash_map::DefaultHasher,
    future::Future,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU32, AtomicU64, Ordering::*},
    sync::Arc,
};

use bytes::Bytes;
use core::mem;
use tokio::{net::TcpListener, spawn, sync::*};
use tracing::*;

use crate::{
    cmd::*,
    connection::*,
    db::*,
    protocol::{Frame, FrameArrays},
    shutdown::Shutdown,
    Result,
};

const BUFFERSIZE: usize = 100;

fn calculate_hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}

#[derive(Debug)]
pub struct Dispatcher {
    num_threads: usize,
    counter: AtomicU64,
    tasks_tx: Vec<mpsc::Sender<TaskParam>>,
    _shift_param: usize,
}

impl Dispatcher {
    pub fn new(
        notify_tx: &broadcast::Sender<()>,
        shutdown_complete_tx: &mpsc::Sender<()>,
        num_threads: usize,
    ) -> Self {
        let mut tasks_tx = Vec::with_capacity(num_threads);
        let mut tasks_rx = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let (tx, rx) = mpsc::channel(BUFFERSIZE);
            tasks_tx.push(tx);
            tasks_rx.push(rx);
        }

        for (id, rx) in tasks_rx.drain(..).enumerate() {
            let notify_copy = notify_tx.subscribe();
            let shutdown_complete_tx_copy = shutdown_complete_tx.clone();
            spawn(async move {
                database_manager(rx, notify_copy, shutdown_complete_tx_copy, id).await;
            });
        }
        Self {
            num_threads,
            counter: AtomicU64::new(0),
            tasks_tx,
            _shift_param: (mem::size_of::<usize>() * 8 - (num_threads.trailing_zeros() as usize)),
        }
    }

    pub fn determine_database(&self, key: &Bytes) -> usize {
        // Leave the high 7 bits for the HashBrown SIMD tag.
        (calculate_hash(key) << 7) >> self._shift_param
    }

    // pub fn determine_database(&self, key: &Bytes) -> usize {
    //     // Leave the high 7 bits for the HashBrown SIMD tag.
    //     let mut hash = 0;
    //     for b in key {
    //         hash = (hash + *b as usize) % self.num_partition;
    //     }
    //     hash
    // }
}
pub struct Listener {
    listener: TcpListener,
    dispatcher: Arc<Dispatcher>,

    shutdown_begin: broadcast::Sender<()>,

    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    num_threads: usize,
}

#[instrument(skip(handler, sender))]
async fn recycle_handler(mut handler: Handler, sender: mpsc::Sender<Handler>) -> bool {
    debug!("[{}]: entered", handler.id);
    handler.age += 1;
    if handler.age % 10 == 0 {
        for db_id in 0..handler.sent.len() {
            debug!("[{}]: checking db({})", handler.id, db_id);
            handler.sent[db_id] = if handler.sent[db_id] == 0 {
                debug!("[{}]: removing ret_tx from {}", handler.id, db_id);
                match handler.dispatcher.tasks_tx[db_id].try_send(TaskParam::Remove(handler.id)) {
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        error!(
                            "background task's closed while recycling handler[{}]",
                            handler.id
                        );
                        return false; // no longer need this handler
                    }
                    Ok(_) => -1, // recycled
                    _ => 0,      // Not yet recycled, just keep it.
                }
            } else {
                0
            };
        }
    }
    debug!("[{}]: send to recycle channel", handler.id);
    let id = handler.id;
    handler.connection.close_connection().await;
    let x = sender.try_send(handler).is_ok();
    debug!("[{}]: {}", id, if x { "recycled" } else { "discarded" });
    return x;
}

impl Listener {
    #[instrument(skip(self))]
    async fn run(&self) -> Result<()> {
        let mut float_num: u64 = 0;
        let mut conn_id: u64 = 0;
        let (recycle_tx, mut recycle_rx) = mpsc::channel(1000);
        let channel_counter = Arc::new(AtomicU32::new(0));
        info!("Server Started");
        loop {
            conn_id += 1;
            // select! {}
            let (stream, _) = self.listener.accept().await?;
            debug!("<{}>: stream accepted", conn_id);

            let conn = Connection::new(stream, conn_id);
            debug!(
                "<{}>: recycle_rx: {:?}, channel_counter: {:?}",
                conn_id, recycle_rx, channel_counter
            );
            let mut handler = if channel_counter
                .fetch_update(SeqCst, Relaxed, |v| if v > 0 { Some(v - 1) } else { None })
                .is_ok()
            {
                let mut ret: Handler = recycle_rx.recv().await.unwrap();
                debug!("<{}>: recv succeed, handler[{}]", conn_id, ret.id);
                ret.connection = conn;
                ret
            } else {
                float_num += 1;
                debug!("<{}>: new handler[{}]", conn_id, float_num);
                let (ret_tx, ret_rx) = mpsc::channel(1);
                Handler {
                    connection: conn,
                    dispatcher: self.dispatcher.clone(),
                    shutdown_begin: Shutdown::new(self.shutdown_begin.subscribe()),
                    sent: vec![-1; self.num_threads],
                    ret_rx,
                    ret_tx,
                    shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                    id: float_num,
                    age: 0,
                }
            };

            let recycle_tx_copy = recycle_tx.clone();
            let channel_counter_copy = channel_counter.clone();
            tokio::spawn(async move {
                match handler.run().await {
                    Err(e) => {
                        let mut use_error = true;
                        if let Some(io_error) = e.downcast_ref::<std::io::Error>() {
                            if io_error.raw_os_error() == Some(10054) {
                                use_error = false;
                            }
                        }
                        if use_error {
                            error!("error occured while handling: {}", e);
                        } else {
                            debug!("error occured while handling: {}", e);
                        }
                    }
                    _ => (),
                }

                let ret = recycle_handler(handler, recycle_tx_copy).await;
                if ret {
                    channel_counter_copy.fetch_add(1, Release);
                }
            });
        }
    }
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    dispatcher: Arc<Dispatcher>,
    shutdown_begin: Shutdown,
    shutdown_complete_tx: mpsc::Sender<()>,
    sent: Vec<i32>,
    ret_tx: mpsc::Sender<Frame>,
    ret_rx: mpsc::Receiver<Frame>,
    id: u64,
    age: u32,
}

impl Handler {
    #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        while !self.shutdown_begin.is_shutdown() {
            let opt_frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame() => res?
            };

            debug!(
                "[{}]<{}>frame received: {:?}",
                self.id, self.connection.id, opt_frame
            );
            let frame = match opt_frame {
                Some(f) => f,
                None => {
                    return Ok(());
                }
            };

            let command = Command::new(&frame);
            let ret_frame = match command {
                Ok(cmd @ Command::Debug(_)) => {
                    let mut ret = Vec::with_capacity(self.dispatcher.num_threads);
                    for db_id in 0..self.dispatcher.num_threads {
                        let option_tx = if self.sent[db_id] == -1 {
                            Some(self.ret_tx.clone())
                        } else {
                            None
                        };
                        let cmd_copy = cmd.clone();
                        self.dispatcher.tasks_tx[db_id]
                            .send(TaskParam::Task((cmd_copy, self.id, option_tx)))
                            .await?;

                        self.sent[db_id] = std::cmp::max(self.sent[db_id] + 1, 1);

                        ret.push(self.ret_rx.recv().await.unwrap());
                    }
                    Frame::Arrays(FrameArrays::new(ret))
                }
                Ok(mut cmd) => {
                    debug!(
                        "[{}]<{}>parsed command: {:?}",
                        self.id, self.connection.id, cmd
                    );
                    let nounce = self.dispatcher.counter.fetch_add(1, Relaxed);

                    cmd.set_nounce(nounce);
                    let db_id = self.dispatcher.determine_database(cmd.get_key());

                    let option_tx = if self.sent[db_id] == -1 {
                        Some(self.ret_tx.clone())
                    } else {
                        None
                    };

                    self.dispatcher.tasks_tx[db_id]
                        .send(TaskParam::Task((cmd, self.id, option_tx)))
                        .await?;

                    self.sent[db_id] = std::cmp::max(self.sent[db_id] + 1, 1);

                    self.ret_rx.recv().await.unwrap()
                }
                Err(e) => match e.downcast_ref::<CommandError>() {
                    Some(e) => Frame::Errors(format!("{}", e).into()),
                    None => {
                        return Err(e);
                    }
                },
            };
            debug!(
                "[{}]<{}>ret_frame: {:?}",
                self.id, self.connection.id, ret_frame
            );
            self.connection.write_frame(&ret_frame).await?;
        }
        Ok(())
    }
}

#[instrument(skip(listener, shutdown_signal))]
pub async fn run(listener: TcpListener, shutdown_signal: impl Future, num_threads: usize) {
    info!("Service Starting");
    let (shutdown_begin_tx, _) = broadcast::channel(1);

    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let server = Listener {
        listener,
        dispatcher: Arc::new(Dispatcher::new(
            &shutdown_begin_tx,
            &shutdown_complete_tx,
            num_threads,
        )),
        shutdown_begin: shutdown_begin_tx,
        shutdown_complete_rx,
        shutdown_complete_tx,
        num_threads,
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
            info!("Ctrl+C");
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
    info!("Shutdown Complete");
}
