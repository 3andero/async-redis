use std::{
    collections::hash_map::DefaultHasher,
    future::Future,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, AtomicUsize},
    sync::Arc,
};

use crate::protocol::*;
use reusable_buf::ReusableBuf;

use anyhow::Error;

use bytes::Bytes;
use tokio::{net::TcpListener, spawn, sync::*};
use tracing::*;

use crate::{
    cmd::traverse_command::*, cmd::unsubscribe::UnsubDispatcher, cmd::*, connection::*, db::*,
    protocol::Frame, shutdown::Shutdown, Result,
};

const BUFSIZE: usize = 50;

#[allow(dead_code)]
fn calculate_hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}

#[derive(Debug)]
pub struct Dispatcher {
    num_threads: usize,
    counter: AtomicU64,
    tasks_tx: Vec<mpsc::UnboundedSender<TaskParam>>,
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
            let (tx, rx) = mpsc::unbounded_channel();
            tasks_tx.push(tx);
            tasks_rx.push(rx);
        }

        for (id, rx) in tasks_rx.drain(..).enumerate() {
            let notify_rx = notify_tx.subscribe();
            let notify_tx_clone = notify_tx.clone();
            let shutdown_complete_tx_copy = shutdown_complete_tx.clone();
            spawn(async move {
                database_manager(
                    rx,
                    notify_tx_clone,
                    notify_rx,
                    shutdown_complete_tx_copy,
                    id,
                )
                .await;
            });
        }
        Self {
            num_threads,
            counter: AtomicU64::new(0),
            tasks_tx,
        }
    }

    // pub fn determine_database(&self, key: &Bytes) -> usize {
    //     // Leave the high 7 bits for the HashBrown SIMD tag.
    //     // (calculate_hash(key) << 7) >> self._shift_param
    //     calculate_hash(key) % self.num_threads
    // }

    pub fn determine_database(&self, key: &[u8]) -> usize {
        // Leave the high 7 bits for the HashBrown SIMD tag.
        let mut hash = 0;
        for b in key {
            hash = (hash + *b as usize) % self.num_threads;
        }
        hash
    }
}
pub struct Listener {
    listener: TcpListener,
    dispatcher: Arc<Dispatcher>,

    shutdown_begin_tx: broadcast::Sender<()>,

    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    // #[instrument(skip(self))]
    async fn run(&self) -> Result<()> {
        let mut float_num: u64 = 0;
        let mut conn_id: u64 = 0;
        info!("Server Started");
        loop {
            conn_id += 1;
            let (stream, _) = self.listener.accept().await?;
            debug!("<{}>: stream accepted", conn_id);

            let conn = Connection::new(stream, conn_id);
            float_num += 1;
            debug!("<{}>: new handler[{}]", conn_id, float_num);
            let mut handler = Handler {
                connection: conn,
                dispatcher: self.dispatcher.clone(),
                shutdown_begin: Shutdown::new(self.shutdown_begin_tx.subscribe()),
                shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                id: conn_id,
                thread_num: self.dispatcher.num_threads,
            };

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
    id: u64,
    thread_num: usize,
}

impl Handler {
    async fn traverse_exec<T>(&self, cmd: &mut T) -> Result<Frame>
    where
        T: DispatchToMultipleDB + std::fmt::Debug,
    {
        trace!(
            "[{}]<{}>enter traverse send: {:?}",
            self.id,
            self.connection.id,
            cmd,
        );

        let mut result_collector = cmd.get_result_collector();

        while let Some((db_id, atomic_cmd)) = cmd.next_command() {
            let (ret_tx, ret_rx) = oneshot::channel();
            trace!(
                "[{}]<{}>send to db: {}: {:?}",
                self.id,
                self.connection.id,
                db_id,
                atomic_cmd
            );
            self.dispatcher.tasks_tx[db_id].send((atomic_cmd, ret_tx))?;

            result_collector.merge(ret_rx).await?;
            trace!(
                "[{}]<{}>merge db {} result",
                self.id,
                self.connection.id,
                db_id
            );
        }
        let mut ret = result_collector.get_ret();
        if ret.len() == 1 {
            Ok(ret.pop().unwrap())
        } else {
            Ok(Frame::Arrays(ret))
        }
    }

    // #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        let mut buf = ReusableBuf::new();
        while !self.shutdown_begin.is_shutdown() {
            let opt_frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame(&mut buf) => res?
            };

            trace!(
                "[{}]<{}>frame received: {:?}",
                self.id,
                self.connection.id,
                opt_frame
            );
            let frame = match opt_frame {
                // This is a really really bad workaround.
                // I'm not willing to support Redis inline command,
                // but want to use redis-benchmark to test my `ping` implementation.
                // This test, however, requires inline command implemented.
                // In order to make this work, I reject all inline command and return `nil` immediately
                // This branch `f @ Frame::NullString` does exactly that.
                Some(f @ Frame::NullString) => {
                    self.connection.write_frame(&f).await?;
                    continue;
                }
                Some(f) => f,
                None => {
                    return Ok(());
                }
            };

            let command = Command::new(frame);
            let ret_frame = match command {
                Ok(Command::Zeroshot(cmd)) => match cmd {
                    ZeroshotCommand::Ping(pong) => {
                        if pong.is_none() {
                            Frame::Pong
                        } else {
                            Frame::BulkStrings(pong.unwrap())
                        }
                    }
                },
                Ok(Command::Traverse(mut cmd)) => {
                    cmd.dispatch(self.thread_num, |key: &[u8]| {
                        self.dispatcher.determine_database(key)
                    });
                    self.traverse_exec(&mut cmd).await?
                }
                Ok(Command::Oneshot(cmd)) => {
                    trace!(
                        "[{}]<{}>parsed command: {:?}",
                        self.id,
                        self.connection.id,
                        cmd
                    );
                    let (ret_tx, ret_rx) = oneshot::channel();
                    let db_id = self.dispatcher.determine_database(cmd.get_key());

                    self.dispatcher.tasks_tx[db_id].send((cmd.into(), ret_tx))?;

                    ret_rx.await.map_err(|e| Error::new(e))?
                }
                Err(e) => match e.downcast_ref::<CommandError>() {
                    Some(e) => Frame::Errors(format!("{}", e).into()),
                    None => {
                        return Err(e);
                    }
                },
                Ok(Command::HoldOn(mut cmd)) => {
                    if cmd.is_unsubscribe() {
                        Frame::Errors(Bytes::from_static(b"nothing to unsubscribe"))
                    } else {
                        cmd.dispatch(self.thread_num, |key: &[u8]| {
                            self.dispatcher.determine_database(key)
                        });
                        if !cmd.need_subscribe() {
                            self.traverse_exec(&mut cmd).await?
                        } else {
                            let mut sub_state = vec![false; self.thread_num];
                            match self
                                .handle_hold_on_cmd(&mut cmd, &mut sub_state, &mut buf)
                                .await
                            {
                                Ok(_) => self.unsubscribe_all(sub_state).await,
                                Err(e) => {
                                    self.unsubscribe_all(sub_state).await;
                                    return Err(e);
                                }
                            }
                            continue;
                        }
                    }
                }
            };
            trace!(
                "[{}]<{}>ret_frame: {:?}",
                self.id,
                self.connection.id,
                ret_frame
            );
            self.connection.write_frame(&ret_frame).await?;
        }
        Ok(())
    }

    async fn unsubscribe_all(&self, sub_state: Vec<bool>) {
        let mut unsub_all = UnsubDispatcher::unsubscribe_all(self.id, sub_state, self.thread_num);
        let _ = self.traverse_exec(&mut unsub_all).await;
    }

    async fn handle_hold_on_cmd(
        &mut self,
        cmd: &mut HoldOnCommand,
        sub_state: &mut Vec<bool>,
        buf: &mut ReusableBuf,
    ) -> Result<()> {
        trace!(
            "[{}]<{}>enter handle_hold_on_cmd",
            self.id,
            self.connection.id
        );
        let (ret_tx, mut ret_rx) = mpsc::channel(BUFSIZE);
        let total_chn_amount = Arc::new(AtomicUsize::new(0));

        cmd.set_subscription(sub_state, &ret_tx, self.id, total_chn_amount.clone());
        let ret_frame = self.traverse_exec(cmd).await?;
        self.connection.write_frame(&ret_frame).await?;

        while !self.shutdown_begin.is_shutdown() {
            let frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame(buf) => {
                    match res? {
                        Some(f) => f,
                        None => {
                            return Ok(());
                        }
                    }
                }
                maybe_update = ret_rx.recv() => {
                    if let Some(update) = maybe_update {
                        match update {
                            Frame::_DetachSubscribeMode(db_id) => {
                                sub_state[db_id] = false;
                                if !sub_state.contains(&true) {
                                    return Ok(());
                                }
                                continue;
                            }
                            v => {
                                self.connection.write_frame(&v).await?;
                                continue;
                            }
                        }
                    } else {
                        return Ok(());
                    }
                }
            };

            trace!(
                "[{}]<{}>frame received: {:?}",
                self.id,
                self.connection.id,
                frame
            );
            let command = Command::new(frame);
            let ret_frame = match command {
                Err(e) => match e.downcast_ref::<CommandError>() {
                    Some(e) => Frame::Errors(format!("{}", e).into()),
                    None => {
                        return Err(e);
                    }
                },
                Ok(Command::HoldOn(mut cmd)) => {
                    cmd.dispatch(self.thread_num, |key: &[u8]| {
                        self.dispatcher.determine_database(key)
                    });
                    if cmd.need_subscribe() {
                        cmd.set_subscription(sub_state, &ret_tx, self.id, total_chn_amount.clone());
                    }
                    self.traverse_exec(&mut cmd).await?
                }
                _ => Frame::Errors(Bytes::from_static(
                    b"command not allowed when subscribing to channels",
                )),
            };
            self.connection.write_frame(&ret_frame).await?;
        }

        Ok(())
    }
}

// #[instrument(skip(listener, shutdown_signal))]
pub async fn run(listener: TcpListener, shutdown_signal: impl Future, num_threads: usize) {
    info!("Service Starting");
    let (shutdown_begin_tx, mut shutdown_begin_rx) = broadcast::channel(1);

    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let server = Listener {
        listener,
        dispatcher: Arc::new(Dispatcher::new(
            &shutdown_begin_tx,
            &shutdown_complete_tx,
            num_threads,
        )),
        shutdown_begin_tx,
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
            info!("Ctrl+C");
        }
        _ = shutdown_begin_rx.recv() => {
            drop(shutdown_begin_rx);
            info!("shutdown by command");
        }
    }

    let Listener {
        shutdown_begin_tx,
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        ..
    } = server;

    let _ = shutdown_begin_tx.send(());

    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
    info!("Shutdown Complete");
}
