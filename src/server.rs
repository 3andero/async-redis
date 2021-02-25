use std::{
    collections::hash_map::DefaultHasher,
    future::Future,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU32, AtomicU64, Ordering::*},
    sync::Arc,
};

use anyhow::Error;

use bytes::Bytes;
use tokio::{net::TcpListener, spawn, sync::*};
use tracing::*;

use crate::{
    cmd::traverse_command::*, cmd::*, connection::*, db::*, protocol::Frame, shutdown::Shutdown,
    Result,
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

// #[instrument(skip(handler, sender))]
async fn recycle_handler(mut handler: Handler, sender: mpsc::Sender<Handler>) -> bool {
    debug!("[{}]: entered", handler.id);
    debug!("[{}]: send to recycle channel", handler.id);
    let id = handler.id;
    handler.connection.close_connection().await;
    let x = sender.try_send(handler).is_ok();
    debug!("[{}]: {}", id, if x { "recycled" } else { "discarded" });
    return x;
}

impl Listener {
    // #[instrument(skip(self))]
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
                ret.connection.refresh(stream, conn_id);
                ret
            } else {
                let conn = Connection::new(stream, conn_id);
                float_num += 1;
                debug!("<{}>: new handler[{}]", conn_id, float_num);
                Handler {
                    connection: conn,
                    dispatcher: self.dispatcher.clone(),
                    shutdown_begin: Shutdown::new(self.shutdown_begin_tx.subscribe()),
                    shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                    id: conn_id,
                    thread_num: self.dispatcher.num_threads,
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

#[macro_use]
macro_rules! traverse {
    ($(use $($dependency:ident),*;)?
     for ($db_id:ident, $atomic_cmd:ident) in $cmd:ident do $do:block then send as $tasktype:ident by $self:ident) => {{
        $cmd.dispatch($self.thread_num, |key: &[u8]| {
            $self.dispatcher.determine_database(key)
        });
        let expected_amount_ret = $cmd.len();

        let mut ret: Vec<Frame> = Vec::with_capacity(expected_amount_ret);
        unsafe {
            ret.set_len(expected_amount_ret);
        }

        let mut result_collector = $cmd.get_result_collector();

        for _ in 0..$self.thread_num {
            let ($db_id, $atomic_cmd) = $cmd.next_command();
            if $atomic_cmd.is_none() {
                continue;
            }
            let (ret_tx, ret_rx) = oneshot::channel();
            let cmd_copy = $do;
            $self.dispatcher.tasks_tx[$db_id].send(TaskParam::$tasktype((cmd_copy, ret_tx)))?;

            result_collector.merge(&mut ret, ret_rx).await?;
        }

        if ret.len() == 1 {
            ret.pop().unwrap()
        } else {
            Frame::Arrays(ret)
        }
    }};
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

#[derive(Debug)]
enum CommandConvert {
    Oneshot,
    PubSub,
}

impl Handler {
    async fn traverse_exec<T>(&mut self, cmd: &mut T, into_task: CommandConvert) -> Result<Frame>
    where
        T: DispatchToMultipleDB + std::fmt::Debug,
    {
        trace!(
            "[{}]<{}>enter traverse send: {:?}, {:?}",
            self.id,
            self.connection.id,
            into_task,
            cmd,
        );
        let expected_amount_ret = cmd.len();

        let mut ret: Vec<Frame> = Vec::with_capacity(expected_amount_ret);
        unsafe {
            ret.set_len(expected_amount_ret);
        }

        let mut result_collector = cmd.get_result_collector();

        for _ in 0..self.thread_num {
            let (db_id, atomic_cmd) = cmd.next_command();
            if atomic_cmd.is_none() {
                continue;
            }
            let (ret_tx, ret_rx) = oneshot::channel();
            trace!(
                "[{}]<{}>send to db: {}: {:?}",
                self.id,
                self.connection.id,
                db_id,
                atomic_cmd
            );
            self.dispatcher.tasks_tx[db_id].send(match into_task {
                CommandConvert::Oneshot => {
                    TaskParam::OneshotTask((atomic_cmd.unwrap_oneshot(), ret_tx))
                }
                CommandConvert::PubSub => {
                    TaskParam::PubSubTask((atomic_cmd.unwrap_pubsub(), ret_tx))
                }
            })?;

            result_collector.merge(&mut ret, ret_rx).await?;
            trace!(
                "[{}]<{}>merge db {} result",
                self.id,
                self.connection.id,
                db_id
            );
        }

        if ret.len() == 1 {
            Ok(ret.pop().unwrap())
        } else {
            Ok(Frame::Arrays(ret))
        }
    }

    // #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        while !self.shutdown_begin.is_shutdown() {
            let opt_frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame() => res?
            };

            trace!(
                "[{}]<{}>frame received: {:?}",
                self.id,
                self.connection.id,
                opt_frame
            );
            let frame = match opt_frame {
                Some(f) => f,
                None => {
                    return Ok(());
                }
            };

            let command = Command::new(frame);
            let ret_frame = match command {
                Ok(Command::Traverse(mut cmd)) => {
                    // traverse!(
                    //     for (db_id, atomic_cmd) in cmd do {
                    //         atomic_cmd.unwrap_oneshot()
                    //     } then send as OneshotTask by self
                    // )
                    cmd.dispatch(self.thread_num, |key: &[u8]| {
                        self.dispatcher.determine_database(key)
                    });
                    self.traverse_exec(&mut cmd, CommandConvert::Oneshot)
                        .await?
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

                    self.dispatcher.tasks_tx[db_id].send(TaskParam::OneshotTask((cmd, ret_tx)))?;

                    ret_rx.await.map_err(|e| Error::new(e))?
                }
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
                    if !cmd.need_subscribe() {
                        self.traverse_exec(&mut cmd, CommandConvert::PubSub).await?
                    } else {
                        self.handle_hold_on_cmd(&mut cmd).await?;
                        continue;
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

    async fn handle_hold_on_cmd(&mut self, cmd: &mut HoldOnCommand) -> Result<()> {
        trace!(
            "[{}]<{}>enter handle_hold_on_cmd",
            self.id,
            self.connection.id
        );
        let (ret_tx, mut ret_rx) = mpsc::channel(BUFSIZE);
        let mut sub_state = vec![false; self.thread_num];
        cmd.set_subscription(&mut sub_state, &ret_tx, self.id);
        let ret_frame = self.traverse_exec(cmd, CommandConvert::PubSub).await?;
        self.connection.write_frame(&ret_frame).await?;

        while !self.shutdown_begin.is_shutdown() {
            let frame = tokio::select! {
                _ = self.shutdown_begin.recv() => {
                    return Ok(());
                }
                res = self.connection.read_frame() => {
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
                            Frame::_DetachSubscribeMode => {
                                return Ok(());
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
                        cmd.set_subscription(&mut sub_state, &ret_tx, self.id);
                    }
                    self.traverse_exec(&mut cmd, CommandConvert::PubSub).await?
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
