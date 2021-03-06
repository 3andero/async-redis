use crate::{cmd::*, protocol::Frame, utils::VecMap};
use bytes::*;
use diagnose::DxCommand;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rustc_hash::FxHashMap;
use std::{cmp::min, collections::BTreeMap};
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot},
    time::{Duration, Instant},
};
use tracing::{debug, info, trace};

pub type TaskParam = (AtomicCMD, oneshot::Sender<Frame>);

#[derive(Debug)]
pub struct Entry {
    pub data: Frame,
    pub expiration: Option<Instant>,
    pub nounce: u64,
}

#[derive(Debug)]
pub struct DB {
    pub database: FxHashMap<Bytes, Entry>,
    pub expiration: ExpirationSubModule,
    pub subscribe: SubscriptionSubModule,
    pub id: usize,
    pub counter: u64,
    pub shutdown_tx: broadcast::Sender<()>,
}

#[derive(Debug)]
pub struct ExpirationSubModule {
    expiration: BTreeMap<(Instant, u64), Bytes>,
    when: Option<Instant>,
}

impl ExpirationSubModule {
    pub fn update(&mut self, expiration: Option<Instant>, nounce: u64, key: &Bytes) {
        if expiration.is_none() {
            return;
        }
        let expiration = expiration.unwrap();
        self.expiration.insert((expiration, nounce), key.clone());
        self.when = match self.when {
            None => Some(expiration),
            Some(v) => Some(v.min(expiration)),
        };
    }
    pub fn remove(&mut self, key: &(Instant, u64)) {
        self.expiration.remove(key);
    }
}

#[derive(Debug, Default)]
pub struct SubscriptionSubModule {
    pub chn_id_gen: usize,
    pub channels: FxHashMap<Bytes, usize>,
    pub channel_info: FxHashMap<usize, Bytes>,
    pub subscriber: FxHashMap<usize, VecMap<u64>>,
    pub subscriber_info: FxHashMap<u64, (mpsc::Sender<Frame>, VecMap<usize>)>,
}

impl SubscriptionSubModule {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}

impl DB {
    fn new(id: usize, shutdown_tx: broadcast::Sender<()>) -> Self {
        Self {
            database: FxHashMap::default(),
            expiration: ExpirationSubModule {
                expiration: BTreeMap::new(),
                when: None,
            },
            subscribe: SubscriptionSubModule::new(),
            id,
            counter: 0,
            shutdown_tx,
        }
    }

    pub fn diagnose(&self, key: &DxCommand) -> Frame {
        match key {
            DxCommand::KeyNum => {
                return Bytes::from(format!("[{}]{}", self.id, self.database.len())).into();
            }
            DxCommand::TotalKeyLen => {
                return Bytes::from(format!(
                    "[{}]{}",
                    self.id,
                    self.database.keys().fold(0, |res, b| res + b.len())
                ))
                .into();
            }
            DxCommand::TotalValLen => {
                return Bytes::from(format!(
                    "[{}]{}",
                    self.id,
                    self.database.values().fold(0, |res, b| res + b.data.len())
                ))
                .into();
            }
            DxCommand::RandomKeys => {
                const TAKE: usize = 5;
                let mut idxs: Vec<usize> = (0..self.database.len()).collect();
                idxs.shuffle(&mut thread_rng());
                let mut rand_idx = idxs[..min(TAKE, idxs.len())].to_vec();
                rand_idx.sort();
                let mut res = Vec::with_capacity(TAKE + 1);
                res.push(Bytes::copy_from_slice(format!("db {}", self.id).as_bytes()).into());
                for (idx, key) in self.database.keys().enumerate() {
                    if idx == rand_idx[res.len() - 1] {
                        res.push(key.clone().into());
                    }
                    if res.len() == TAKE {
                        break;
                    }
                }
                return Frame::Arrays(res);
            }
            DxCommand::Shutdown => {
                let _ = self.shutdown_tx.send(());
                return Frame::Ok;
            }
        }
    }
}

macro_rules! exec {
    ($cmd:ident, $db:expr) => {{
        use AtomicCMD::*;
        match $cmd {
            Get(c) => c.exec($db),
            Set(c) => c.exec($db),
            MGet(c) => c.exec($db),
            MSet(c) => c.exec($db),
            Dx(c) => c.exec($db),
            Incr(c) => c.exec($db),
            Subscribe(c) => c.exec($db),
            Publish(c) => c.exec($db).await,
            Unsubscribe(c) => c.exec($db).await,
        }
    }};
}

pub async fn database_manager(
    mut tasks_rx: mpsc::UnboundedReceiver<TaskParam>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    taskid: usize,
) {
    let mut when: Option<Instant> = None;
    let mut db = DB::new(taskid, shutdown_tx);
    info!("[{}] starting backgroud task", taskid);

    loop {
        let now = Instant::now();

        select! {
            _ = shutdown_rx.recv() => {
                info!("[{}] shutting down backgroud task", taskid);
                return;
            }
            res = tasks_rx.recv() => {
                if res.is_none() {
                    continue;
                }
                let (cmd, ret_tx) = res.unwrap();
                trace!("[{}] scheduling: {:?}, now: {:?}", taskid, &cmd, &now);
                trace!("db before: {:?}", db);
                let _ = ret_tx.send(exec!(cmd, &mut db));
                trace!("db after: {:?}", db);
            }
            _ = tokio::time::sleep_until(
                when.map(|v| v.max(now + Duration::new(1000, 0)))
                    .unwrap_or(now + Duration::new(3000, 0)),
            ) => {
                debug!("[{}] task waked up, expirations: {:?}", taskid, db.expiration);
                when = None;
                if db.expiration.expiration.len() > 0 {
                    let now = Instant::now();
                    while let Some((&(expire_next, id), _)) = db.expiration.expiration.iter().next() {
                        if expire_next <= now {
                            let key = db.expiration.expiration.remove(&(expire_next, id)).unwrap();
                            db.database.remove(&key);
                            trace!("[{}] collecting expired key({:?}): {:?}", taskid, &now, &key);
                        } else {
                            when = Some(expire_next);
                            break;
                        }
                    }
                }
            }
        }
    }
}
