use crate::{cmd::*, protocol::Frame};
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

#[derive(Debug)]
pub enum TaskParam {
    OneshotTask((OneshotCommand, oneshot::Sender<Frame>)),
}

#[derive(Debug)]
pub struct Entry {
    pub data: Frame,
    pub expiration: Option<Instant>,
    pub nounce: u64,
}

#[derive(Debug)]
pub struct DB {
    pub database: FxHashMap<Bytes, Entry>,
    pub expiration: BTreeMap<(Instant, u64), Bytes>,
    pub when: Option<Instant>,
    pub id: usize,
    pub counter: u64,
}

impl DB {
    fn new(id: usize) -> Self {
        Self {
            database: FxHashMap::default(),
            expiration: BTreeMap::new(),
            when: None,
            id,
            counter: 0,
        }
    }

    pub fn get(&self, key: &Bytes) -> Frame {
        self.database
            .get(key)
            .filter(|v| v.expiration.is_none() || v.expiration.unwrap() > Instant::now())
            .map_or_else(|| Frame::NullString, |v| v.data.clone())
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
        }
    }

    pub fn set(&mut self, key: Bytes, data: Frame, nounce: u64, expiration: Option<Instant>) {
        self.database.insert(
            key,
            Entry {
                data,
                expiration,
                nounce,
            },
        );
    }
}

pub async fn database_manager(
    mut tasks_rx: mpsc::UnboundedReceiver<TaskParam>,
    mut shutdown: broadcast::Receiver<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    taskid: usize,
) {
    let mut when: Option<Instant> = None;
    let mut db = DB::new(taskid);
    info!("[{}] starting backgroud task", taskid);

    loop {
        let now = Instant::now();

        select! {
            _ = shutdown.recv() => {
                info!("[{}] shutting down backgroud task", taskid);
                return;
            }
            res = tasks_rx.recv() => {
                if res.is_none() {
                    continue;
                }
                let (cmd, ret_tx) = match res.unwrap() {
                    TaskParam::OneshotTask(v) => v,
                };
                trace!("[{}] scheduling: {:?}, now: {:?}", taskid, &cmd, &now);
                let _ = ret_tx.send(cmd.exec(&mut db));
            }
            _ = tokio::time::sleep_until(
                when.map(|v| v.max(now + Duration::new(10, 0)))
                    .unwrap_or(now + Duration::new(30, 0)),
            ) => {
                debug!("[{}] task waked up, expirations: {:?}", taskid, db.expiration);
                when = None;
                if db.expiration.len() > 0 {
                    let now = Instant::now();
                    while let Some((&(expire_next, id), _)) = db.expiration.iter().next() {
                        if expire_next <= now {
                            let key = db.expiration.remove(&(expire_next, id)).unwrap();
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
