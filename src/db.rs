use crate::{cmd::*, protocol::Frame};
use bytes::*;
use std::{
    collections::{BTreeMap, HashMap},
};
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot},
    time::{Duration, Instant},
};
use tracing::debug;

pub type TaskParam = (Command, oneshot::Sender<Frame>);

#[derive(Debug)]
pub struct Entry {
    data: Bytes,
    expiration: Option<Instant>,
    nounce: u64,
}

#[derive(Debug)]
pub struct DB {
    pub database: HashMap<Bytes, Entry>,
    pub expiration: BTreeMap<(Instant, u64), Bytes>,
    pub when: Option<Instant>,
    pub id: usize,
}

impl DB {
    fn new(id: usize) -> Self {
        Self {
            database: HashMap::new(),
            expiration: BTreeMap::new(),
            when: None,
            id,
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.database
            .get(key)
            .filter(|v| v.expiration.is_none() || v.expiration.unwrap() > Instant::now())
            .map(|v| v.data.clone())
    }

    pub fn set(&mut self, key: Bytes, data: Bytes, nounce: u64, expiration: Option<Instant>) {
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
    mut tasks_rx: mpsc::Receiver<TaskParam>,
    mut shutdown: broadcast::Receiver<()>,
    taskid: usize,
) {
    let mut when: Option<Instant> = None;
    let mut db = DB::new(taskid);
    debug!("[{}] starting backgroud task", taskid);

    loop {
        let now = Instant::now();

        select! {
            _ = shutdown.recv() => {
                debug!("[{}] shutting down backgroud task", taskid);
                return;
            }
            res = tasks_rx.recv() => {
                if res.is_none() {
                    continue;
                }
                let (cmd, ret_tx) = res.unwrap();
                debug!("[{}] scheduling: {:?}, now: {:?}", taskid, &cmd, &now);
                let _ = ret_tx.send(cmd.exec(&mut db));
            }
            _ = tokio::time::sleep_until(
                when.map(|v| v.max(now + Duration::new(10, 0)))
                    .unwrap_or(now + Duration::new(60, 0)),
            ) => {
                debug!("[{}] task waked up, expirations: {:?}", taskid, db.expiration);
                when = None;
                if db.expiration.len() > 0 {
                    let now = Instant::now();
                    while let Some((&(expire_next, id), _)) = db.expiration.iter().next() {
                        if expire_next <= now {
                            let key = db.expiration.remove(&(expire_next, id)).unwrap();
                            db.database.remove(&key);
                            debug!("[{}] collecting expired key({:?}): {:?}", taskid, &now, &key);
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
