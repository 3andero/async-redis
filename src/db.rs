use crate::{cmd::*, protocol::Frame};
use bytes::*;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot, Notify},
    time::{Duration, Instant},
};
use tracing::debug;

pub type TaskParam = (Command, oneshot::Sender<Frame>, Arc<Notify>);

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
        self.database.get(key).map(|v| v.data.clone())
    }

    pub fn set(&self, key: Bytes, data: Bytes, nounce: u64, expiration: Option<Instant>) {
        // let nounce = self
        //     .shared
        //     .counter
        //     .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // let db = &self.shared.state.database;
        // let partition_id = db.determine_map(&key);

        self.database.insert(
            key,
            Entry {
                data,
                expiration,
                nounce,
            },
        );

        // if let Some(when) = expiration {
        //     let task_tx = self.shared.tasks_tx[partition_id % self.shared.tasks_tx.len()].clone();
        //     let params = (when, nounce, key, partition_id);
        //     spawn(async move {
        //         match task_tx.send(params).await {
        //             Err(e) => error!("{}", e),
        //             _ => (),
        //         }
        //     });
        // }
    }
}

pub async fn database_manager(
    mut tasks_rx: mpsc::Receiver<TaskParam>,
    mut shutdown: broadcast::Receiver<()>,
    taskid: usize,
) {
    let mut when: Option<Instant> = None;
    let db = DB::new(taskid);
    debug!("[{}] starting backgroud task", taskid);

    loop {
        let now = Instant::now();

        let res = tasks_rx.recv().await;
        {
            if res.is_none() {
                continue;
            }
            let (cmd, ret_tx, notify) = res.unwrap();
            // let now = Instant::now();
            debug!("[{}] scheduling: {:?}, now: {:?}", taskid, &cmd, &now);
            ret_tx.send(cmd.exec(&mut db));
            notify.notify_one();
        }

        select! {
            _ = shutdown.recv() => {
                debug!("[{}] shutting down backgroud task", taskid);
                return;
            }
            _ = tokio::time::sleep_until(
                when.map(|v| v.max(now + Duration::new(10, 0)))
                    .unwrap_or(now + Duration::new(20, 0)),
            ) => {
                debug!("[{}] task waked up, expirations: {:?}", taskid, expirations);
                when = None;
                if expirations.len() > 0 {
                    let now = Instant::now();
                    let mut key_bucket = vec![vec![]; shared.num_partition];
                    while let Some((&(expire_next, id), _)) = expirations.iter().next() {
                        if expire_next <= now {
                            let key = expirations.remove(&(expire_next, id)).unwrap();
                            debug!("[{}] collecting expired key({:?}): {:?}", taskid, &now, &key);
                            key_bucket[key.1].push(key);
                        } else {
                            when = Some(expire_next);
                            break;
                        }
                    }

                    for (pid, keys) in key_bucket.iter().enumerate().filter(|v| v.1.len() > 0) {
                            debug!("[{}] removing expired key: {:?} from {}", taskid, &keys, &pid);
                            let _ = shared
                            .state
                            .database
                            .remove_list(keys.iter().map(|x| &x.0), pid);
                    }
                }
            }
        }
    }
}
