use bytes::*;
use std::{collections::BTreeMap, sync::atomic::AtomicU64, sync::Arc};
use tokio::{
    select, spawn,
    sync::{broadcast, mpsc},
    time::{Duration, Instant},
};
use tracing::error;

use dashmap::DashMap;

const BUFFERSIZE: usize = 10;

#[derive(Debug)]
pub struct Entry {
    data: Bytes,
    expiration: Option<Instant>,
    nounce: u64,
}

#[derive(Debug)]
pub struct State {
    database: DashMap<String, Entry>,
}

#[derive(Debug)]
pub struct Shared {
    num_partition: usize,
    state: State,
    counter: AtomicU64,
    tasks_tx: Vec<mpsc::Sender<(Instant, u64, String, usize)>>,
}

impl Shared {
    fn new() -> Self {
        let num_partition = dashmap::shard_amount();
        let ret = Self {
            num_partition,
            state: State {
                database: DashMap::new(),
            },
            counter: AtomicU64::new(0),
            tasks_tx: Vec::with_capacity(num_partition),
        };
        ret
    }
}
#[derive(Clone, Debug)]
pub struct DB {
    shared: Arc<Shared>,
    notify_background_task: Arc<broadcast::Sender<()>>,
}

impl DB {
    pub fn new() -> Self {
        let mut shared = Shared::new();
        let mut tasks_rx = Vec::with_capacity(shared.num_partition);

        for _ in 0..((shared.num_partition as f64).sqrt() as usize) {
            let (tx, rx) = mpsc::channel(BUFFERSIZE);
            shared.tasks_tx.push(tx);
            tasks_rx.push(rx);
        }

        let shared = Arc::new(shared);
        let (notify_tx, _) = broadcast::channel(1);
        for rx in tasks_rx {
            let shared_copy = shared.clone();
            let notify_copy = notify_tx.subscribe();
            spawn(async move {
                purge_expired_keys(shared_copy, rx, notify_copy).await;
            });
        }
        Self {
            shared,
            notify_background_task: Arc::new(notify_tx),
        }
    }

    pub fn get(&self, key: &String) -> Option<Bytes> {
        let db = &self.shared.state.database;
        db.get(key).map(|v| v.data.clone())
    }

    pub fn set(&self, key: String, val: Bytes, expiration_sec: Option<u64>) {
        let nounce = self
            .shared
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let db = &self.shared.state.database;
        let partition_id = db.determine_map(&key);
        let expiration = expiration_sec.map(|v| Instant::now() + Duration::new(v, 0));
        db.insert(
            key.clone(),
            Entry {
                data: val,
                expiration: expiration.clone(),
                nounce,
            },
        );

        if let Some(when) = expiration {
            let task_tx = self.shared.tasks_tx[partition_id % self.shared.tasks_tx.len()].clone();
            let params = (when, nounce, key, partition_id);
            spawn(async move {
                match task_tx.send(params).await {
                    Err(e) => error!("{}", e),
                    _ => (),
                }
            });
        }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == self.shared.num_partition + 1 {
            let _ = self.notify_background_task.send(());
        }
    }
}

async fn purge_expired_keys(
    shared: Arc<Shared>,
    mut tasks_rx: mpsc::Receiver<(Instant, u64, String, usize)>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut when: Option<Instant> = None;
    let mut expirations: BTreeMap<(Instant, u64), (String, usize)> = BTreeMap::new();

    loop {
        let now = Instant::now();

        select! {
            _ = shutdown.recv() => {
                return;
            }
            res = tasks_rx.recv() => {
                if res.is_none() {
                    return;
                }
                let res = res.unwrap();
                let now = Instant::now();
                when = match when {
                    Some(v) => {
                        if res.0 < v && res.0 > now {
                            expirations.insert((res.0, res.1), (res.2, res.3));
                            Some(res.0)
                        } else if res.0 < now {
                            let _ = shared.state.database.remove(&res.2);
                            None
                        } else {
                            expirations.insert((res.0, res.1), (res.2, res.3));
                            Some(v)
                        }
                    }
                    None => Some(res.0),
                };
            }
            _ = tokio::time::sleep_until(
                when.map(|v| v.min(now + Duration::new(3, 0)))
                    .unwrap_or(now + Duration::new(300, 0)),
            ) => {
                if expirations.len() > 0 {
                    when = None;
                    let now = Instant::now();
                    let mut key_bucket = vec![vec![]; shared.num_partition];
                    while let Some((&(expire_next, id), _)) = expirations.iter().next() {
                        if expire_next <= now {
                            let key = expirations.remove(&(expire_next, id)).unwrap();
                            key_bucket[key.1].push(key);
                        } else {
                            when = Some(expire_next);
                            break;
                        }
                    }

                    for (pid, keys) in key_bucket.iter().enumerate().filter(|v| v.1.len() > 0) {
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
