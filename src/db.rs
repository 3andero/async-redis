use bytes::*;
use std::{
    cmp::min,
    collections::BTreeMap,
    sync::Arc,
    sync::{atomic::AtomicU64, Mutex},
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc};

use dashmap::DashMap;

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
    states: Vec<State>,
    counter: AtomicU64,
    tasks_tx: Vec<mpsc::Sender<(Instant, u64, String, usize)>>,
}

impl Shared {
    fn new(num_partition: usize) -> Self {
        let mut tmp = vec![];
        let mut senders = vec![];
        for i in 0..num_partition {
            tmp.push(State {
                database: DashMap::new(),
            });
            if i % 10 == 0 {
                // senders.push()
            }
        }
        Self {
            num_partition,
            states: tmp,
            counter: AtomicU64::new(0),
            tasks_tx: senders,
        }
    }

    fn partition_id(&self, s: &String) -> usize {
        let mut p: usize = 0;
        for char in s.as_bytes()[..min(10, s.len())].iter() {
            p += *char as usize;
        }
        return p % self.num_partition;
    }
}
#[derive(Clone, Debug)]
pub struct DB {
    shared: Arc<Shared>,
}

impl DB {
    pub fn new(num_partition: usize) -> Self {
        Self {
            shared: Arc::new(Shared::new(num_partition)),
        }
    }

    pub fn get(&self, key: &String) -> Option<Bytes> {
        let partition_id = self.shared.partition_id(&key);
        let db = &self.shared.states[partition_id].database;
        db.get(key).map(|v| v.data.clone())
    }

    pub fn set(&self, key: String, val: Bytes, expiration_sec: Option<u64>) {
        let nounce = self
            .shared
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let partition_id = self.shared.partition_id(&key);
        let db = &self.shared.states[partition_id].database;
        db.insert(
            key,
            Entry {
                data: val,
                expiration: expiration_sec.map(|v| Instant::now() + Duration::new(v, 0)),
                nounce,
            },
        );
    }
}

async fn purge_expired_keys(
    states: &Vec<State>,
    mut tasks_rx: mpsc::Receiver<(Instant, u64, String, usize)>,
) {
    let when: Option<Instant> = None;
    let expirations: BTreeMap<(Instant, u64), (String, usize)> = BTreeMap::new();

    loop {
        // select! {
        let res = tasks_rx.recv().await.unwrap();
        {
            match when {
                Some(time) => if res.0 < time {},
                None => (),
            }
        }
        // }
    }
}
