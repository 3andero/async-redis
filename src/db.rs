use bytes::*;
use std::{
    cmp::min,
    collections::BTreeMap,
    sync::Arc,
    sync::{atomic::AtomicU64, Mutex},
    unimplemented,
};
use tokio::{
    select,
    sync::mpsc,
    time::{Duration, Instant},
};

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
    state: State,
    counter: AtomicU64,
    tasks_tx: Vec<mpsc::Sender<(Instant, u64, String, usize)>>,
}

impl Shared {
    fn new(num_partition: usize) -> Self {
        let mut senders = vec![];
        for i in 0..num_partition {
            if i % 10 == 0 {
                // senders.push()
            }
        }
        Self {
            num_partition,
            state: State {
                database: DashMap::new(),
            },
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
    let mut when: Option<Instant> = None;
    let expirations: BTreeMap<(Instant, u64), (String, usize)> = BTreeMap::new();

    loop {
        let now = Instant::now();

        // select! {
        let res = tasks_rx.recv().await.unwrap();
        {
            let now = Instant::now();
            when = match when {
                Some(v) => {
                    if res.0 < v && res.0 > now {
                        expirations.insert((res.0, res.1), (res.2, res.3));
                        Some(res.0)
                    } else if res.0 < now {
                        unimplemented!(); // purge expired keys.
                        None
                    } else {
                        expirations.insert((res.0, res.1), (res.2, res.3));
                        Some(v)
                    }
                }
                None => Some(res.0),
            };
        }
        let _ = tokio::time::sleep(
            when.map(|v| v.min(now + Duration::new(3, 0)))
                .unwrap_or_default(now + Duration::MAX),
        );

        {
            let now = Instant::now();
            let keys = vec![];
            while let Some((&(next_expire, _), (key, partition_id))) = expirations.iter().next() {
                if next_expire <= now {
                    
                }
            }
        }
        // }
    }
}
