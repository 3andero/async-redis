use bytes::*;
use std::{cmp::min, sync::Arc, sync::{Mutex, atomic::AtomicU64}, time::Duration};

use dashmap::DashMap;

#[derive(Debug)]
pub struct Entry {
    data: Bytes,
    expiration: Option<Duration>,
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
    counter: AtomicU64
}

impl Shared {
    fn new(num_partition: usize) -> Self {
        let mut tmp = vec![];
        for _ in 0..num_partition {
            tmp.push(State {
                database: DashMap::new(),
            });
        }
        Self {
            num_partition,
            states: tmp,
            counter: AtomicU64::new(0)
        }
    }

    fn partition(&self, s: &String) -> &State {
        let mut p: usize = 0;
        for char in s.as_bytes()[..min(10, s.len())].iter() {
            p += *char as usize;
        }
        &self.states[p % self.num_partition]
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
        let db = &self.shared.partition(key).database;
        db.get(key).map(|v| v.data.clone())
    }

    pub fn set(&self, key: String, val: Bytes, expiration_sec: Option<u64>) {
        let db = &self.shared.partition(&key).database;
        db.insert(
            key,
            Entry {
                data: val,
                expiration: expiration_sec.map(|v| Duration::new(v, 0)),
                nounce: 0
            },
        );
    }
}
