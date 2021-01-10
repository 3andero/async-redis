use bytes::*;
use std::{collections::HashMap, sync::Arc, sync::Mutex};

#[derive(Debug)]
pub struct State {
    database: HashMap<String, Bytes>,
}

#[derive(Debug)]
pub struct Shared {
    num_partition: usize,
    states: Vec<Mutex<State>>,
}

impl Shared {
    fn new(num_partition: usize) -> Self {
        let mut tmp = vec![];
        for _ in 0..num_partition {
            tmp.push(Mutex::new(State {
                database: HashMap::new(),
            }));
        }
        Self {
            num_partition,
            states: tmp,
        }
    }

    fn partition(&self, s: &String) -> &Mutex<State> {
        let mut p: usize = 0;
        for char in s.as_bytes().iter() {
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
        let db = self.shared.partition(key).lock().ok()?;
        db.database.get(key).map(|v| v.clone())
    }

    pub fn set(&self, key: String, val: Bytes) {
        let mut db = match self.shared.partition(&key).lock() {
            Ok(v) => v,
            Err(_e) => {
                return;
            }
        };
        db.database.insert(key, val);
    }
}
