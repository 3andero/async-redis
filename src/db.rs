use std::{collections::HashMap, sync::Mutex, sync::Arc};
use bytes::*;
use get::Get;
use crate::{Result, cmd::*};

pub struct State {
    database: HashMap<String, Bytes>
}

pub struct Shared {
    state: Mutex<State>
}

pub struct DB {
    shared: Arc<Shared>
}

impl DB {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(
                Shared {
                    state: Mutex::new(
                        State {
                            database: HashMap::new()
                        }
                    )
                }
            )
        }
    }

    pub fn get(&self, key: &String) -> Option<Bytes> {
        let db = self.shared.state.lock().ok()?;
        db.database.get(key).map(|v| v.clone())
    }

    pub fn set(&self, key: String, val: Bytes) {
        let mut db = match self.shared.state.lock() {
            Ok(v) => v,
            Err(e) => {
                return;
            }
        };
        db.database.insert(key, val);
    }
}