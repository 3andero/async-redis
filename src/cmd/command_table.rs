use crate::utils::*;

use super::{get::GetVariant, incr::IncrVariant, set::SetVariant};

#[derive(Clone, Debug, Copy)]
pub enum CommandTable {
    GET(GetVariant),
    SET(SetVariant),
    MSET,
    MGET,
    INCR(IncrVariant),
    DX,
    SHUTDOWN,
    SUBSCRIBE,
    UNIMPLEMENTED,
}

const GET: usize = rolling_hash_const(b"get");
const TTL: usize = rolling_hash_const(b"ttl");
const PTTL: usize = rolling_hash_const(b"pttl");
const SET: usize = rolling_hash_const(b"set");
const SETEX: usize = rolling_hash_const(b"setex");
const PSETEX: usize = rolling_hash_const(b"psetex");
const SETNX: usize = rolling_hash_const(b"setnx");
const GETSET: usize = rolling_hash_const(b"getset");
const MSET: usize = rolling_hash_const(b"mset");
const MGET: usize = rolling_hash_const(b"mget");
const INCR: usize = rolling_hash_const(b"incr");
const DECR: usize = rolling_hash_const(b"decr");
const INCRBY: usize = rolling_hash_const(b"incrby");
const DECRBY: usize = rolling_hash_const(b"decrby");
const DX: usize = rolling_hash_const(b"dx");
const SHUTDOWN: usize = rolling_hash_const(b"shutdown");
const SUBSCRIBE: usize = rolling_hash_const(b"subscribe");

pub const COMMAND_NUM: usize = 17;

const UNSORTED_TBL: [(usize, CommandTable); COMMAND_NUM] = [
    (GET, CommandTable::GET(GetVariant::Get)),
    (TTL, CommandTable::GET(GetVariant::TTL)),
    (PTTL, CommandTable::GET(GetVariant::PTTL)),
    (SET, CommandTable::SET(SetVariant::Set)),
    (SETEX, CommandTable::SET(SetVariant::SetEX)),
    (PSETEX, CommandTable::SET(SetVariant::PSetEX)),
    (SETNX, CommandTable::SET(SetVariant::SetNX)),
    (GETSET, CommandTable::SET(SetVariant::GetSet)),
    (MSET, CommandTable::MSET),
    (MGET, CommandTable::MGET),
    (INCR, CommandTable::INCR(IncrVariant::Incr)),
    (DECR, CommandTable::INCR(IncrVariant::Decr)),
    (INCRBY, CommandTable::INCR(IncrVariant::IncrBy)),
    (DECRBY, CommandTable::INCR(IncrVariant::DecrBy)),
    (SUBSCRIBE, CommandTable::SUBSCRIBE),
    (DX, CommandTable::DX),
    (SHUTDOWN, CommandTable::SHUTDOWN),
];

pub const fn build_table() -> [(usize, CommandTable); COMMAND_NUM] {
    let mut arr = UNSORTED_TBL;
    let mut i = 0;
    while i < arr.len() {
        let mut j = 0;
        while j < arr.len() - i - 1 {
            if arr[j].0 > arr[j + 1].0 {
                let t = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = t;
            }
            j += 1;
        }
        i += 1;
    }

    i = 0;
    while i < arr.len() - 1 {
        if arr[i].0 == arr[i + 1].0 {
            const_panic();
        }
        i += 1;
    }
    arr
}

pub static COMMAND_LOOKUP: [(usize, CommandTable); COMMAND_NUM] = build_table();
