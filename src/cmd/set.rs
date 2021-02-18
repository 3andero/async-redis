use crate::{
    cmd::*,
    db::{Entry, DB},
};
use anyhow::Result;
use tokio::time::{Duration, Instant};

// const MAX_U32: i64 = std::u32::MAX as i64;

const NX: usize = rolling_hash_const(b"nx");
const XX: usize = rolling_hash_const(b"xx");

const EX: usize = rolling_hash_const(b"ex");
const EXAT: usize = rolling_hash_const(b"exat");
const PX: usize = rolling_hash_const(b"px");
const PXAT: usize = rolling_hash_const(b"pxat");
const KEEPTTL: usize = rolling_hash_const(b"keepttl");

const GET: usize = rolling_hash_const(b"get");

const I_NX: usize = 0;
const I_XX: usize = 1;
const I_EX: usize = 2;
const I_EXAT: usize = 3;
const I_PX: usize = 4;
const I_PXAT: usize = 5;
const I_KEEPTTL: usize = 6;
const I_GET: usize = 7;

pub enum SetVariant {
    Set,
    SetEX,
    PSetEX,
    SetNX,
    GetSet,
}

#[derive(Debug, Clone)]
pub enum LoadBehavior {
    None,
    NX,
    XX,
}

#[derive(Debug, Clone)]
pub enum Expiration {
    None,
    KeepTTL,
    At(Instant),
}

#[derive(Debug, Clone)]
pub struct Set {
    key: Bytes,
    val: Frame,
    expiration: Expiration,
    load_behavior: LoadBehavior,
    get: bool,
}

// #[macro_export]
macro_rules! check_set {
    ($checklist:expr, $to_set:expr, $($to_check:expr),*) => {
        if $checklist[$to_set] {
            return Err(Error::new(CommandError::InvalidOperation));
        }
        $(
            if $checklist[$to_check] {
                return Err(Error::new(CommandError::InvalidOperation));
            }
        )*
        $checklist[$to_set] = true;
    }
}

impl Set {
    pub fn new(parser: &mut CommandParser, variant: SetVariant) -> Result<Set> {
        match variant {
            SetVariant::Set => {
                let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let v = parser.next().ok_or_else(missing_operand)?;
                if parser.len() == 0 {
                    return Ok(Self {
                        key: k,
                        val: v,
                        expiration: Expiration::None,
                        load_behavior: LoadBehavior::None,
                        get: false,
                    });
                }
                let mut checklist = [false; 8]; // [NX/XX, EX/PX(etc.), GET]
                let mut expiration = Expiration::None;
                let mut load_behavior = LoadBehavior::None;
                let mut get = false;
                while parser.len() > 0 {
                    let next_byte = parser.next_bytes()?.ok_or_else(missing_operand)?;
                    #[deny(unreachable_patterns)]
                    match rolling_hash(next_byte.as_ref())? {
                        NX => {
                            check_set!(checklist, I_NX, I_GET, I_XX);
                            load_behavior = LoadBehavior::NX;
                        }
                        XX => {
                            check_set!(checklist, I_XX, I_NX);
                            load_behavior = LoadBehavior::XX;
                        }
                        GET => {
                            check_set!(checklist, I_GET, I_NX);
                            get = true;
                        }
                        EX => {
                            check_set!(checklist, I_EX, I_EXAT, I_PX, I_PXAT, I_KEEPTTL);
                            let next_int = parser
                                .next_integer()?
                                .filter(|v| *v > 0)
                                .ok_or_else(missing_operand)?
                                as u64;
                            expiration =
                                Expiration::At(Instant::now() + Duration::new(next_int, 0));
                        }
                        PX => {
                            check_set!(checklist, I_PX, I_EXAT, I_EX, I_PXAT, I_KEEPTTL);
                            let next_int = parser
                                .next_integer()?
                                .filter(|v| *v > 0)
                                .ok_or_else(missing_operand)?;
                            expiration = Expiration::At(
                                Instant::now()
                                    + Duration::new(
                                        next_int as u64 / 1000,
                                        (next_int % 1000) as u32 * 1000000,
                                    ),
                            );
                        }
                        EXAT => {
                            check_set!(checklist, I_EXAT, I_PX, I_EX, I_PXAT, I_KEEPTTL);
                            let next_int = parser
                                .next_integer()?
                                .filter(|v| *v > 0)
                                .ok_or_else(missing_operand)?;
                            let expire_sys = std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(next_int as u64);
                            let dur = expire_sys
                                .duration_since(std::time::SystemTime::now())
                                .map_err(|_| invalid_operand())?;
                            if dur.as_millis() == 0 {
                                invalid_operand();
                            }
                            expiration = Expiration::At(Instant::now() + dur);
                        }
                        PXAT => {
                            check_set!(checklist, I_PXAT, I_PX, I_EX, I_EXAT, I_KEEPTTL);
                            let next_int = parser
                                .next_integer()?
                                .filter(|v| *v > 0)
                                .ok_or_else(missing_operand)?;
                            let expire_sys = std::time::UNIX_EPOCH
                                + std::time::Duration::from_millis(next_int as u64);
                            let dur = expire_sys
                                .duration_since(std::time::SystemTime::now())
                                .map_err(|_| invalid_operand())?;
                            if dur.as_millis() == 0 {
                                invalid_operand();
                            }
                            expiration = Expiration::At(Instant::now() + dur);
                        }
                        KEEPTTL => {
                            check_set!(checklist, I_KEEPTTL, I_PXAT, I_PX, I_EX, I_EXAT);
                            expiration = Expiration::KeepTTL;
                        }
                        _ => {
                            return Err(Error::new(CommandError::InvalidOperation));
                        }
                    }
                }
                return Ok(Self {
                    key: k,
                    val: v,
                    expiration,
                    load_behavior,
                    get,
                });
            }
            SetVariant::GetSet => {
                let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let v = parser.next().ok_or_else(missing_operand)?;
                return Ok(Self {
                    key: k,
                    val: v,
                    expiration: Expiration::None,
                    load_behavior: LoadBehavior::None,
                    get: true,
                });
            }
            SetVariant::SetEX => {
                let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let next_int = parser
                    .next_integer()?
                    .filter(|v| *v > 0)
                    .ok_or_else(missing_operand)? as u64;
                let expiration = Expiration::At(Instant::now() + Duration::new(next_int, 0));
                let v = parser.next().ok_or_else(missing_operand)?;
                return Ok(Self {
                    key: k,
                    val: v,
                    expiration,
                    load_behavior: LoadBehavior::None,
                    get: false,
                });
            }
            SetVariant::PSetEX => {
                let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let next_int = parser
                    .next_integer()?
                    .filter(|v| *v > 0)
                    .ok_or_else(missing_operand)? as u64;
                let expiration = Expiration::At(
                    Instant::now()
                        + Duration::new(next_int as u64 / 1000, (next_int % 1000) as u32 * 1000000),
                );
                let v = parser.next().ok_or_else(missing_operand)?;
                return Ok(Self {
                    key: k,
                    val: v,
                    expiration,
                    load_behavior: LoadBehavior::None,
                    get: false,
                });
            }
            SetVariant::SetNX => {
                let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let v = parser.next().ok_or_else(missing_operand)?;
                return Ok(Self {
                    key: k,
                    val: v,
                    expiration: Expiration::None,
                    load_behavior: LoadBehavior::NX,
                    get: false,
                });
            }
        }
    }
}

impl OneshotExecDB for Set {
    fn exec(self, db: &mut DB) -> Frame {
        db.counter += 1; // might not be necessary.
        let nounce = db.counter;
        let (expiration, keep_ttl) = match self.expiration {
            Expiration::None => (None, false),
            Expiration::At(ex) => (Some(ex), false),
            Expiration::KeepTTL => (None, true),
        };

        db.set(
            &self.load_behavior,
            keep_ttl,
            self.key,
            self.val,
            nounce,
            expiration,
            self.get,
        )
    }

    fn get_key(&self) -> &[u8] {
        &self.key.as_ref()
    }
}

impl DB {
    pub fn set_lite(&mut self, key: Bytes, data: Frame, nounce: u64, expiration: Option<Instant>) {
        self.database.insert(
            key,
            Entry {
                data,
                expiration,
                nounce,
            },
        );
    }

    pub fn set(
        &mut self,
        load_behaviour: &LoadBehavior,
        keep_ttl: bool,
        key: Bytes,
        data: Frame,
        nounce: u64,
        expiration: Option<Instant>,
        get: bool,
    ) -> Frame {
        match load_behaviour {
            LoadBehavior::None => {
                if keep_ttl {
                    if let Some(en) = self.database.get_mut(&key) {
                        return if get {
                            std::mem::replace(&mut en.data, data)
                        } else {
                            en.data = data;
                            Frame::Ok
                        };
                    } else {
                        self.expiration.update(expiration, nounce, &key);
                        self.database.insert(
                            key,
                            Entry {
                                data,
                                expiration,
                                nounce,
                            },
                        );
                        return if get { Frame::NullString } else { Frame::Ok };
                    }
                }
                self.expiration.update(expiration, nounce, &key);
                return self
                    .database
                    .insert(
                        key,
                        Entry {
                            data,
                            expiration,
                            nounce,
                        },
                    )
                    .map_or_else(
                        || if get { Frame::NullString } else { Frame::Ok },
                        |en| {
                            if en.expiration.is_some() {
                                self.expiration.remove(&(en.expiration.unwrap(), en.nounce));
                            }
                            if get {
                                en.data
                            } else {
                                Frame::Ok
                            }
                        },
                    );
            }
            LoadBehavior::NX => match self.database.get_mut(&key) {
                None => {
                    self.expiration.update(expiration, nounce, &key);
                    self.database.insert(
                        key,
                        Entry {
                            data,
                            expiration,
                            nounce,
                        },
                    );
                    return Frame::Ok;
                }
                _ => {
                    return Frame::NullString;
                }
            },
            LoadBehavior::XX => match self.database.get_mut(&key) {
                Some(en) => {
                    if !keep_ttl {
                        if en.expiration.is_some() {
                            self.expiration.remove(&(en.expiration.unwrap(), en.nounce));
                        }
                        en.expiration = expiration;
                        en.nounce = nounce;
                        self.expiration.update(expiration, nounce, &key);
                    }
                    return if get {
                        std::mem::replace(&mut en.data, data)
                    } else {
                        en.data = data;
                        Frame::Ok
                    };
                }
                _ => {
                    return Frame::NullString;
                }
            },
        }
    }
}
