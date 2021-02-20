use tokio::sync::mpsc;
use crate::{cmd::*, protocol::Frame};
use traverse_command::*;
use super::{MiniCommand};

#[derive(Debug, Clone)]
pub struct Subscribe {
    pairs: Vec<MiniCommand>,
}

impl Subscribe {
    pub fn new(pairs: Vec<MiniCommand>) -> Self {
        Self { pairs }
    }
}