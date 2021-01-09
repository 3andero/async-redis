use tokio::sync::*;

#[derive(Debug)]
pub struct Shutdown {
    shutdown_begin: broadcast::Receiver<()>,
    is_shutdown_: bool,
}

impl Shutdown {
    pub fn new(recv: broadcast::Receiver<()>) -> Self {
        Self {
            shutdown_begin: recv,
            is_shutdown_: false,
        }
    }

    pub async fn recv(&mut self) -> bool {
        if self.is_shutdown_ {
            return true;
        }

        let _ = self.shutdown_begin.recv().await;
        self.is_shutdown_ = true;
        return true;
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown_
    }
}
