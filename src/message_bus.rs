use std::sync::Mutex;

use crossbeam_channel::Sender;

pub struct MessageBus<T> {
    inbox: Mutex<Vec<T>>,
    notify_tx: Sender<()>,
}

impl<T> MessageBus<T> {
    pub fn new(inbox: Mutex<Vec<T>>, notify_tx: Sender<()>) -> Self {
        Self { inbox, notify_tx }
    }

    pub fn push(&self, msg: T) {
        self.inbox.lock().unwrap().push(msg);
        let _ = self.notify_tx.send(());
    }

    pub fn drain(&self) -> Vec<T> {
        std::mem::take(&mut *self.inbox.lock().unwrap())
    }
}
