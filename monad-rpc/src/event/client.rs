use std::sync::Arc;

use tokio::{sync::broadcast, task::JoinHandle};

use super::events::EventServerEvent;

#[derive(Clone)]
pub struct EventServerClient {
    tx: broadcast::Sender<EventServerEvent>,
    handle: Arc<JoinHandle<()>>,
}

impl EventServerClient {
    pub(super) fn new(tx: broadcast::Sender<EventServerEvent>, handle: JoinHandle<()>) -> Self {
        Self {
            tx,
            handle: Arc::new(handle),
        }
    }

    pub fn subscribe(
        &self,
    ) -> Result<broadcast::Receiver<EventServerEvent>, EventServerClientError> {
        if self.handle.is_finished() {
            return Err(EventServerClientError::ServerCrashed);
        }

        Ok(self.tx.subscribe())
    }
}

#[derive(Debug)]
pub enum EventServerClientError {
    ServerCrashed,
}
