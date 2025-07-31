// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
