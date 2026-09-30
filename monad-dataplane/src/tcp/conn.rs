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

use std::{cell::RefCell, collections::BTreeMap, net::SocketAddr, rc::Rc};

use tracing::trace;

use super::{
    tx::{bounded_queue, BoundedQueueReceiver, BoundedQueueSender},
    TcpMsg, TcpSocketId,
};
use crate::metrics::DataplaneMetrics;

// Shared routing for accepted and outgoing connections. Admission limits are
// owned by the accept and connect paths, independently of this registry.
#[derive(Clone)]
pub(crate) struct ConnectionRegistry(Rc<RefCell<RegistryInner>>);

struct RegistryInner {
    send_queues: BTreeMap<(TcpSocketId, SocketAddr), BoundedQueueSender>,
    next_connection_id: u64,
}

impl ConnectionRegistry {
    pub(crate) fn new() -> Self {
        Self(Rc::new(RefCell::new(RegistryInner {
            send_queues: BTreeMap::new(),
            next_connection_id: 0,
        })))
    }

    pub(crate) fn register(
        &self,
        key: (TcpSocketId, SocketAddr),
    ) -> Option<(BoundedQueueReceiver, ConnectionRegistration)> {
        let mut inner = self.0.borrow_mut();
        if inner.send_queues.contains_key(&key) {
            return None;
        }

        let (sender, receiver) = bounded_queue();
        inner.send_queues.insert(key, sender);
        let conn_id = inner.next_connection_id;
        inner.next_connection_id += 1;
        Some((
            receiver,
            ConnectionRegistration {
                registry: self.clone(),
                key,
                conn_id,
            },
        ))
    }

    pub(crate) fn try_send(
        &self,
        key: &(TcpSocketId, SocketAddr),
        msg: TcpMsg,
        metrics: &DataplaneMetrics,
    ) -> Option<TcpMsg> {
        let inner = self.0.borrow();
        let Some(sender) = inner.send_queues.get(key) else {
            return Some(msg);
        };
        sender.enqueue(key.1, msg, metrics);
        None
    }
}

pub(crate) struct ConnectionRegistration {
    registry: ConnectionRegistry,
    key: (TcpSocketId, SocketAddr),
    pub(super) conn_id: u64,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        self.registry.0.borrow_mut().send_queues.remove(&self.key);
        let addr = self.key.1;
        trace!(?addr, "removed connection send queue");
    }
}
