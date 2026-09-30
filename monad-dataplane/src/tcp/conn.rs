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

use std::{
    cell::RefCell,
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    rc::Rc,
    sync::Arc,
};

use tracing::{trace, warn};

use super::{
    tx::{bounded_queue, BoundedQueueReceiver, BoundedQueueSender},
    TcpMsg, TcpSocketId,
};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::DataplaneMetrics,
};

// Accepted and outgoing connections use separate instances of the same limits.
#[derive(Clone)]
pub(crate) struct Limits {
    inner: Rc<RefCell<LimitsInner>>,
    addrlist: Arc<Addrlist>,
}

struct LimitsInner {
    total_limit: usize,
    per_ip_limit: usize,
    total: usize,
    per_ip: BTreeMap<IpAddr, usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LimitError {
    Banned,
    Total { count: usize, limit: usize },
    PerIp { count: usize, limit: usize },
}

#[derive(Clone, Copy)]
enum Quota {
    Counted,
    Exempt,
}

pub(crate) struct Permit {
    limits: Limits,
    ip: IpAddr,
    quota: Quota,
}

impl Limits {
    pub(crate) fn new(addrlist: Arc<Addrlist>, total_limit: usize, per_ip_limit: usize) -> Self {
        Self {
            inner: Rc::new(RefCell::new(LimitsInner {
                total_limit,
                per_ip_limit,
                total: 0,
                per_ip: BTreeMap::new(),
            })),
            addrlist,
        }
    }

    pub(crate) fn is_banned(&self, ip: IpAddr) -> bool {
        self.addrlist.status(&ip) == Status::Banned
    }

    pub(crate) fn try_acquire(&self, ip: IpAddr) -> Result<Permit, LimitError> {
        let quota = match self.addrlist.status(&ip) {
            Status::Banned => return Err(LimitError::Banned),
            Status::Trusted => Quota::Exempt,
            Status::Unknown => {
                let mut inner = self.inner.borrow_mut();
                if inner.total >= inner.total_limit {
                    return Err(LimitError::Total {
                        count: inner.total,
                        limit: inner.total_limit,
                    });
                }
                let per_ip_limit = inner.per_ip_limit;
                let count = inner.per_ip.entry(ip).or_insert(0);
                if *count >= per_ip_limit {
                    return Err(LimitError::PerIp {
                        count: *count,
                        limit: per_ip_limit,
                    });
                }
                *count += 1;
                inner.total += 1;
                Quota::Counted
            }
        };
        Ok(Permit {
            limits: self.clone(),
            ip,
            quota,
        })
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.inner.borrow().total
    }
}

impl Permit {
    pub(crate) fn is_banned(&self) -> bool {
        self.limits.is_banned(self.ip)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if matches!(self.quota, Quota::Exempt) {
            return;
        }
        let mut inner = self.limits.inner.borrow_mut();
        inner.total -= 1;
        if let Some(count) = inner.per_ip.get_mut(&self.ip) {
            if *count > 1 {
                *count -= 1;
            } else {
                inner.per_ip.remove(&self.ip);
            }
        } else {
            warn!(ip = ?self.ip, "num_connections_per_ip should not be empty");
        }
    }
}

#[cfg(test)]
mod limit_tests {
    use super::*;

    #[test]
    fn per_ip_capacity_is_released_and_trusted_peers_are_exempt() {
        let first: IpAddr = "127.0.0.1".parse().unwrap();
        let second: IpAddr = "127.0.0.2".parse().unwrap();
        let trusted: IpAddr = "127.0.0.3".parse().unwrap();
        let other: IpAddr = "127.0.0.4".parse().unwrap();
        let addrlist = Arc::new(Addrlist::new_with_trusted([trusted].into_iter()));
        let limits = Limits::new(addrlist, 2, 1);

        let first_permit = limits.try_acquire(first).unwrap();
        let _trusted_permit = limits.try_acquire(trusted).unwrap();
        assert_eq!(limits.count(), 1);
        assert!(matches!(
            limits.try_acquire(first),
            Err(LimitError::PerIp { count: 1, limit: 1 })
        ));

        let _second_permit = limits.try_acquire(second).unwrap();
        assert!(matches!(
            limits.try_acquire(other),
            Err(LimitError::Total { count: 2, limit: 2 })
        ));

        drop(first_permit);
        assert!(limits.try_acquire(first).is_ok());
    }
}

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
