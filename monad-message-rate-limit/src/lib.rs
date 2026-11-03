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

//! Per-identity message budgets, selected before decoding a message body.
//!
//! Dispatch uses array indexing followed by one identity-map lookup on a hit.
//! All identities share a bounded LRU. Eviction discards an identity's buckets.

use std::{
    hash::Hash,
    num::{NonZeroU32, NonZeroUsize},
};

use governor::{
    clock::{Clock, DefaultClock},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota,
};
use lru::LruCache;

pub const DEFAULT_CAPACITY: usize = 20_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Protocol {
    /// Application messages without a more specific header classifier.
    Application,
    Consensus,
    BlockSyncRequest,
    BlockSyncResponse,
    ForwardedTx,
    StateSync,
    PeerDiscovery,
    FullNodesGroup,
}

impl Protocol {
    const COUNT: usize = Self::FullNodesGroup as usize + 1;
}

/// A protocol and its optional wire message discriminator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageKind {
    pub protocol: Protocol,
    pub message: Option<u8>,
}

impl MessageKind {
    pub const PROPOSAL: Self = Self::new(Protocol::Consensus, 1);
    pub const VOTE: Self = Self::new(Protocol::Consensus, 2);
    pub const BLOCK_SYNC_HEADERS_REQUEST: Self = Self::new(Protocol::BlockSyncRequest, 1);
    pub const BLOCK_SYNC_PAYLOAD_REQUEST: Self = Self::new(Protocol::BlockSyncRequest, 2);
    pub const BLOCK_SYNC_HEADERS_RESPONSE: Self = Self::new(Protocol::BlockSyncResponse, 1);
    pub const BLOCK_SYNC_PAYLOAD_RESPONSE: Self = Self::new(Protocol::BlockSyncResponse, 2);

    pub const fn new(protocol: Protocol, message: u8) -> Self {
        Self {
            protocol,
            message: Some(message),
        }
    }
}

impl From<Protocol> for MessageKind {
    fn from(protocol: Protocol) -> Self {
        Self {
            protocol,
            message: None,
        }
    }
}

type Bucket<C> =
    governor::RateLimiter<NotKeyed, InMemoryState, C, NoOpMiddleware<<C as Clock>::Instant>>;

struct Dispatch {
    protocol: Option<usize>,
    messages: [Option<usize>; 256],
}

impl Default for Dispatch {
    fn default() -> Self {
        Self {
            protocol: None,
            messages: [None; 256],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Unregistered,
    RateLimited,
}

pub struct RateLimiter<I, C: Clock = DefaultClock> {
    dispatch: [Dispatch; Protocol::COUNT],
    quotas: Vec<Quota>,
    identities: LruCache<I, Vec<Option<Bucket<C>>>>,
    clock: C,
}

impl<I: Hash + Eq + Clone> RateLimiter<I> {
    pub fn new(capacity: usize) -> Self {
        Self::with_clock(capacity, DefaultClock::default())
    }

    /// The node's registrations. More specific rules override protocol rules;
    /// only the selected rule is charged.
    pub fn registered() -> Self {
        let mut limiter = Self::new(DEFAULT_CAPACITY);
        let quota = |rate| {
            Quota::per_second(NonZeroU32::new(rate).unwrap())
                .allow_burst(NonZeroU32::new(rate * 2).unwrap())
        };
        limiter.register(Protocol::Application, quota(1_000));
        limiter.register(Protocol::Consensus, quota(10));
        limiter.register(MessageKind::PROPOSAL, quota(10));
        limiter.register(MessageKind::VOTE, quota(10));
        limiter.register(Protocol::BlockSyncRequest, quota(20));
        limiter.register(MessageKind::BLOCK_SYNC_HEADERS_REQUEST, quota(20));
        limiter.register(MessageKind::BLOCK_SYNC_PAYLOAD_REQUEST, quota(20));
        limiter.register(Protocol::BlockSyncResponse, quota(20));
        limiter.register(MessageKind::BLOCK_SYNC_HEADERS_RESPONSE, quota(20));
        limiter.register(MessageKind::BLOCK_SYNC_PAYLOAD_RESPONSE, quota(20));
        limiter.register(Protocol::ForwardedTx, quota(2_000));
        limiter.register(Protocol::StateSync, quota(20));
        limiter.register(Protocol::PeerDiscovery, quota(10));
        limiter.register(Protocol::FullNodesGroup, quota(10));
        limiter
    }
}

impl<I: Hash + Eq + Clone, C: Clock + Clone> RateLimiter<I, C> {
    pub fn with_clock(capacity: usize, clock: C) -> Self {
        Self {
            dispatch: std::array::from_fn(|_| Dispatch::default()),
            quotas: Vec::new(),
            identities: LruCache::new(
                NonZeroUsize::new(capacity).expect("capacity must be nonzero"),
            ),
            clock,
        }
    }

    /// Register rules before adding identities. Registration order does not
    /// affect specificity. Re-registering a matcher replaces its quota.
    pub fn register(&mut self, matcher: impl Into<MessageKind>, quota: Quota) {
        assert!(
            self.identities.is_empty(),
            "register rules before identities"
        );
        let matcher = matcher.into();
        let dispatch = &mut self.dispatch[matcher.protocol as usize];
        let slot = match matcher.message {
            Some(message) => &mut dispatch.messages[message as usize],
            None => &mut dispatch.protocol,
        };
        if let Some(rule) = *slot {
            self.quotas[rule] = quota;
        } else {
            *slot = Some(self.quotas.len());
            self.quotas.push(quota);
        }
    }

    /// Check one message. Invalid or unregistered headers should be rejected
    /// before calling this method. An admitted malformed body still spends
    /// its budget.
    pub fn check(&mut self, identity: &I, kind: impl Into<MessageKind>) -> Result<(), Rejection> {
        let kind = kind.into();
        let dispatch = &self.dispatch[kind.protocol as usize];
        let rule = kind
            .message
            .and_then(|message| dispatch.messages[message as usize])
            .or(dispatch.protocol)
            .ok_or(Rejection::Unregistered)?;

        let rules = self.quotas.len();
        let buckets = self
            .identities
            .get_or_insert_mut_ref(identity, || (0..rules).map(|_| None).collect());
        let quota = self.quotas[rule];
        let clock = &self.clock;
        buckets[rule]
            .get_or_insert_with(|| governor::RateLimiter::direct_with_clock(quota, clock.clone()))
            .check()
            .map_err(|_| Rejection::RateLimited)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use governor::clock::FakeRelativeClock;

    use super::*;

    fn quota(burst: u32) -> Quota {
        Quota::per_second(NonZeroU32::new(1).unwrap()).allow_burst(NonZeroU32::new(burst).unwrap())
    }

    fn limiter(capacity: usize) -> (RateLimiter<u64, FakeRelativeClock>, FakeRelativeClock) {
        let clock = FakeRelativeClock::default();
        (RateLimiter::with_clock(capacity, clock.clone()), clock)
    }

    #[test]
    fn specificity_is_independent_of_registration_order() {
        for specific_first in [true, false] {
            let (mut limiter, _) = limiter(2);
            if specific_first {
                limiter.register(MessageKind::VOTE, quota(1));
            }
            limiter.register(Protocol::Consensus, quota(2));
            if !specific_first {
                limiter.register(MessageKind::VOTE, quota(1));
            }
            assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
            assert_eq!(
                limiter.check(&0, MessageKind::VOTE),
                Err(Rejection::RateLimited)
            );
            // Non-overridden kinds share the protocol bucket.
            assert_eq!(limiter.check(&0, MessageKind::PROPOSAL), Ok(()));
            assert_eq!(
                limiter.check(&0, MessageKind::new(Protocol::Consensus, 3)),
                Ok(())
            );
            assert_eq!(
                limiter.check(&0, MessageKind::PROPOSAL),
                Err(Rejection::RateLimited)
            );
        }
    }

    #[test]
    fn identities_protocols_and_refill_are_independent() {
        let (mut limiter, clock) = limiter(2);
        limiter.register(Protocol::Consensus, quota(1));
        limiter.register(Protocol::ForwardedTx, quota(1));
        assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
        assert_eq!(
            limiter.check(&0, MessageKind::VOTE),
            Err(Rejection::RateLimited)
        );
        assert_eq!(limiter.check(&1, MessageKind::VOTE), Ok(()));
        assert_eq!(limiter.check(&0, Protocol::ForwardedTx), Ok(()));
        clock.advance(Duration::from_millis(999));
        assert_eq!(
            limiter.check(&0, MessageKind::VOTE),
            Err(Rejection::RateLimited)
        );
        clock.advance(Duration::from_millis(1));
        assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
    }

    #[test]
    fn unregistered_messages_do_not_allocate_identity_state() {
        let (mut limiter, _) = limiter(1);
        limiter.register(MessageKind::VOTE, quota(1));
        assert_eq!(
            limiter.check(&0, MessageKind::PROPOSAL),
            Err(Rejection::Unregistered)
        );
        assert!(limiter.identities.is_empty());
        assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
    }

    #[test]
    fn lru_evicts_whole_identities_and_refreshes_on_rejection() {
        let (mut limiter, _) = limiter(2);
        limiter.register(Protocol::Consensus, quota(1));
        limiter.register(Protocol::ForwardedTx, quota(1));
        assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
        assert_eq!(limiter.check(&0, Protocol::ForwardedTx), Ok(()));
        assert_eq!(limiter.check(&1, MessageKind::VOTE), Ok(()));
        assert_eq!(
            limiter.check(&0, MessageKind::VOTE),
            Err(Rejection::RateLimited)
        );
        assert_eq!(limiter.check(&2, MessageKind::VOTE), Ok(()));
        assert!(!limiter.identities.contains(&1));
        assert_eq!(
            limiter.check(&0, Protocol::ForwardedTx),
            Err(Rejection::RateLimited)
        );
        assert_eq!(limiter.check(&1, MessageKind::VOTE), Ok(()));
        assert!(!limiter.identities.contains(&2));
        assert_eq!(limiter.identities.len(), 2);
    }

    #[test]
    fn reregister_replaces_the_quota() {
        let (mut limiter, _) = limiter(1);
        limiter.register(MessageKind::VOTE, quota(1));
        limiter.register(MessageKind::VOTE, quota(2));
        assert_eq!(limiter.quotas.len(), 1);
        assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
        assert_eq!(limiter.check(&0, MessageKind::VOTE), Ok(()));
        assert_eq!(
            limiter.check(&0, MessageKind::VOTE),
            Err(Rejection::RateLimited)
        );
    }
}
