// Copyright (C) 2025 Category Labs, Inc.
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_executor::ExecutorMetrics;

use crate::{
    metrics::*, Acknowledgement, EncodeError, FragmentBitmap, FragmentIter, MESSAGE_LIFETIME,
};

#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_messages: usize,
    pub max_bytes: usize,
    pub max_messages_per_peer: usize,
    pub max_bytes_per_peer: usize,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_messages: 1024,
            max_bytes: 64 * 1024 * 1024,
            max_messages_per_peer: 100,
            max_bytes_per_peer: 32 * 1024 * 1024,
        }
    }
}

struct Pending<T> {
    fragments: FragmentIter,
    context: T,
    interval: Duration,
    retries: u8,
    fresh_ack: Option<FragmentBitmap>,
    started: bool,
    created: Instant,
    deadline: Instant,
    expires: Instant,
}

#[derive(Default)]
struct PeerUsage {
    bytes: usize,
    oldest: BTreeSet<(Instant, u16)>,
    waiting: BTreeSet<u16>,
}

/// Retains one payload per outgoing message. IDs come from the encoder's global
/// wrapping counter; insertion replaces older tracking for the same peer and ID.
pub struct RetrySender<I, T>
where
    I: Clone + Hash + Eq + Ord,
    T: Clone,
{
    config: RetryConfig,
    messages: HashMap<(I, u16), Pending<T>>,
    deadlines: BTreeSet<(Instant, (I, u16))>,
    oldest: BTreeSet<(Instant, (I, u16))>,
    peers: HashMap<I, PeerUsage>,
    bytes: usize,
    metrics: ExecutorMetrics,
}

impl<I, T> RetrySender<I, T>
where
    I: Clone + Hash + Eq + Ord,
    T: Clone,
{
    pub fn new(config: RetryConfig) -> Self {
        assert!(config.max_messages > 0 && config.max_bytes > 0);
        assert!(config.max_messages_per_peer > 0 && config.max_bytes_per_peer > 0);
        Self {
            config,
            messages: HashMap::new(),
            deadlines: BTreeSet::new(),
            oldest: BTreeSet::new(),
            peers: HashMap::new(),
            bytes: 0,
            metrics: init_retry_metrics(),
        }
    }

    /// Registers an initial transmission. Bounds affect retry tracking only:
    /// the caller must still send the initial fragments when tracking is pruned.
    pub fn track(
        &mut self,
        peer: I,
        fragments: FragmentIter,
        context: T,
        interval: Duration,
        now: Instant,
    ) -> Result<(), EncodeError> {
        if interval.is_zero() || now.checked_add(interval).is_none() {
            return Err(EncodeError::InvalidRetryInterval);
        }
        let key = (peer.clone(), fragments.msg_id());
        if self.remove(&key).is_some() {
            self.metrics.gauge(COUNTER_LEANUDP_RETRY_PRUNED).inc();
        }
        let size = fragments.payload_len();
        if size > self.config.max_bytes || size > self.config.max_bytes_per_peer {
            self.metrics.gauge(COUNTER_LEANUDP_RETRY_PRUNED).inc();
            return Ok(());
        }
        while self.peers.get(&peer).is_some_and(|usage| {
            usage.oldest.len() >= self.config.max_messages_per_peer
                || usage.bytes + size > self.config.max_bytes_per_peer
        }) {
            let id = self.peers[&peer]
                .oldest
                .first()
                .expect("peer at capacity")
                .1;
            self.remove(&(peer.clone(), id));
            self.metrics.gauge(COUNTER_LEANUDP_RETRY_PRUNED).inc();
        }
        while self.messages.len() >= self.config.max_messages
            || self.bytes + size > self.config.max_bytes
        {
            let oldest = self.oldest.first().expect("sender at capacity").1.clone();
            self.remove(&oldest);
            self.metrics.gauge(COUNTER_LEANUDP_RETRY_PRUNED).inc();
        }
        // Handshake buffering gets a bounded wait too; starting transmission
        // resets the deadline to two seconds from the actual initial send.
        let expires = now + MESSAGE_LIFETIME;
        self.messages.insert(
            key.clone(),
            Pending {
                fragments,
                context,
                interval,
                retries: 0,
                fresh_ack: None,
                started: false,
                created: now,
                deadline: expires,
                expires,
            },
        );
        self.deadlines.insert((expires, key.clone()));
        self.oldest.insert((now, key.clone()));
        let usage = self.peers.entry(peer).or_default();
        usage.bytes += size;
        usage.oldest.insert((now, key.1));
        usage.waiting.insert(key.1);
        self.bytes += size;
        self.update_gauges();
        Ok(())
    }

    pub fn mark_sent(&mut self, peer: &I, msg_id: u16, now: Instant) {
        let key = (peer.clone(), msg_id);
        let Some(state) = self.messages.get_mut(&key) else {
            return;
        };
        if state.started {
            return;
        }
        self.deadlines.remove(&(state.deadline, key.clone()));
        state.started = true;
        state.expires = now + MESSAGE_LIFETIME;
        state.deadline = now
            .checked_add(state.interval)
            .unwrap_or(state.expires)
            .min(state.expires);
        self.deadlines.insert((state.deadline, key));
        self.peers
            .get_mut(peer)
            .expect("tracked peer")
            .waiting
            .remove(&msg_id);
    }

    pub fn waiting_peers(&self) -> Vec<I> {
        self.peers
            .iter()
            .filter(|(_, usage)| !usage.waiting.is_empty())
            .map(|(peer, _)| peer.clone())
            .collect()
    }

    pub fn mark_connected(&mut self, peer: &I, now: Instant) {
        let waiting: Vec<_> = self
            .peers
            .get(peer)
            .map(|usage| usage.waiting.iter().copied().collect())
            .unwrap_or_default();
        for id in waiting {
            self.mark_sent(peer, id, now);
        }
    }

    pub fn cancel(&mut self, peer: &I, msg_id: u16) {
        self.remove(&(peer.clone(), msg_id));
    }

    pub fn acknowledge(&mut self, peer: &I, ack: Acknowledgement, now: Instant) {
        let key = (peer.clone(), ack.msg_id);
        let Some(state) = self.messages.get_mut(&key) else {
            return;
        };
        if now >= state.expires {
            self.remove(&key);
            self.metrics.gauge(COUNTER_LEANUDP_RETRY_EXPIRED).inc();
        } else if ack.received.is_complete(state.fragments.fragment_count()) {
            self.remove(&key);
            self.metrics.gauge(COUNTER_LEANUDP_RETRY_ACKNOWLEDGED).inc();
        } else {
            // A receiver can discard chunks on eviction, so replace rather than OR.
            state.fresh_ack = Some(ack.received);
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|entry| entry.0)
    }

    pub fn poll_retries(&mut self, now: Instant) -> Vec<(I, T, Bytes)> {
        let mut packets = Vec::new();
        while self.deadlines.first().is_some_and(|entry| entry.0 <= now) {
            let (_, key) = self.deadlines.pop_first().expect("due retransmission");
            let state = self.messages.get_mut(&key).expect("indexed transmission");
            if now >= state.expires || !state.started {
                self.remove(&key);
                self.metrics.gauge(COUNTER_LEANUDP_RETRY_EXPIRED).inc();
                continue;
            }
            let received = state.fresh_ack.take().unwrap_or_default();
            let before = packets.len();
            for seq in 0..state.fragments.fragment_count() as u16 {
                if !received.contains(seq) {
                    packets.push((
                        key.0.clone(),
                        state.context.clone(),
                        state.fragments.packet(seq).expect("valid fragment"),
                    ));
                }
            }
            self.metrics
                .gauge(COUNTER_LEANUDP_RETRY_TRANSMISSIONS)
                .inc();
            self.metrics
                .gauge(COUNTER_LEANUDP_RETRY_FRAGMENTS)
                .add((packets.len() - before) as u64);
            state.retries += 1;
            state.deadline = if state.retries < 3 {
                now.checked_add(state.interval)
                    .unwrap_or(state.expires)
                    .min(state.expires)
            } else {
                state.expires
            };
            self.deadlines.insert((state.deadline, key));
        }
        packets
    }

    fn remove(&mut self, key: &(I, u16)) -> Option<Pending<T>> {
        let state = self.messages.remove(key)?;
        self.deadlines.remove(&(state.deadline, key.clone()));
        self.oldest.remove(&(state.created, key.clone()));
        let size = state.fragments.payload_len();
        self.bytes -= size;
        let usage = self.peers.get_mut(&key.0).expect("tracked peer");
        usage.bytes -= size;
        usage.oldest.remove(&(state.created, key.1));
        usage.waiting.remove(&key.1);
        if usage.oldest.is_empty() {
            self.peers.remove(&key.0);
        }
        self.update_gauges();
        Some(state)
    }

    fn update_gauges(&mut self) {
        self.metrics
            .gauge(GAUGE_LEANUDP_RETRY_MESSAGES)
            .set(self.messages.len() as u64);
        self.metrics
            .gauge(GAUGE_LEANUDP_RETRY_BYTES)
            .set(self.bytes as u64);
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, FragmentPolicy, IdentityScore, Packet};
    struct Score;
    impl IdentityScore for Score {
        type Identity = u64;
        fn score(&self, _: &u64) -> FragmentPolicy {
            FragmentPolicy::Regular
        }
    }
    fn fragments() -> FragmentIter {
        Config {
            max_fragment_payload: 64,
            ..Config::default()
        }
        .build(Score)
        .0
        .fragment(Bytes::from(vec![7; 150]))
        .unwrap()
    }
    fn ack(id: u16, seqs: &[u16]) -> Acknowledgement {
        let mut received = FragmentBitmap::default();
        for seq in seqs {
            received.insert(*seq);
        }
        Acknowledgement {
            msg_id: id,
            received,
        }
    }
    fn sequences(packets: Vec<(u64, (), Bytes)>) -> Vec<u16> {
        packets
            .into_iter()
            .map(|(_, _, packet)| Packet::try_from(packet).unwrap().header.seq_num())
            .collect()
    }

    #[test]
    fn selective_retries_replace_snapshots_and_fall_back_without_fresh_ack() {
        let now = Instant::now();
        let mut sender = RetrySender::new(RetryConfig::default());
        sender
            .track(1, fragments(), (), Duration::from_millis(500), now)
            .unwrap();
        sender.mark_sent(&1, 0, now);
        sender.acknowledge(&1, ack(0, &[0]), now);
        sender.acknowledge(&1, ack(0, &[1]), now); // discarded receiver state
        assert!(sender
            .poll_retries(now + Duration::from_millis(499))
            .is_empty());
        assert_eq!(
            sequences(sender.poll_retries(now + Duration::from_millis(500))),
            [0, 2]
        );
        assert_eq!(
            sequences(sender.poll_retries(now + Duration::from_millis(1000))),
            [0, 1, 2]
        );
        sender.acknowledge(&1, ack(0, &[0, 2]), now + Duration::from_millis(1200));
        assert_eq!(
            sequences(sender.poll_retries(now + Duration::from_millis(1500))),
            [1]
        );
        assert!(sender
            .poll_retries(now + Duration::from_millis(1999))
            .is_empty());
        assert_eq!(sender.messages.len(), 1);
        assert!(sender
            .poll_retries(now + Duration::from_millis(2000))
            .is_empty());
        assert!(sender.messages.is_empty() && sender.peers.is_empty());
        assert_eq!(
            sender.metrics.gauge(COUNTER_LEANUDP_RETRY_FRAGMENTS).get(),
            6
        );
        assert_eq!(sender.metrics.gauge(COUNTER_LEANUDP_RETRY_EXPIRED).get(), 1);
    }

    #[test]
    fn completion_ack_is_peer_scoped_and_releases_payload_immediately() {
        let now = Instant::now();
        let mut sender = RetrySender::new(RetryConfig::default());
        for peer in [1, 2] {
            sender
                .track(peer, fragments(), (), Duration::from_millis(500), now)
                .unwrap();
            sender.mark_sent(&peer, 0, now);
        }
        sender.acknowledge(&1, ack(0, &[0, 1, 2]), now);
        assert!(!sender.messages.contains_key(&(1, 0)));
        assert!(sender.messages.contains_key(&(2, 0)));
        assert_eq!(sender.bytes, 150);
        sender.acknowledge(&1, ack(0, &[0, 1, 2]), now);
        assert_eq!(
            sender
                .metrics
                .gauge(COUNTER_LEANUDP_RETRY_ACKNOWLEDGED)
                .get(),
            1
        );
    }

    #[test]
    fn interval_is_per_message_and_clock_starts_after_handshake() {
        let now = Instant::now();
        let mut sender = RetrySender::new(RetryConfig::default());
        sender
            .track(1, fragments(), (), Duration::from_millis(300), now)
            .unwrap();
        assert!(sender
            .poll_retries(now + Duration::from_millis(500))
            .is_empty());
        sender.mark_connected(&1, now + Duration::from_secs(1));
        assert_eq!(
            sender.next_deadline(),
            Some(now + Duration::from_millis(1300))
        );
        for ms in [1300, 1600, 1900] {
            assert_eq!(
                sender.poll_retries(now + Duration::from_millis(ms)).len(),
                3
            );
        }
        assert_eq!(sender.next_deadline(), Some(now + Duration::from_secs(3)));
        sender.poll_retries(now + Duration::from_secs(3));
        assert!(sender.messages.is_empty());
        assert!(sender
            .track(1, fragments(), (), Duration::ZERO, now)
            .is_err());
    }

    #[test]
    fn collisions_and_byte_limits_prune_older_tracking() {
        let now = Instant::now();
        let config = RetryConfig {
            max_bytes: 300,
            max_bytes_per_peer: 150,
            ..RetryConfig::default()
        };
        let mut sender = RetrySender::new(config);
        sender
            .track(1, fragments(), (), Duration::from_millis(500), now)
            .unwrap();
        sender
            .track(1, fragments(), (), Duration::from_millis(200), now)
            .unwrap();
        assert_eq!(
            sender.messages[&(1, 0)].interval,
            Duration::from_millis(200)
        );
        for peer in [2, 3] {
            sender
                .track(peer, fragments(), (), Duration::from_millis(500), now)
                .unwrap();
        }
        assert_eq!(sender.messages.len(), 2);
        assert!(!sender.messages.contains_key(&(1, 0)));
        let mut encoder = Config::default().build(Score).0;
        encoder.fragment(Bytes::new()).unwrap();
        let next = encoder.fragment(Bytes::from(vec![7; 150])).unwrap();
        sender
            .track(3, next, (), Duration::from_millis(500), now)
            .unwrap();
        assert!(!sender.messages.contains_key(&(3, 0)));
        assert!(sender.messages.contains_key(&(3, 1)));
        assert_eq!(sender.bytes, 300);
        assert_eq!(sender.deadlines.len(), 2);
        assert_eq!(sender.oldest.len(), 2);
    }

    #[test]
    fn message_limits_and_unstarted_timeout_clean_all_indexes() {
        let now = Instant::now();
        let config = RetryConfig {
            max_messages: 2,
            max_messages_per_peer: 1,
            ..RetryConfig::default()
        };
        let mut sender = RetrySender::new(config);
        let mut encoder = Config::default().build(Score).0;
        for peer in [1, 1, 2, 3] {
            let iter = encoder.fragment(Bytes::from_static(b"x")).unwrap();
            sender
                .track(peer, iter, (), Duration::from_millis(500), now)
                .unwrap();
        }
        assert_eq!(sender.messages.len(), 2);
        sender.poll_retries(now + MESSAGE_LIFETIME);
        assert!(
            sender.messages.is_empty()
                && sender.deadlines.is_empty()
                && sender.oldest.is_empty()
                && sender.peers.is_empty()
        );
        assert_eq!(sender.bytes, 0);
    }
}
