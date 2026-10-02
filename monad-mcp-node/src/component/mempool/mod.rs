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

//! A FIFO of txs waiting for this node's next proposal. A drained tx stays
//! in flight until its slot settles: committed, it is remembered to reject
//! re-sends; left out of the block, it goes back to the head of the queue.

mod source;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
};

use alloy_rlp::Encodable as _;
use bytes::Bytes;
use monad_mcp_chorus::ledger::{BatchBuilder, Hash, MAX_TX_PAYLOAD, Tx, TxError};

pub use self::source::{MempoolSource, ProposalSource, RandomSource};
use crate::{
    chorus::types::{ProposalIndex, Slot},
    config::MempoolConfig,
};

// the tx with the longest encoding the node accepts
pub fn largest_tx() -> Tx {
    Tx {
        sender: [0xff; 20],
        nonce: u64::MAX,
        payload: Bytes::from(vec![0xff; MAX_TX_PAYLOAD]),
        sent_at_ns: u64::MAX,             // demo(tx-timeline)
        rpc_received_at_ns: u64::MAX,     // demo(tx-timeline)
        mempool_admitted_at_ns: u64::MAX, // demo(tx-timeline)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Reject {
    #[error("tx is already known")]
    Duplicate,
    #[error("mempool is full")]
    Full,
    #[error("invalid tx: {0}")]
    Invalid(#[from] TxError),
}

struct Entry {
    hash: Hash,
    size: usize,
    tx: Tx,
}

pub struct Mempool {
    config: MempoolConfig,
    queue: VecDeque<Entry>,
    queued_bytes: usize,
    in_flight: BTreeMap<(Slot, ProposalIndex), Vec<Entry>>,
    // every hash queued or in flight
    pending: HashSet<Hash>,
    recent: VecDeque<Hash>,
    recent_set: HashSet<Hash>,
    // slots at or past which a drain may still commit: not settled, not below the cap
    floor: Slot,
    settled: BTreeSet<Slot>,
}

impl Mempool {
    pub fn new(config: MempoolConfig) -> Self {
        Self {
            config,
            queue: VecDeque::new(),
            queued_bytes: 0,
            in_flight: BTreeMap::new(),
            pending: HashSet::new(),
            recent: VecDeque::new(),
            recent_set: HashSet::new(),
            floor: Slot::FIRST,
            settled: BTreeSet::new(),
        }
    }

    // demo(tx-timeline): `now_ns` is the node clock's unix ns, stamped as `mempool_admitted_at_ns`
    pub fn admit(&mut self, mut tx: Tx, now_ns: u64) -> Result<(), Reject> {
        tx.validate()?;
        let hash = tx.hash();
        if self.pending.contains(&hash) || self.recent_set.contains(&hash) {
            return Err(Reject::Duplicate);
        }
        tx.mempool_admitted_at_ns = now_ns; // demo(tx-timeline)
        let size = tx.length();
        if self.queue.len() >= self.config.max_txs
            || self.queued_bytes + size > self.config.max_bytes
        {
            return Err(Reject::Full);
        }
        self.pending.insert(hash);
        self.queued_bytes += size;
        self.queue.push_back(Entry { hash, size, tx });
        Ok(())
    }

    // demo(tx-timeline): `rlp[sealed_at_ns, Vec<Tx>]` of the queue's head within
    // `proposal_size_limit`, in flight at (slot, index) until the slot settles. A slot
    // already settled gets an empty batch: nothing drained for it could commit.
    pub fn drain(
        &mut self,
        slot: Slot,
        index: ProposalIndex,
        proposal_size_limit: usize,
        sealed_at_ns: u64, // demo(tx-timeline)
    ) -> Bytes {
        let mut batch = BatchBuilder::new(proposal_size_limit);
        if slot < self.floor || self.settled.contains(&slot) {
            return batch.finish(sealed_at_ns); // demo(tx-timeline)
        }
        let mut drained = Vec::new();
        while let Some(entry) = self.queue.front() {
            if !batch.fits(&entry.tx) {
                break;
            }
            let entry = self.queue.pop_front().expect("front is present");
            self.queued_bytes -= entry.size;
            batch.try_push(entry.tx.clone()).expect("checked by fits");
            drained.push(entry);
        }
        if !drained.is_empty() {
            self.in_flight
                .entry((slot, index))
                .or_default()
                .extend(drained);
        }
        batch.finish(sealed_at_ns) // demo(tx-timeline)
    }

    // the slot finalized: txs of committed lanes are done, the rest are
    // re-queued ahead of everything, over the bounds they were admitted under
    pub fn settle(&mut self, slot: Slot, committed: impl Fn(ProposalIndex) -> bool) {
        if slot < self.floor {
            return;
        }
        self.settled.insert(slot);
        let lanes: Vec<_> = self
            .in_flight
            .range((slot, 0)..=(slot, ProposalIndex::MAX))
            .map(|(key, _)| *key)
            .collect();
        let mut requeue = Vec::new();
        for key in lanes {
            let entries = self.in_flight.remove(&key).expect("listed above");
            if committed(key.1) {
                for entry in entries {
                    self.pending.remove(&entry.hash);
                    self.remember(entry.hash);
                }
            } else {
                requeue.extend(entries);
            }
        }
        for entry in requeue.into_iter().rev() {
            self.queued_bytes += entry.size;
            self.queue.push_front(entry);
        }
    }

    // every slot below cap is closed. An unsettled drain below it was
    // skipped by a cap jump and may have committed elsewhere, so it is
    // released rather than re-queued, and a re-send is accepted again.
    pub fn expire_below(&mut self, cap: Slot) {
        if cap <= self.floor {
            return;
        }
        self.floor = cap;
        self.settled = self.settled.split_off(&cap);
        let kept = self.in_flight.split_off(&(cap, 0));
        for entry in std::mem::replace(&mut self.in_flight, kept)
            .into_values()
            .flatten()
        {
            self.pending.remove(&entry.hash);
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.values().map(Vec::len).sum()
    }

    fn remember(&mut self, hash: Hash) {
        if self.config.recent_txs == 0 {
            return;
        }
        if self.recent.len() >= self.config.recent_txs {
            let oldest = self.recent.pop_front().expect("non-empty");
            self.recent_set.remove(&oldest);
        }
        self.recent.push_back(hash);
        self.recent_set.insert(hash);
    }
}

// admitted to by the runtime, drained by the proposing component
#[derive(Clone)]
pub struct SharedMempool(Arc<Mutex<Mempool>>);

impl SharedMempool {
    pub fn new(mempool: Mempool) -> Self {
        Self(Arc::new(Mutex::new(mempool)))
    }

    pub fn lock(&self) -> MutexGuard<'_, Mempool> {
        self.0.lock().expect("mempool lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::ledger::{decode_batch, encode_batch}; // demo(tx-timeline)

    use super::*;

    fn tx(nonce: u64, payload_len: usize) -> Tx {
        Tx {
            sender: [7; 20],
            nonce,
            payload: Bytes::from(vec![nonce as u8; payload_len]),
            sent_at_ns: 0,               // demo(tx-timeline)
            rpc_received_at_ns: 0,       // demo(tx-timeline)
            mempool_admitted_at_ns: NOW, // demo(tx-timeline)
        }
    }

    fn mempool(max_txs: usize, max_bytes: usize, recent_txs: usize) -> Mempool {
        Mempool::new(MempoolConfig {
            max_txs,
            max_bytes,
            recent_txs,
        })
    }

    fn nonces(batch: &[u8]) -> Vec<u64> {
        decode_batch(batch)
            .unwrap()
            .iter()
            .map(|tx| tx.nonce)
            .collect()
    }

    const SEAL: u64 = 1_700_000_000_000_000_000; // demo(tx-timeline)
    const NOW: u64 = SEAL - 1; // demo(tx-timeline)

    // demo(tx-timeline): the smallest limit that holds `n` txs of `payload_len`, header included
    fn limit_for(n: usize, payload_len: usize) -> usize {
        let mut batch = BatchBuilder::new(usize::MAX);
        (0..n).for_each(|_| batch.try_push(tx(0, payload_len)).unwrap());
        batch.encoded_size()
    }

    #[test]
    fn admission_rejects_duplicates_invalid_and_over_bound() {
        let mut pool = mempool(2, 1 << 20, 16);
        assert_eq!(pool.admit(tx(1, 10), NOW), Ok(()));
        assert_eq!(pool.admit(tx(1, 10), NOW), Err(Reject::Duplicate));
        assert!(matches!(
            pool.admit(tx(9, MAX_TX_PAYLOAD + 1), NOW),
            Err(Reject::Invalid(TxError::PayloadTooLarge { .. }))
        ));
        assert_eq!(pool.admit(tx(2, 10), NOW), Ok(()));
        assert_eq!(pool.admit(tx(3, 10), NOW), Err(Reject::Full));
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.queued_bytes(), tx(1, 10).length() * 2);
    }

    // demo(tx-timeline): admission overwrites the stamp, and the bounds count the stamped tx
    #[test]
    fn admission_stamps_admitted_at() {
        let unstamped = Tx {
            mempool_admitted_at_ns: 0,
            ..tx(1, 10)
        };
        let stamped_len = tx(1, 10).length();
        let mut pool = mempool(100, stamped_len - 1, 16);
        assert_eq!(pool.admit(unstamped.clone(), NOW), Err(Reject::Full));
        let mut pool = mempool(100, stamped_len, 16);
        assert_eq!(pool.admit(unstamped, NOW), Ok(()));
        assert_eq!(pool.queued_bytes(), tx(1, 10).length());
        let batch = pool.drain(Slot(0), 0, 1 << 20, SEAL);
        assert_eq!(decode_batch(&batch).unwrap(), [tx(1, 10)]);
    }

    #[test]
    fn the_byte_bound_counts_encoded_txs() {
        let size = tx(1, 100).length();
        let mut pool = mempool(100, size * 2, 16);
        pool.admit(tx(1, 100), NOW).unwrap();
        pool.admit(tx(2, 100), NOW).unwrap();
        assert_eq!(pool.admit(tx(3, 1), NOW), Err(Reject::Full));
        pool.drain(Slot(0), 0, 1 << 20, SEAL); // demo(tx-timeline)
        assert_eq!(pool.queued_bytes(), 0);
        assert_eq!(pool.admit(tx(3, 1), NOW), Ok(()));
    }

    #[test]
    fn a_drain_is_fifo_and_fits_the_proposal_size_limit() {
        let mut pool = mempool(100, 1 << 20, 16);
        for nonce in 0..10 {
            pool.admit(tx(nonce, 100), NOW).unwrap();
        }
        let proposal_size_limit = limit_for(3, 100); // demo(tx-timeline)
        let batch = pool.drain(Slot(0), 0, proposal_size_limit, SEAL); // demo(tx-timeline)
        assert!(batch.len() <= proposal_size_limit);
        assert_eq!(nonces(&batch), [0, 1, 2]);
        assert_eq!(
            nonces(&pool.drain(Slot(0), 1, proposal_size_limit, SEAL)), // demo(tx-timeline)
            [3, 4, 5]
        );
        assert_eq!(pool.len(), 4);
        assert_eq!(pool.in_flight(), 6);
    }

    #[test]
    fn an_empty_pool_drains_the_empty_list() {
        let mut pool = mempool(1, 1 << 20, 16);
        assert_eq!(pool.drain(Slot(0), 0, 1024, SEAL), encode_batch(SEAL, &[])); // demo(tx-timeline)
        assert_eq!(pool.in_flight(), 0);
    }

    #[test]
    fn a_head_that_does_not_fit_blocks_the_drain() {
        let mut pool = mempool(100, 1 << 20, 16);
        pool.admit(tx(0, 500), NOW).unwrap();
        pool.admit(tx(1, 1), NOW).unwrap();
        assert_eq!(
            nonces(&pool.drain(Slot(0), 0, 100, SEAL)),
            Vec::<u64>::new()
        ); // demo(tx-timeline)
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn in_flight_and_committed_txs_are_duplicates() {
        let mut pool = mempool(100, 1 << 20, 16);
        pool.admit(tx(1, 10), NOW).unwrap();
        pool.drain(Slot(3), 0, 1 << 20, SEAL); // demo(tx-timeline)
        assert_eq!(pool.admit(tx(1, 10), NOW), Err(Reject::Duplicate));
        pool.settle(Slot(3), |_| true);
        assert_eq!(pool.in_flight(), 0);
        assert_eq!(pool.admit(tx(1, 10), NOW), Err(Reject::Duplicate));
    }

    #[test]
    fn the_recent_set_is_bounded() {
        let mut pool = mempool(100, 1 << 20, 2);
        for nonce in 0..3 {
            pool.admit(tx(nonce, 10), NOW).unwrap();
        }
        pool.drain(Slot(0), 0, 1 << 20, SEAL); // demo(tx-timeline)
        pool.settle(Slot(0), |_| true);
        // the oldest committed hash was evicted
        assert_eq!(pool.admit(tx(0, 10), NOW), Ok(()));
        assert_eq!(pool.admit(tx(1, 10), NOW), Err(Reject::Duplicate));
        assert_eq!(pool.admit(tx(2, 10), NOW), Err(Reject::Duplicate));
    }

    #[test]
    fn uncommitted_lanes_are_requeued_at_the_head_in_order() {
        let mut pool = mempool(100, 1 << 20, 16);
        for nonce in 0..6 {
            pool.admit(tx(nonce, 10), NOW).unwrap();
        }
        let two = limit_for(2, 10); // demo(tx-timeline)
        pool.drain(Slot(5), 0, two, SEAL); // demo(tx-timeline)
        pool.drain(Slot(5), 1, two, SEAL); // demo(tx-timeline)
        pool.settle(Slot(5), |index| index == 1);
        assert_eq!(pool.len(), 4);
        assert_eq!(pool.in_flight(), 0);
        assert_eq!(nonces(&pool.drain(Slot(6), 0, 1 << 20, SEAL)), [0, 1, 4, 5]); // demo(tx-timeline)
        // the committed lane stays known
        assert_eq!(pool.admit(tx(2, 10), NOW), Err(Reject::Duplicate));
    }

    #[test]
    fn settling_touches_only_its_own_slot() {
        let mut pool = mempool(100, 1 << 20, 16);
        pool.admit(tx(0, 10), NOW).unwrap();
        pool.admit(tx(1, 10), NOW).unwrap();
        let one = limit_for(1, 10); // demo(tx-timeline)
        pool.drain(Slot(5), 0, one, SEAL); // demo(tx-timeline)
        pool.drain(Slot(6), 0, one, SEAL); // demo(tx-timeline)
        pool.settle(Slot(5), |_| false);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.in_flight(), 1);
    }

    #[test]
    fn a_settled_or_closed_slot_drains_nothing() {
        let mut pool = mempool(100, 1 << 20, 16);
        pool.admit(tx(0, 10), NOW).unwrap();
        pool.settle(Slot(4), |_| false);
        let empty = encode_batch(SEAL, &[]); // demo(tx-timeline)
        assert_eq!(pool.drain(Slot(4), 0, 1 << 20, SEAL), empty); // demo(tx-timeline)
        pool.expire_below(Slot(10));
        assert_eq!(pool.drain(Slot(9), 0, 1 << 20, SEAL), empty); // demo(tx-timeline)
        assert_eq!(pool.len(), 1);
        assert_eq!(nonces(&pool.drain(Slot(10), 0, 1 << 20, SEAL)), [0]); // demo(tx-timeline)
    }

    #[test]
    fn a_cap_jump_releases_unsettled_drains() {
        let mut pool = mempool(100, 1 << 20, 16);
        pool.admit(tx(0, 10), NOW).unwrap();
        pool.admit(tx(1, 10), NOW).unwrap();
        let one = limit_for(1, 10); // demo(tx-timeline)
        pool.drain(Slot(3), 0, one, SEAL); // demo(tx-timeline)
        pool.drain(Slot(8), 0, one, SEAL); // demo(tx-timeline)
        pool.expire_below(Slot(5));
        assert_eq!(pool.in_flight(), 1);
        assert_eq!(pool.len(), 0);
        // released: a re-send is admitted again, the later drain is still known
        assert_eq!(pool.admit(tx(0, 10), NOW), Ok(()));
        assert_eq!(pool.admit(tx(1, 10), NOW), Err(Reject::Duplicate));
        // a late settle below the floor is ignored
        pool.settle(Slot(3), |_| false);
        assert_eq!(pool.len(), 1);
    }
}
