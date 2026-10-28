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

use std::collections::VecDeque;

use bytes::Bytes;
use monad_mcp_chorus::ledger::Tx;
use monad_mcp_common::{Lane, Lanes, Proposal, Slot, Timestamp};
use rand_chacha::ChaCha8Rng;

use super::{Component, Mempool};
use crate::{
    chorus::proposing::ProposalPlanner, common::NodeId, config::ProposalConfig, epoch::EpochHandle,
};

pub enum ProposingInput {
    SlotOpen(Slot, Timestamp),
    // every slot strictly below cap is finalized
    CapAdvance(Slot),
    // demo(tx-timeline): `at` stamps mempool admission
    Tx { at: Timestamp, from: NodeId, tx: Tx },
    // the lanes the slot's finalization commits
    Finalized(Slot, Lanes<bool>),
}

pub type ProposingOutput = Proposal<Bytes>;

// any proposing component; the node holds one and never knows which
pub type Proposing = Box<dyn Component<Input = ProposingInput, Output = ProposingOutput> + Send>;

impl Component for Proposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        (**self).handle(now, input);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        (**self).poll()
    }

    fn next_due(&self) -> Option<Timestamp> {
        (**self).next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        (**self).handle_due(now);
    }
}

// decides when we propose in a slot and proposes the mempool's head
pub struct MempoolProposing {
    planner: ProposalPlanner,
    mempool: Mempool,
    proposal_size_limit: usize,
    outbox: VecDeque<ProposingOutput>,
}

impl MempoolProposing {
    pub fn new(epoch_handle: &EpochHandle, config: &ProposalConfig, mempool: Mempool) -> Self {
        let planner = ProposalPlanner::new(
            epoch_handle.self_id,
            epoch_handle.proposers.clone(),
            config.planner(&epoch_handle.proposers),
        );
        Self {
            planner,
            mempool,
            proposal_size_limit: config.proposal_size_limit(),
            outbox: VecDeque::new(),
        }
    }

    // demo(tx-timeline): `at` is the node clock, stamped as `mempool_admitted_at_ns`
    fn admit(&mut self, tx: Tx, from: NodeId, at: Timestamp) {
        let admitted_at_ns = at.as_nanos() as u64;
        match self.mempool.admit(tx, admitted_at_ns) {
            Ok(()) => tracing::debug!(?from, "tx admitted"),
            Err(reject) => tracing::debug!(?from, %reject, "tx dropped"),
        }
    }

    fn collect(&mut self, now: Timestamp) {
        while let Some((slot, lane)) = self.planner.poll(now) {
            let sealed_at_ns = u64::try_from(now.as_nanos()).unwrap_or(0); // demo(tx-timeline)
            let payload = self
                .mempool
                .drain(slot, lane, self.proposal_size_limit, sealed_at_ns);
            tracing::debug!(
                slot = slot.0,
                lane = usize::from(lane),
                len = payload.len(),
                "drained mempool"
            );
            let proposal = Proposal {
                slot,
                lane,
                payload,
            };
            self.outbox.push_back(proposal);
        }
    }
}

impl Component for MempoolProposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        match input {
            ProposingInput::SlotOpen(slot, deadline) => {
                self.planner.handle_slot_open(now, slot, deadline);
            }
            ProposingInput::CapAdvance(cap) => {
                self.planner.handle_cap_advance(now, cap);
                self.mempool.expire_below(cap);
            }
            ProposingInput::Tx { at, from, tx } => self.admit(tx, from, at),
            ProposingInput::Finalized(slot, committed) => {
                let is_committed = |lane: Lane| committed.get(usize::from(lane)) == Some(&true);
                self.mempool.settle(slot, is_committed);
            }
        }
        self.collect(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.planner.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.collect(now);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        self.outbox.pop_front()
    }
}

// proposes random bytes when the schedule says so, for load tests without
// clients and for scenarios that do not care what a proposal carries
pub struct RandomProposing {
    planner: ProposalPlanner,
    proposal_size_limit: usize,
    rng: ChaCha8Rng,
    outbox: VecDeque<ProposingOutput>,
}

impl RandomProposing {
    pub fn new(epoch_handle: &EpochHandle, config: &ProposalConfig, rng: ChaCha8Rng) -> Self {
        let planner = ProposalPlanner::new(
            epoch_handle.self_id,
            epoch_handle.proposers.clone(),
            config.planner(&epoch_handle.proposers),
        );
        Self {
            planner,
            proposal_size_limit: config.proposal_size_limit(),
            rng,
            outbox: VecDeque::new(),
        }
    }

    fn collect(&mut self, now: Timestamp) {
        while let Some((slot, lane)) = self.planner.poll(now) {
            let payload = random_payload(&mut self.rng, self.proposal_size_limit);
            let proposal = Proposal {
                slot,
                lane,
                payload,
            };
            self.outbox.push_back(proposal);
        }
    }
}

impl Component for RandomProposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        match input {
            ProposingInput::SlotOpen(slot, deadline) => {
                self.planner.handle_slot_open(now, slot, deadline);
            }
            ProposingInput::CapAdvance(cap) => self.planner.handle_cap_advance(now, cap),
            ProposingInput::Tx { from, .. } => tracing::debug!(?from, "no mempool, tx dropped"),
            ProposingInput::Finalized(..) => {}
        }
        self.collect(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.planner.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.collect(now);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        self.outbox.pop_front()
    }
}

// never empty, log-uniform in length: every octave [2^k, 2^(k+1)) from 1 byte
// up to the proposal size limit is equally likely, uniform within it. Integer
// only: libm's log2/powf differ in the last bit across platforms, which would
// make a seeded simulation diverge between, e.g., arm and x86.
fn random_payload(rng: &mut impl rand::Rng, proposal_size_limit: usize) -> Bytes {
    let limit = proposal_size_limit.max(1) as u64;
    let max_bits = u64::BITS - 1 - limit.leading_zeros();
    let low = 1u64 << rng.gen_range(0..=max_bits);
    let len = rng.gen_range(low..=(2 * low - 1).min(limit));
    let mut message = vec![0u8; len as usize];
    rng.fill_bytes(&mut message);
    Bytes::from(message)
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::ledger::{MAX_TX_PAYLOAD, decode_batch, decode_sealed_batch}; // demo(tx-timeline)

    use super::*;
    use crate::config::NodeConfig;

    fn tx(nonce: u64) -> Tx {
        Tx {
            sender: [5; 20],
            nonce,
            payload: Bytes::from_static(b"demux"),
            sent_at_ns: 0,             // demo(tx-timeline)
            rpc_received_at_ns: 0,     // demo(tx-timeline)
            mempool_admitted_at_ns: 0, // demo(tx-timeline)
        }
    }

    fn config() -> NodeConfig {
        NodeConfig::single_node(0, Timestamp::from_millis(10_000), "unused-ledger")
    }

    fn proposing_of(config: &NodeConfig) -> MempoolProposing {
        let epoch_handle = config.epoch_handle().unwrap();
        let mempool = Mempool::new(config.mempool);
        MempoolProposing::new(&epoch_handle, &config.proposal, mempool)
    }

    fn arrive(proposing: &mut MempoolProposing, tx: Tx, at: Timestamp) {
        let from = NodeId::dummy(0);
        proposing.handle(at, ProposingInput::Tx { at, from, tx });
    }

    fn lane() -> Lane {
        Lane::new_unchecked(0)
    }

    #[test]
    fn a_tx_is_admitted_once_and_stamped_with_its_arrival() {
        let mut proposing = proposing_of(&config());
        let at = Timestamp::from_millis(1_000); // demo(tx-timeline)
        arrive(&mut proposing, tx(1), at);
        arrive(&mut proposing, tx(1), at);
        arrive(&mut proposing, tx(2), at);
        assert_eq!(proposing.mempool.len(), 2);

        let batch = proposing.mempool.drain(Slot(0), lane(), 1 << 20, 0);
        let admitted = decode_batch(&batch).unwrap();
        // demo(tx-timeline): admission stamps the arrival time
        let at_ns = at.as_nanos() as u64;
        assert!(admitted.iter().all(|tx| tx.mempool_admitted_at_ns == at_ns));
    }

    #[test]
    fn an_oversized_tx_is_dropped() {
        let mut proposing = proposing_of(&config());
        let oversized = Tx {
            payload: Bytes::from(vec![0; MAX_TX_PAYLOAD + 1]),
            ..tx(1)
        };
        arrive(&mut proposing, oversized, Timestamp::from_millis(1_000));
        assert!(proposing.mempool.is_empty());
    }

    #[test]
    fn a_full_mempool_drops_the_overflow() {
        let mut config = config();
        config.mempool.max_txs = 1;
        let mut proposing = proposing_of(&config);
        for nonce in 1..=3 {
            arrive(&mut proposing, tx(nonce), Timestamp::from_millis(1_000));
        }
        assert_eq!(proposing.mempool.len(), 1);
    }

    #[test]
    fn a_finalized_slot_settles_its_lanes() {
        let mut proposing = proposing_of(&config());
        let now = Timestamp::from_millis(1_000);
        arrive(&mut proposing, tx(1), now);
        proposing.mempool.drain(Slot(3), lane(), 1 << 20, 0);
        arrive(&mut proposing, tx(2), now);
        let left_out = Lane::new_unchecked(1);
        proposing.mempool.drain(Slot(3), left_out, 1 << 20, 0);
        assert_eq!(proposing.mempool.in_flight(), 2);

        let committed = Lanes::new(2, |j| j != left_out);
        proposing.handle(now, ProposingInput::Finalized(Slot(3), committed));
        assert_eq!(proposing.mempool.in_flight(), 0);
        assert_eq!(proposing.mempool.len(), 1);
    }

    #[test]
    fn a_cap_advance_releases_unsettled_drains() {
        let mut proposing = proposing_of(&config());
        let now = Timestamp::from_millis(1_000);
        arrive(&mut proposing, tx(1), now);
        proposing.mempool.drain(Slot(3), lane(), 1 << 20, 0);
        assert_eq!(proposing.mempool.in_flight(), 1);

        proposing.handle(now, ProposingInput::CapAdvance(Slot(4)));
        assert_eq!(proposing.mempool.in_flight(), 0);
    }

    // demo(tx-timeline)
    #[test]
    fn a_proposal_is_sealed_at_the_time_it_is_released() {
        let mut proposing = proposing_of(&config());
        let deadline = Timestamp::from_millis(10_000);
        proposing.handle(
            Timestamp::from_millis(1_000),
            ProposingInput::SlotOpen(Slot(0), deadline),
        );
        let due = proposing.next_due().expect("a seal is due");
        proposing.handle_due(due);

        let Some(proposal) = proposing.poll() else {
            panic!("a proposal at the seal time");
        };
        assert_eq!(proposal.slot, Slot(0));
        let sealed_at_ns = due.as_nanos() as u64;
        assert_eq!(
            decode_sealed_batch(&proposal.payload).unwrap(),
            (sealed_at_ns, vec![])
        );
    }

    // pinned: integer sampling and ChaCha8 are the same on every platform, so a
    // seeded run proposes the same lengths on arm and x86
    #[test]
    fn random_payload_lengths_are_fixed_by_the_seed() {
        use rand::SeedableRng as _;

        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let lengths: Vec<usize> = (0..8)
            .map(|_| random_payload(&mut rng, 1 << 20).len())
            .collect();
        assert_eq!(lengths, PINNED_LENGTHS);
    }

    const PINNED_LENGTHS: [usize; 8] = [9, 52470, 32166, 12, 6184, 391, 4996, 43471];

    #[test]
    fn a_random_payload_is_non_empty_and_within_the_proposal_size_limit() {
        use rand::SeedableRng as _;

        let mut rng = ChaCha8Rng::seed_from_u64(7);
        for proposal_size_limit in [0, 1, 7, 1024, 1 << 20] {
            for _ in 0..64 {
                let message = random_payload(&mut rng, proposal_size_limit);
                assert!(!message.is_empty());
                assert!(message.len() <= proposal_size_limit.max(1));
            }
        }
    }
}
