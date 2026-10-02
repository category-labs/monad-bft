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

use bytes::Bytes;
use rand::{Rng as _, RngCore as _};

use super::SharedMempool;
use crate::chorus::types::{ProposalIndex, Slot, Timestamp}; // demo(tx-timeline)

// what a sealed proposal carries
pub trait ProposalSource {
    // at most `proposal_size_limit` bytes, and never empty
    fn next_payload(
        &mut self,
        now: Timestamp, // demo(tx-timeline)
        slot: Slot,
        index: ProposalIndex,
        proposal_size_limit: usize,
    ) -> Bytes;
}

// random bytes up to the proposal size limit, log-uniform in length: every doubling
// from 1 byte to the proposal size limit is equally likely
pub struct RandomSource;

impl ProposalSource for RandomSource {
    fn next_payload(
        &mut self,
        _now: Timestamp, // demo(tx-timeline)
        _slot: Slot,
        _index: ProposalIndex,
        proposal_size_limit: usize,
    ) -> Bytes {
        let proposal_size_limit = proposal_size_limit.max(1);
        let mut rng = rand::thread_rng();
        let max_bits = (proposal_size_limit as f64).log2();
        let len = 2f64.powf(rng.gen_range(0.0..=max_bits)).round() as usize;
        let mut message = vec![0u8; len.clamp(1, proposal_size_limit)];
        rng.fill_bytes(&mut message);
        Bytes::from(message)
    }
}

// demo(tx-timeline): the mempool's head as `rlp[sealed_at_ns, Vec<Tx>]` stamped with `now`;
// an empty mempool yields an empty tx list
pub struct MempoolSource(pub SharedMempool);

impl ProposalSource for MempoolSource {
    fn next_payload(
        &mut self,
        now: Timestamp, // demo(tx-timeline)
        slot: Slot,
        index: ProposalIndex,
        proposal_size_limit: usize,
    ) -> Bytes {
        let sealed_at_ns = u64::try_from(now.as_nanos()).unwrap_or(0); // demo(tx-timeline)
        let payload = self
            .0
            .lock()
            .drain(slot, index, proposal_size_limit, sealed_at_ns); // demo(tx-timeline)
        tracing::debug!(slot = slot.0, index, len = payload.len(), "drained mempool");
        payload
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::ledger::{Tx, decode_batch, decode_sealed_batch}; // demo(tx-timeline)

    use super::*;
    use crate::{component::Mempool, config::MempoolConfig};

    const NOW: Timestamp = Timestamp::from_millis(1_234); // demo(tx-timeline)

    #[test]
    fn a_random_payload_is_non_empty_and_within_the_proposal_size_limit() {
        for proposal_size_limit in [0, 1, 7, 1024, 1 << 20] {
            for slot in 0..64 {
                let message = RandomSource.next_payload(NOW, Slot(slot), 0, proposal_size_limit); // demo(tx-timeline)
                assert!(!message.is_empty());
                assert!(message.len() <= proposal_size_limit.max(1));
            }
        }
    }

    #[test]
    fn a_mempool_payload_decodes_and_fits_the_proposal_size_limit() {
        let mempool = SharedMempool::new(Mempool::new(MempoolConfig::default()));
        let mut source = MempoolSource(mempool.clone());
        // demo(tx-timeline): the seal time is the proposing clock's `now`
        let empty = source.next_payload(NOW, Slot(0), 0, 64);
        assert_eq!(
            decode_sealed_batch(&empty).unwrap(),
            (1_234_000_000, vec![])
        );

        for nonce in 0..100 {
            let tx = Tx {
                sender: [1; 20],
                nonce,
                payload: Bytes::from(vec![0xab; 200]),
                received_at_ns: 0, // demo(tx-timeline)
            };
            mempool.lock().admit(tx).unwrap();
        }
        let proposal_size_limit = 4096;
        let mut seen = 0;
        for slot in 1.. {
            let payload = source.next_payload(NOW, Slot(slot), 0, proposal_size_limit); // demo(tx-timeline)
            assert!(payload.len() <= proposal_size_limit);
            let txs = decode_batch(&payload).unwrap();
            if txs.is_empty() {
                break;
            }
            for tx in txs {
                assert_eq!(tx.nonce, seen);
                seen += 1;
            }
        }
        assert_eq!(seen, 100);
        assert!(mempool.lock().is_empty());
    }
}
