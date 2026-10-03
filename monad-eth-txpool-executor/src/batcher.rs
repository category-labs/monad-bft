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

use std::collections::{BTreeMap, VecDeque};

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::Address;
use alloy_rlp::{Encodable, Header};
use bytes::Bytes;
use indexmap::IndexMap;
use monad_types::{LimitedVec, MAX_FORWARDED_TXS_PER_MESSAGE};
use tracing::error;

pub(crate) type Batch = LimitedVec<Bytes, MAX_FORWARDED_TXS_PER_MESSAGE>;

/// groups each input list by sender and nonce, rotating groups after each batch.
/// groups from separate input lists remain independent.
pub(crate) struct Batcher {
    txs: VecDeque<BTreeMap<u64, Bytes>>,
    target_bytes: usize,
}

impl Batcher {
    pub(crate) fn new(target_bytes: usize) -> Self {
        assert!(target_bytes > 0, "batch target must be positive");
        Self {
            txs: VecDeque::new(),
            target_bytes,
        }
    }

    pub(crate) fn extend<'a>(&mut self, txs: impl IntoIterator<Item = &'a Recovered<TxEnvelope>>) {
        let mut groups = IndexMap::<Address, BTreeMap<u64, Bytes>>::new();
        for tx in txs {
            groups
                .entry(tx.signer())
                .or_default()
                .insert(tx.nonce(), tx.encoded_2718().into());
        }
        self.txs.extend(groups.into_values());
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    /// yields one sender per batch in increasing nonce order within each input list.
    /// the target includes rlp framing of the transaction list, excluding outer network envelopes.
    /// a valid transaction exceeding the target is emitted alone.
    pub(crate) fn batches(&mut self, max_tx_bytes: usize) -> impl Iterator<Item = Batch> + '_ {
        std::iter::from_fn(move || self.next_batch(max_tx_bytes))
    }

    fn next_batch(&mut self, max_tx_bytes: usize) -> Option<Batch> {
        while let Some(mut pending) = self.txs.pop_front() {
            let mut batch = Batch::default();
            let mut payload_length = 0;
            let target_bytes = self.target_bytes.min(max_tx_bytes);

            while batch.len() < MAX_FORWARDED_TXS_PER_MESSAGE {
                let Some((_, tx)) = pending.first_key_value() else {
                    break;
                };
                if tx.len() > max_tx_bytes {
                    error!("txpool batcher detected tx larger than max tx byte size, skipping forwarding");
                    pending.pop_first();
                    continue;
                }

                let next_payload_length = payload_length + tx.length();
                let next_size = next_payload_length
                    + Header {
                        list: true,
                        payload_length: next_payload_length,
                    }
                    .length();
                if !batch.is_empty() && next_size > target_bytes {
                    break;
                }

                let (_, tx) = pending.pop_first().expect("pending transaction exists");
                batch.try_push(tx).expect("batch count checked above");
                payload_length = next_payload_length;
            }

            if !pending.is_empty() {
                self.txs.push_back(pending);
            }
            if !batch.is_empty() {
                return Some(batch);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
    use alloy_eips::{Decodable2718, Encodable2718};
    use alloy_rlp::Encodable;
    use bytes::Bytes;
    use monad_eth_testutil::{make_legacy_tx, recover_tx, S1, S2};
    use monad_types::MAX_FORWARDED_TXS_PER_MESSAGE;
    use rstest::{fixture, rstest};

    use super::{Batch, Batcher};

    #[fixture]
    fn batcher(#[default(16 * 1024)] target_bytes: usize) -> Batcher {
        Batcher::new(target_bytes)
    }

    fn tx(nonce: u64, input_len: usize) -> Recovered<TxEnvelope> {
        recover_tx(make_legacy_tx(
            S1,
            100_000_000_000,
            30_000_000,
            nonce,
            input_len,
        ))
    }

    fn decode(batch: &Batch) -> Vec<Recovered<TxEnvelope>> {
        batch
            .iter()
            .map(|bytes| recover_tx(TxEnvelope::decode_2718(&mut bytes.as_ref()).unwrap()))
            .collect()
    }

    #[rstest]
    fn groups_senders_and_sorts_nonces(mut batcher: Batcher) {
        let other = recover_tx(make_legacy_tx(S2, 100_000_000_000, 100_000, 0, 0));
        let txs = [tx(2, 0), other.clone(), tx(0, 0), tx(1, 0)];
        batcher.extend(&txs);
        let batches: Vec<_> = batcher.batches(384 * 1024).collect();
        assert_eq!(batches.len(), 2);
        let first = decode(&batches[0]);
        assert_eq!(
            first.iter().map(|tx| tx.nonce()).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(first.iter().all(|tx| tx.signer() == txs[0].signer()));
        assert_eq!(decode(&batches[1]), [other]);
        assert!(batcher.is_empty());
    }

    #[rstest]
    #[case::exact_fit(0, 2)]
    #[case::one_byte_short(1, 1)]
    fn splits_at_framed_byte_limit_and_rotates_senders(
        #[case] below_exact_size: usize,
        #[case] expected_first_len: usize,
    ) {
        let txs = [tx(0, 0), tx(1, 0), tx(2, 0)];
        let other = recover_tx(make_legacy_tx(S2, 100_000_000_000, 100_000, 0, 0));
        let first_two: Vec<Bytes> = txs[..2].iter().map(|tx| tx.encoded_2718().into()).collect();
        let target = first_two.length() - below_exact_size;
        let mut batcher = Batcher::new(target);
        batcher.extend(txs.iter().chain([&other]));
        let batches: Vec<_> = batcher.batches(384 * 1024).collect();
        assert_eq!(batches[0].len(), expected_first_len);
        assert_eq!(decode(&batches[1]), [other]);
        assert!(batches.iter().all(|batch| batch.length() <= target));
        let own: Vec<_> = batches
            .iter()
            .flat_map(decode)
            .filter(|tx| tx.signer() == txs[0].signer())
            .collect();
        assert_eq!(own, txs);
    }

    #[rstest]
    fn sends_large_valid_transaction_alone_and_skips_invalid(#[with(512)] mut batcher: Batcher) {
        let txs = [tx(0, 0), tx(1, 1024), tx(2, 0), tx(3, 2048)];
        let max_tx_bytes = txs[1].eip2718_encoded_length();
        batcher.extend(&txs);
        let batches: Vec<_> = batcher.batches(max_tx_bytes).collect();
        assert_eq!(
            batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
            [1, 1, 1]
        );
        assert_eq!(
            batches.iter().flat_map(decode).collect::<Vec<_>>(),
            txs[..3]
        );
        assert!(batcher.is_empty());
    }

    #[rstest]
    fn keeps_input_lists_independent(mut batcher: Batcher) {
        let old = tx(1, 0);
        let replacement = tx(1, 10);
        batcher.extend([&old]);
        batcher.extend([&replacement, &replacement, &tx(0, 0)]);
        batcher.extend([&old]);
        let batches: Vec<_> = batcher.batches(384 * 1024).collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(decode(&batches[0]), [old.clone()]);
        assert_eq!(decode(&batches[1]), [tx(0, 0), replacement]);
        assert_eq!(decode(&batches[2]), [old]);
        assert!(batcher.is_empty());
    }

    #[rstest]
    fn new_input_does_not_merge_with_partially_drained_group(#[with(1)] mut batcher: Batcher) {
        batcher.extend([&tx(2, 0), &tx(1, 0)]);
        assert_eq!(
            decode(&batcher.batches(384 * 1024).next().unwrap()),
            [tx(1, 0)]
        );

        batcher.extend([&tx(0, 0)]);
        let batches: Vec<_> = batcher.batches(384 * 1024).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(decode(&batches[0]), [tx(2, 0)]);
        assert_eq!(decode(&batches[1]), [tx(0, 0)]);
        assert!(batcher.is_empty());
    }

    #[rstest]
    fn respects_count_limit(#[with(usize::MAX)] mut batcher: Batcher) {
        let txs: Vec<_> = (0..=MAX_FORWARDED_TXS_PER_MESSAGE as u64)
            .map(|nonce| tx(nonce, 0))
            .collect();
        batcher.extend(&txs);
        let batches: Vec<_> = batcher.batches(usize::MAX).collect();
        assert_eq!(
            batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
            [MAX_FORWARDED_TXS_PER_MESSAGE, 1]
        );
        assert_eq!(batches.iter().flat_map(decode).collect::<Vec<_>>(), txs);
    }

    #[rstest]
    #[case::empty(vec![])]
    #[case::all_invalid(vec![tx(0, 1024)])]
    fn empty_and_all_invalid_inputs_do_not_emit_empty_batches(
        #[with(512)] mut batcher: Batcher,
        #[case] txs: Vec<Recovered<TxEnvelope>>,
    ) {
        batcher.extend(&txs);
        assert!(batcher.batches(512).next().is_none());
        assert!(batcher.is_empty());
        batcher.extend([&tx(1, 0)]);
        assert_eq!(decode(&batcher.batches(512).next().unwrap()), [tx(1, 0)]);
    }
}
