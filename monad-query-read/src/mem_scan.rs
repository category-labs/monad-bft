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

//! In-memory scanning of unfinalized blocks (above the published head),
//! producing the exact rows and filter semantics the indexed path would, so
//! callers can merge tip results with indexed results. `log_index` is assigned
//! block-globally in transaction order, matching ingest (`logs::ingest`).

use alloy_primitives::{Address, Bytes, Log};
use monad_query_engine::clause::IndexedFilter;
use monad_query_primitives::Hash32;

use crate::{
    logs::LogEntry,
    txs::{StoredTxEnvelope, TxEntry},
    LogFilter, TxFilter,
};

/// One unfinalized block's logs; index `i` holds the logs of transaction `i`.
/// Mirrors the per-tx grouping (`FinalizedBlock`) used at ingest so
/// `log_index` is assigned identically.
pub struct MemLogsBlock<'a> {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub logs_by_tx: &'a [Vec<Log>],
}

/// One unfinalized transaction for in-memory scanning. `sender` is
/// caller-authoritative (not recovered from `signed_tx_bytes`), matching
/// the ingest contract; `to`/`selector` filters decode `signed_tx_bytes`.
pub struct MemTx {
    pub tx_hash: Hash32,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

/// Builds the matching [`LogEntry`] rows for one in-memory block, in block
/// order (ascending `log_index`); the caller handles query-order reversal and
/// limits across blocks.
pub fn scan_block_logs(block: &MemLogsBlock<'_>, filter: &LogFilter) -> Vec<LogEntry> {
    let mut matches = Vec::new();
    let mut log_index: u32 = 0;

    for (tx_index, tx_logs) in block.logs_by_tx.iter().enumerate() {
        let tx_index = tx_index as u32;
        for log in tx_logs {
            let entry = LogEntry {
                block_number: block.block_number,
                block_hash: block.block_hash,
                tx_index,
                log_index,
                address: log.address,
                topics: log.data.topics().to_vec(),
                data: log.data.data.clone(),
            };
            log_index += 1;
            if filter.matches(&entry) {
                matches.push(entry);
            }
        }
    }

    matches
}

/// Builds the matching [`TxEntry`] rows for one in-memory block, in block
/// order (ascending `tx_index`); the caller handles query-order reversal and
/// limits across blocks.
pub fn scan_block_txs(
    block_number: u64,
    block_hash: Hash32,
    txs: &[MemTx],
    filter: &TxFilter,
) -> Vec<TxEntry> {
    txs.iter()
        .enumerate()
        .filter_map(|(tx_index, tx)| {
            // Undecodable envelopes cannot exist in finalized blocks (ingest
            // validates them); in-memory blocks drop such rows rather than
            // failing the scan.
            let entry = StoredTxEnvelope {
                tx_hash: tx.tx_hash,
                sender: tx.sender,
                signed_tx_bytes: tx.signed_tx_bytes.clone(),
            }
            .into_tx_entry(block_number, block_hash, tx_index as u32)
            .ok()?;
            filter.matches(&entry).then_some(entry)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, LogData, B256};

    use super::*;

    fn log(address: Address, topic: B256) -> Log {
        Log {
            address,
            data: LogData::new_unchecked(vec![topic], Bytes::from_static(&[1, 2, 3])),
        }
    }

    #[test]
    fn scan_block_logs_assigns_block_global_log_index_and_filters() {
        let a = Address::from([0xaa; 20]);
        let b = Address::from([0xbb; 20]);
        let t0 = B256::from([0x11; 32]);
        let t1 = B256::from([0x22; 32]);

        let logs_by_tx = vec![vec![log(a, t0), log(b, t1)], vec![log(a, t1)]];
        let block = MemLogsBlock {
            block_number: 7,
            block_hash: B256::from([0x99; 32]),
            logs_by_tx: &logs_by_tx,
        };

        let all = scan_block_logs(&block, &LogFilter::default());
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.iter()
                .map(|l| (l.tx_index, l.log_index))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1), (1, 2)]
        );

        let filter = LogFilter {
            address: Some([a].into_iter().collect()),
            ..Default::default()
        };
        let only_a = scan_block_logs(&block, &filter);
        assert_eq!(
            only_a.iter().map(|l| l.log_index).collect::<Vec<_>>(),
            vec![0, 2]
        );

        let filter = LogFilter {
            address: Some([a].into_iter().collect()),
            topics: [Some([t1].into_iter().collect()), None, None, None],
        };
        let narrowed = scan_block_logs(&block, &filter);
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].log_index, 2);
        assert_eq!(narrowed[0].block_number, 7);
    }

    fn signed_tx_bytes(nonce: u64) -> Bytes {
        use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_primitives::{Signature, TxKind, U256};

        let signed = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 10,
            gas_limit: 21_000,
            to: TxKind::Call(Address::repeat_byte(0xee)),
            value: U256::from(1u64),
            input: Bytes::new(),
        }
        .into_signed(Signature::test_signature());
        let mut buf = Vec::new();
        TxEnvelope::Legacy(signed).encode_2718(&mut buf);
        buf.into()
    }

    #[test]
    fn scan_block_txs_filters_by_sender() {
        let sender_a = Address::from([0x01; 20]);
        let sender_b = Address::from([0x02; 20]);
        let txs = vec![
            MemTx {
                tx_hash: B256::from([0x10; 32]),
                sender: sender_a,
                signed_tx_bytes: signed_tx_bytes(0),
            },
            MemTx {
                tx_hash: B256::from([0x20; 32]),
                sender: sender_b,
                signed_tx_bytes: signed_tx_bytes(1),
            },
        ];

        let filter = TxFilter {
            from: Some([sender_b].into_iter().collect()),
            ..Default::default()
        };
        let matched = scan_block_txs(3, B256::from([0x77; 32]), &txs, &filter);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].tx_index, 1);
        assert_eq!(matched[0].sender, sender_b);
        assert_eq!(matched[0].block_number, 3);
    }
}
