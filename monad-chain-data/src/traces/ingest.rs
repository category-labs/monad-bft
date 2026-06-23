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

use alloy_primitives::U256;

use monad_query_types::traces::{selector_from_input, StoredTrace};
use crate::{
    engine::{
        bitmap::{IndexKind, StreamKey},
        digest::ChainDigest,
        row_codec::{digest_block_rows, encode_block_rows, RowCodec},
    },
    error::{MonadChainDataError, Result},
    ingest_types::{CallKind, IngestTrace},
    primitives::records::BlockBlobHeader,
};

/// Compresses a block's trace rows into the framed per-family blob. Frames
/// must already be DFS-flattened with `trace_address` assigned.
pub(crate) fn encode_block_traces(
    traces: &[IngestTrace],
    codec: &RowCodec,
) -> Result<(BlockBlobHeader, Vec<u8>, ChainDigest)> {
    encode_block_rows(traces, codec, "block trace blob too large", |trace| {
        StoredTrace::from(trace).encode()
    })
}

/// The [`encode_block_traces`] row digest alone (external-payload ingest).
pub(crate) fn digest_block_traces(traces: &[IngestTrace]) -> ChainDigest {
    digest_block_rows(traces, |trace| StoredTrace::from(trace).encode())
}

/// Expands one frame into the indexed streams written at ingest:
/// `to`/`selector` skipped when absent, `top_level` only for root frames,
/// `has_transfer` only when the transfer predicate holds.
pub(crate) fn stream_entries_for_trace(trace: &IngestTrace) -> Vec<StreamKey> {
    let mut entries = Vec::with_capacity(5);
    entries.push(StreamKey::new(IndexKind::From, trace.from.as_slice()));

    if let Some(to) = trace.to {
        entries.push(StreamKey::new(IndexKind::To, to.as_slice()));
    }

    if let Some(selector) = selector_from_input(&trace.input) {
        entries.push(StreamKey::new(IndexKind::Selector, &selector));
    }

    if trace.trace_address.is_empty() {
        entries.push(StreamKey::new(IndexKind::TopLevel, &[]));
    }

    if is_transfer_frame(trace) {
        entries.push(StreamKey::new(IndexKind::HasTransfer, &[]));
    }

    entries
}

/// THE definition of the `has_transfer` index bit: every trace frame this
/// predicate accepts gets the bit at ingest, and the transfers view is
/// exactly the frames carrying it (no read-side mirror exists). A frame
/// transfers value iff the call kind moves value (DelegateCall/StaticCall
/// never do), value > 0, the frame succeeded (`status == 0`), and the
/// containing tx committed. Both status checks are required: the tracer
/// does not rewrite descendant statuses when a parent reverts.
pub fn is_transfer_frame(trace: &IngestTrace) -> bool {
    let kind_moves_value = matches!(
        trace.typ,
        CallKind::Call
            | CallKind::CallCode
            | CallKind::Create
            | CallKind::Create2
            | CallKind::SelfDestruct
    );
    trace.value > U256::ZERO && kind_moves_value && trace.status == 0 && trace.tx_status
}

/// Computes the OpenEthereum-style `trace_address` for each frame in a
/// pre-flattened DFS sequence belonging to a single tx. The depth of the
/// first frame must be `0`; deeper frames must have `depth == parent.depth + 1`.
///
/// For a tx with the call tree `root -> A -> A1, A2; root -> B`, the
/// emitted addresses are `[]`, `[0]`, `[0, 0]`, `[0, 1]`, `[1]`.
pub fn compute_trace_addresses<I>(frames: I) -> Result<Vec<Vec<u32>>>
where
    I: IntoIterator<Item = u32>,
{
    // Path components so far; `cursor.len()` == current depth.
    let mut cursor: Vec<u32> = Vec::new();
    // `next_child_slot[d]` is the next sibling index to assign at depth d + 1.
    let mut next_child_slot: Vec<u32> = Vec::new();
    let mut out: Vec<Vec<u32>> = Vec::new();
    let mut seen_root = false;

    for depth in frames {
        let d = depth as usize;
        if d == 0 {
            cursor.clear();
            next_child_slot.clear();
            out.push(Vec::new());
            seen_root = true;
        } else {
            if !seen_root {
                return Err(MonadChainDataError::InvalidBlock(
                    "trace_address: first frame must have depth 0",
                ));
            }
            if d > cursor.len() + 1 {
                return Err(MonadChainDataError::InvalidBlock(
                    "trace_address: depth skipped a level",
                ));
            }
            // Returning to a shallower depth closes deeper subtrees: drop
            // their path components and sibling counters.
            cursor.truncate(d - 1);
            next_child_slot.resize(d, 0);
            let slot_idx = d - 1;
            let slot = next_child_slot[slot_idx];
            next_child_slot[slot_idx] = slot + 1;
            cursor.push(slot);
            out.push(cursor.clone());
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_address_simple_tree() {
        // root -> {A -> {A1, A2}, B -> B1}
        let depths = [0u32, 1, 2, 2, 1, 2];
        let addresses = compute_trace_addresses(depths).expect("compute");
        assert_eq!(
            addresses,
            vec![vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![1, 0],]
        );
    }

    #[test]
    fn trace_address_three_deep() {
        // root -> A -> A1 -> A1a, A1b; root -> B
        let depths = [0u32, 1, 2, 3, 3, 1];
        let addresses = compute_trace_addresses(depths).expect("compute");
        assert_eq!(
            addresses,
            vec![
                vec![],
                vec![0],
                vec![0, 0],
                vec![0, 0, 0],
                vec![0, 0, 1],
                vec![1],
            ]
        );
    }

    #[test]
    fn trace_address_multiple_txs() {
        // Two top-level frames, each its own tx.
        let depths = [0u32, 1, 1, 0, 1, 2];
        let addresses = compute_trace_addresses(depths).expect("compute");
        assert_eq!(
            addresses,
            vec![vec![], vec![0], vec![1], vec![], vec![0], vec![0, 0],]
        );
    }

    #[test]
    fn trace_address_rejects_orphan_depth() {
        let err = compute_trace_addresses([1u32]).unwrap_err();
        assert!(matches!(err, MonadChainDataError::InvalidBlock(_)));
    }

    #[test]
    fn trace_address_rejects_depth_jump() {
        let err = compute_trace_addresses([0u32, 2]).unwrap_err();
        assert!(matches!(err, MonadChainDataError::InvalidBlock(_)));
    }
}
