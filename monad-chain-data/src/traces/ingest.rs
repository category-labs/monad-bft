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

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::U256;

use super::types::{BlockTraceHeader, StoredTrace};
use crate::{
    engine::bitmap::{
        encode_grouped_bitmap_fragments, sharded_stream_id, touched_streams_by_page,
        BitmapFragmentWrite,
    },
    error::{MonadChainDataError, Result},
    family::{CallKind, FinalizedBlock, IngestTrace},
    primitives::state::{FamilyWindowRecord, TraceId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceIngestPlan {
    pub trace_window: FamilyWindowRecord,
    pub block_trace_header: BlockTraceHeader,
    pub block_trace_blob: Vec<u8>,
    pub bitmap_fragments: Vec<BitmapFragmentWrite>,
    pub touched_bitmap_streams_by_page: BTreeMap<u64, BTreeSet<String>>,
    pub written_traces: usize,
}

impl TraceIngestPlan {
    /// Derives the per-block trace artifacts and index fragments for one
    /// finalized block. The caller is responsible for flattening the
    /// per-tx `Vec<Vec<CallFrame>>` into a single DFS-ordered
    /// `Vec<IngestTrace>` and assigning `trace_address` per frame; this
    /// builder only consumes the flattened slice.
    pub fn build(block: &FinalizedBlock, first_trace_id: TraceId) -> Result<Self> {
        let trace_count = u32::try_from(block.traces.len())
            .map_err(|_| MonadChainDataError::Decode("trace count overflow"))?;

        let (block_trace_header, block_trace_blob) = encode_block_traces(&block.traces)?;
        let bitmap_fragments = collect_bitmap_fragments(&block.traces, first_trace_id)?;
        let touched_bitmap_streams_by_page = touched_streams_by_page(&bitmap_fragments)?;
        let trace_window = FamilyWindowRecord {
            first_primary_id: first_trace_id.into(),
            count: trace_count,
        };

        Ok(Self {
            trace_window,
            block_trace_header,
            block_trace_blob,
            bitmap_fragments,
            touched_bitmap_streams_by_page,
            written_traces: block.traces.len(),
        })
    }
}

fn encode_block_traces(traces: &[IngestTrace]) -> Result<(BlockTraceHeader, Vec<u8>)> {
    let mut offsets = Vec::with_capacity(traces.len() + 1);
    let mut blob = Vec::new();

    for trace in traces {
        offsets.push(
            u32::try_from(blob.len())
                .map_err(|_| MonadChainDataError::Decode("block trace blob too large"))?,
        );
        let stored = StoredTrace::from(trace);
        blob.extend_from_slice(&stored.encode());
    }

    offsets.push(
        u32::try_from(blob.len())
            .map_err(|_| MonadChainDataError::Decode("block trace blob too large"))?,
    );

    Ok((BlockTraceHeader { offsets }, blob))
}

fn collect_bitmap_fragments(
    traces: &[IngestTrace],
    first_trace_id: TraceId,
) -> Result<Vec<BitmapFragmentWrite>> {
    let mut stream_values = Vec::new();

    for (ordinal, trace) in traces.iter().enumerate() {
        let ordinal = u64::try_from(ordinal)
            .map_err(|_| MonadChainDataError::Decode("trace ordinal overflow"))?;
        let trace_id = first_trace_id.checked_add(ordinal)?;
        stream_values.extend(stream_entries_for_trace(trace, trace_id));
    }

    encode_grouped_bitmap_fragments(stream_values)
}

/// Expands one frame into the indexed stream entries written at ingest.
/// `to` and `selector` streams are skipped when the field is absent.
/// `top_level` is emitted only for the root frame of each tx.
/// `has_transfer` is emitted only when the transfer predicate holds.
fn stream_entries_for_trace(trace: &IngestTrace, global_trace_id: TraceId) -> Vec<(String, u32)> {
    let shard = global_trace_id.shard();
    let local = global_trace_id.local();

    let mut entries = Vec::with_capacity(5);
    entries.push((
        sharded_stream_id("from", trace.from.as_slice(), shard),
        local,
    ));

    if let Some(to) = trace.to {
        entries.push((sharded_stream_id("to", to.as_slice(), shard), local));
    }

    if trace.input.len() >= 4 {
        entries.push((
            sharded_stream_id("selector", &trace.input[..4], shard),
            local,
        ));
    }

    if trace.trace_address.is_empty() {
        entries.push((sharded_stream_id("top_level", &[], shard), local));
    }

    if is_transfer_frame(trace) {
        entries.push((sharded_stream_id("has_transfer", &[], shard), local));
    }

    entries
}

/// Native-transfer predicate. A frame qualifies for the `has_transfer`
/// indexed clause when:
/// - The call kind moves value (Call/CallCode/Create/Create2/SelfDestruct;
///   DelegateCall and StaticCall never move value, so they are excluded
///   even when their `value` field is set on the parent).
/// - The transferred value is strictly positive.
/// - The frame itself succeeded (`status == 0`, EVMC_SUCCESS).
/// - The top-level tx that contains the frame committed (`tx_status`).
///
/// Both status checks are required: a successful sub-call inside a
/// reverted parent tx keeps `frame.status == 0` because the tracer does
/// not rewrite descendants when the parent reverts.
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
    // `cursor[d]` is the path component chosen at depth d so far; its
    // length always equals the current depth (0 means we're at root).
    let mut cursor: Vec<u32> = Vec::new();
    // `next_child_slot[d]` is the next sibling index to assign at depth
    // `d + 1`. Length grows monotonically with the maximum observed depth.
    let mut next_child_slot: Vec<u32> = Vec::new();
    let mut out: Vec<Vec<u32>> = Vec::new();
    let mut seen_root = false;

    for depth in frames {
        let d = depth as usize;
        if d == 0 {
            // New root frame. Reset all per-tx state.
            cursor.clear();
            next_child_slot.clear();
            out.push(Vec::new());
            seen_root = true;
        } else {
            if !seen_root {
                return Err(MonadChainDataError::InvalidRequest(
                    "trace_address: first frame must have depth 0",
                ));
            }
            if d > cursor.len() + 1 {
                return Err(MonadChainDataError::InvalidRequest(
                    "trace_address: depth skipped a level",
                ));
            }
            // Pop sibling counters for any depths deeper than the parent.
            // Walking from a deep frame back up to a shallower one means
            // those subtrees are closed: the next frame at depth d is a
            // sibling at depth d, not a continuation of a deeper line.
            cursor.truncate(d - 1);
            // Ensure the sibling-counter slot for this depth exists.
            if next_child_slot.len() < d {
                next_child_slot.resize(d, 0);
            }
            // We're about to assign a child at depth d under the current
            // parent. Drop any sibling counters deeper than d-1; those
            // belonged to a subtree that just closed.
            next_child_slot.truncate(d);
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
        // Tree:
        //   root  (depth 0)        []
        //     A   (depth 1)        [0]
        //       A1 (depth 2)       [0, 0]
        //       A2 (depth 2)       [0, 1]
        //     B   (depth 1)        [1]
        //       B1 (depth 2)       [1, 0]
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
        assert!(matches!(err, MonadChainDataError::InvalidRequest(_)));
    }

    #[test]
    fn trace_address_rejects_depth_jump() {
        let err = compute_trace_addresses([0u32, 2]).unwrap_err();
        assert!(matches!(err, MonadChainDataError::InvalidRequest(_)));
    }
}
