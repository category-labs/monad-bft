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
    error::Result,
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

