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

#![allow(dead_code)]

pub mod observed_store;
pub mod populate;

use alloy_primitives::{Address, Bytes, U256};
use monad_chain_data::{CallKind, EvmBlockHeader, IngestTrace, IngestTx, B256};

pub fn test_header(number: u64, parent_hash: B256) -> EvmBlockHeader {
    EvmBlockHeader {
        number,
        parent_hash,
        ..EvmBlockHeader::default()
    }
}

pub fn chain_header(number: u64, parent: &EvmBlockHeader) -> EvmBlockHeader {
    test_header(number, parent.hash_slow())
}

/// Valid, minimal EIP-2718-encoded signed tx envelope (legacy, contract
/// creation, empty calldata). Tests use it when they need `TxIngestPlan`
/// to decode the envelope successfully but don't care about the contents.
pub fn minimal_ingest_tx() -> IngestTx {
    use alloy_primitives::Address;

    ingest_tx(Address::ZERO, None, Vec::new())
}

/// Builds an `IngestTx` with the given `sender`, recipient, and calldata.
/// `to = None` produces a contract-creation envelope. The signed-tx bytes
/// always use `Signature::test_signature`; the recovered address of the
/// envelope signer therefore does NOT equal `sender`. Filter semantics
/// read `IngestTx::sender` (the ingest-declared `from`), not the
/// envelope-recovered signer, so tests can set `sender` freely.
pub fn ingest_tx(
    sender: alloy_primitives::Address,
    to: Option<alloy_primitives::Address>,
    input: Vec<u8>,
) -> IngestTx {
    use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Signature, TxKind, U256};

    let signed = TxLegacy {
        chain_id: Some(1),
        nonce: 0,
        gas_price: 0,
        gas_limit: 21_000,
        to: to.map_or(TxKind::Create, TxKind::Call),
        value: U256::ZERO,
        input: input.into(),
    }
    .into_signed(Signature::test_signature());

    let mut signed_tx_bytes = Vec::new();
    TxEnvelope::Legacy(signed).encode_2718(&mut signed_tx_bytes);

    IngestTx {
        tx_hash: B256::ZERO,
        sender,
        signed_tx_bytes: signed_tx_bytes.into(),
    }
}

/// Builds an `IngestTrace` with the most common fields. The producer is
/// normally responsible for computing `trace_address`; tests can pass
/// `vec![]` for top-level frames and `vec![0, ...]` for nested ones.
#[allow(clippy::too_many_arguments)]
pub fn make_trace(
    tx_index: u32,
    depth: u32,
    trace_address: Vec<u32>,
    typ: CallKind,
    from: Address,
    to: Option<Address>,
    value: U256,
    input: Vec<u8>,
    status: u8,
    tx_status: bool,
) -> IngestTrace {
    IngestTrace {
        typ,
        from,
        to,
        value,
        gas: 100_000,
        gas_used: 21_000,
        input: Bytes::from(input),
        output: Bytes::new(),
        status,
        depth,
        tx_index,
        trace_address,
        tx_status,
    }
}

/// Top-level successful CALL frame. `value > 0` and the parent tx
/// succeeded, so this frame qualifies for `has_transfer`.
pub fn top_level_call(
    tx_index: u32,
    from: Address,
    to: Address,
    value: U256,
    input: Vec<u8>,
) -> IngestTrace {
    make_trace(
        tx_index,
        0,
        Vec::new(),
        CallKind::Call,
        from,
        Some(to),
        value,
        input,
        0,
        true,
    )
}

/// Nested CALL frame (one level deep, first child of the root).
pub fn nested_call(
    tx_index: u32,
    from: Address,
    to: Address,
    value: U256,
    input: Vec<u8>,
) -> IngestTrace {
    make_trace(
        tx_index,
        1,
        vec![0],
        CallKind::Call,
        from,
        Some(to),
        value,
        input,
        0,
        true,
    )
}

/// Test seed: durably write one compacted bitmap page artifact via the staged
/// write path (the production write path), for tests that need pre-existing
/// page artifacts.
pub async fn seed_bitmap_page_artifact<M, B>(
    tables: &monad_chain_data::engine::tables::Tables<M, B>,
    family: monad_chain_data::engine::family::Family,
    stream_id: &str,
    page_start_local: u32,
    artifact: &monad_chain_data::engine::bitmap::BitmapPageArtifact,
) where
    M: monad_chain_data::store::MetaStore,
    B: monad_chain_data::store::BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables.family(family).bitmap().stage_page_artifact(
                    w,
                    stream_id,
                    page_start_local,
                    artifact,
                );
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page artifact");
}

/// Test seed: durably write one sealed-shard page-count manifest.
pub async fn seed_bitmap_page_counts<M, B>(
    tables: &monad_chain_data::engine::tables::Tables<M, B>,
    family: monad_chain_data::engine::family::Family,
    stream_id: &str,
    counts: &monad_chain_data::engine::bitmap::BitmapPageCounts,
) where
    M: monad_chain_data::store::MetaStore,
    B: monad_chain_data::store::BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables
                    .family(family)
                    .bitmap()
                    .stage_page_counts(w, stream_id, counts);
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page counts");
}
