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

//! External archive payloads: instead of re-encoding rows into chain-data's
//! own pack blobs, ingest can index byte ranges inside an EXISTING
//! monad-archive object store ("BDR": per-block uncompressed RLP objects).
//!
//! The source supplies an [`ExternalPayloadSpec`] per block — per-family
//! container byte ranges plus row/status manifests — which ingest validates
//! against the decoded block and stamps into [`BlockBlobHeader`]s
//! (`encoding = 1`). Queries then range-read the archive objects through an
//! [`ExternalBlobReader`] and decode containers with the mirrors below.
//!
//! The decode mirrors must produce rows byte-identical to what native ingest
//! stores for the same block (the cross-crate round-trip test in
//! `monad-archive` is the guard):
//!
//! - tx container: a `monad_eth_types::TxEnvelopeWithSender` item — an RLP
//!   list of `[rlp-string(network tx encoding), sender]`.
//! - receipt container: a `monad_eth_types::ReceiptWithLogIndex` item — an
//!   RLP list of `[rlp-string(network receipt encoding), starting_log_index]`.
//! - trace container: a `Vec<u8>` item (alloy-rlp encodes it as a list of
//!   byte scalars) whose content decodes as the archive's
//!   `Vec<Vec<CallFrame>>` custom RLP; frames flatten in DFS order, one row
//!   each.

use std::{collections::BTreeMap, sync::RwLock};

use alloy_consensus::ReceiptEnvelope;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, U256, U64, U8};
use alloy_rlp::Decodable;
use bytes::Bytes as RawBytes;
use futures::future::BoxFuture;

use crate::{
    error::{MonadChainDataError, Result},
    ingest_types::{CallKind, FinalizedBlock},
    logs::StoredLog,
    traces::{compute_trace_addresses, StoredTrace},
    txs::StoredTxEnvelope,
};

/// One family's byte layout inside its archive object, as supplied by the
/// ingest source. All offsets are object-relative through `base_offset`;
/// containers are contiguous (`container_offsets` is a partition), which the
/// source asserts when collapsing the scanner's exact item ranges into it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalFamilyRegion {
    /// Raw archive object key (e.g. `block/000000000123`); never empty.
    pub key: Vec<u8>,
    /// Start byte of the first container within the object.
    pub base_offset: u32,
    /// Container partition relative to `base_offset`: container `j` spans
    /// `[container_offsets[j], container_offsets[j + 1])`; length is
    /// `containers + 1` (`[0]` for an empty block).
    pub container_offsets: Vec<u32>,
    /// Cumulative row counts per container; empty = identity (one row per
    /// container, the tx family) — also empty when there are no containers.
    pub container_rows: Vec<u32>,
    /// Per-container tx-status bitvec, LSB-first (traces only; empty
    /// otherwise). Trailing pad bits must be zero.
    pub container_status: Vec<u8>,
}

/// Per-block external payload locators for all three families.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalPayloadSpec {
    /// The block the locators were scanned from. Binds the spec to its block
    /// so a source that mis-pairs a spec with another block's `FinalizedBlock`
    /// fails validation even when per-family counts coincide (any equal tx
    /// count satisfies the identity tx manifest).
    pub block_number: u64,
    pub logs: ExternalFamilyRegion,
    pub txs: ExternalFamilyRegion,
    pub traces: ExternalFamilyRegion,
}

impl ExternalFamilyRegion {
    /// Structural invariants shared by every family: a non-empty key, a
    /// well-formed container partition (each container non-empty, so item
    /// ranges are exact), and a consistent cumulative row manifest whose
    /// per-container counts match `rows_per_container`.
    fn validate(&self, rows_per_container: &[u32], identity: bool) -> Result<()> {
        let containers = rows_per_container.len();
        if self.key.is_empty() {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload region missing archive key",
            ));
        }
        if self.container_offsets.len() != containers + 1 || self.container_offsets[0] != 0 {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container partition has the wrong shape",
            ));
        }
        if self.container_offsets.windows(2).any(|w| w[0] >= w[1]) {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload containers must be non-empty and ascending",
            ));
        }
        if self.container_offsets.last().copied().unwrap_or(0) as u64 + u64::from(self.base_offset)
            > u32::MAX as u64
        {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload region exceeds the u32 offset space",
            ));
        }
        if identity {
            if !self.container_rows.is_empty() {
                return Err(MonadChainDataError::InvalidBlock(
                    "identity external region must omit container_rows",
                ));
            }
            if rows_per_container.iter().any(|&rows| rows != 1) {
                return Err(MonadChainDataError::InvalidBlock(
                    "identity external region requires one row per container",
                ));
            }
            return Ok(());
        }
        if self.container_rows.len() != containers {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container_rows length mismatch",
            ));
        }
        let mut cumulative = 0u32;
        for (&rows, &cum) in rows_per_container.iter().zip(&self.container_rows) {
            cumulative = cumulative
                .checked_add(rows)
                .ok_or(MonadChainDataError::InvalidBlock(
                    "external payload row count overflow",
                ))?;
            if cum != cumulative {
                return Err(MonadChainDataError::InvalidBlock(
                    "external payload container_rows disagrees with the block's rows",
                ));
            }
        }
        Ok(())
    }

    /// Status-bitvec invariants: exactly `containers.div_ceil(8)` bytes with
    /// zero trailing pad bits, matching `statuses` bit for bit.
    fn validate_status(&self, statuses: impl ExactSizeIterator<Item = bool>) -> Result<()> {
        let containers = statuses.len();
        if self.container_status.len() != containers.div_ceil(8) {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container_status length mismatch",
            ));
        }
        let mut expected = vec![0u8; containers.div_ceil(8)];
        for (j, status) in statuses.enumerate() {
            if status {
                expected[j / 8] |= 1 << (j % 8);
            }
        }
        if self.container_status != expected {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container_status disagrees with the block's receipts",
            ));
        }
        Ok(())
    }
}

impl ExternalPayloadSpec {
    /// Validates the spec against the decoded block it locates: the spec was
    /// scanned from this block number, container counts equal the tx count
    /// for every family, row manifests match the block's logs/frames exactly,
    /// and the trace status bits match every frame's `tx_status`. Hard error
    /// on any mismatch — a bad locator would otherwise serve another tx's
    /// bytes.
    pub fn validate(&self, block_number: u64, block: &FinalizedBlock) -> Result<()> {
        if self.block_number != block_number {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload spec belongs to a different block",
            ));
        }
        let tx_count = block.txs.len();
        if block.logs_by_tx.len() != tx_count {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload requires one receipt container per tx",
            ));
        }

        self.txs.validate(&vec![1u32; tx_count], true)?;
        self.txs.validate_status(std::iter::empty())?;

        let logs_per_tx: Vec<u32> = block
            .logs_by_tx
            .iter()
            .map(|logs| u32::try_from(logs.len()))
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| MonadChainDataError::Decode("log count overflow"))?;
        self.logs.validate(&logs_per_tx, false)?;
        self.logs.validate_status(std::iter::empty())?;

        let mut frames_per_tx = vec![0u32; tx_count];
        let mut statuses: Vec<Option<bool>> = vec![None; tx_count];
        for trace in &block.traces {
            let slot = frames_per_tx.get_mut(trace.tx_index as usize).ok_or(
                MonadChainDataError::InvalidBlock("external payload trace tx_index out of range"),
            )?;
            *slot += 1;
            let status = &mut statuses[trace.tx_index as usize];
            if status.is_some_and(|s| s != trace.tx_status) {
                return Err(MonadChainDataError::InvalidBlock(
                    "external payload trace tx_status inconsistent within a tx",
                ));
            }
            *status = Some(trace.tx_status);
        }
        self.traces.validate(&frames_per_tx, false)?;
        // Containers without frames carry the receipt status the source read;
        // rows only ever consult the bits of their own container, so bits of
        // frameless containers are accepted as supplied.
        let supplied_bit = |j: usize| {
            self.traces
                .container_status
                .get(j / 8)
                .is_some_and(|byte| byte >> (j % 8) & 1 == 1)
        };
        self.traces.validate_status(
            statuses
                .iter()
                .enumerate()
                .map(|(j, status)| status.unwrap_or_else(|| supplied_bit(j)))
                .collect::<Vec<_>>()
                .into_iter(),
        )?;
        Ok(())
    }
}

/// Read-only byte-range access to the external archive's objects. Keys are
/// the RAW archive object keys (no chain-data prefix/table/hex mangling).
/// Object-safe (boxed futures) so [`crate::engine::tables::Tables`] can hold
/// it without growing a third type parameter.
pub trait ExternalBlobReader: Send + Sync + 'static {
    /// Reads `[start, end_exclusive)` of `key`; `Ok(None)` when the object is
    /// absent. Semantics match [`crate::store::BlobStore::read_range`]: the
    /// end clamps to EOF, a start strictly past EOF is an error.
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> BoxFuture<'_, Result<Option<RawBytes>>>;
}

/// In-memory [`ExternalBlobReader`] for tests: a key -> object map.
#[derive(Debug, Default)]
pub struct InMemoryExternalBlobReader {
    objects: RwLock<BTreeMap<Vec<u8>, RawBytes>>,
}

impl InMemoryExternalBlobReader {
    pub fn insert(&self, key: impl Into<Vec<u8>>, object: impl Into<RawBytes>) {
        self.objects
            .write()
            .expect("poisoned lock")
            .insert(key.into(), object.into());
    }
}

impl ExternalBlobReader for InMemoryExternalBlobReader {
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> BoxFuture<'_, Result<Option<RawBytes>>> {
        let result = (|| {
            let objects = self.objects.read().expect("poisoned lock");
            let Some(object) = objects.get(key) else {
                return Ok(None);
            };
            if start > end_exclusive || start > object.len() {
                return Err(MonadChainDataError::Decode("invalid blob range"));
            }
            Ok(Some(object.slice(start..end_exclusive.min(object.len()))))
        })();
        Box::pin(async move { result })
    }
}

/// Decodes the RLP `Header` of one container item and bounds-checks it against
/// the item slice, returning the payload. Item ranges are exact, so trailing
/// bytes are an error.
fn item_header(
    buf: &mut &[u8],
    expect_list: bool,
    what: &'static str,
) -> Result<alloy_rlp::Header> {
    let header = alloy_rlp::Header::decode(buf).map_err(|_| MonadChainDataError::Decode(what))?;
    if header.list != expect_list {
        return Err(MonadChainDataError::Decode(what));
    }
    Ok(header)
}

fn expect_consumed(buf: &[u8], what: &'static str) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(MonadChainDataError::Decode(what))
    }
}

/// Decodes one `TxEnvelopeWithSender` archive item into the stored tx row:
/// outer list of `[rlp-string(network tx), sender]`. `tx_hash` and
/// `signed_tx_bytes` are derived exactly like the native ingest conversion
/// (`tx.tx_hash()` / `encoded_2718`).
pub fn decode_external_tx(bytes: &[u8]) -> Result<StoredTxEnvelope> {
    const ERR: &str = "invalid archive tx container";
    let buf = &mut &bytes[..];
    item_header(buf, true, ERR)?;
    // The wrapper string around the tx's network encoding.
    item_header(buf, false, ERR)?;
    let tx =
        alloy_consensus::TxEnvelope::decode(buf).map_err(|_| MonadChainDataError::Decode(ERR))?;
    let sender = Address::decode(buf).map_err(|_| MonadChainDataError::Decode(ERR))?;
    expect_consumed(buf, ERR)?;
    Ok(StoredTxEnvelope {
        tx_hash: *tx.tx_hash(),
        sender,
        signed_tx_bytes: Bytes::from(tx.encoded_2718()),
    })
}

/// Decodes one `ReceiptWithLogIndex` archive item into stored log rows:
/// outer list of `[rlp-string(network receipt), starting_log_index]`.
/// `log_index` is block-global, derived from the header's row manifest
/// (`first_log_index`) exactly like native `flatten_logs`; the stored
/// `starting_log_index` is not trusted for it.
pub fn decode_external_receipt_logs(
    bytes: &[u8],
    tx_index: u32,
    first_log_index: u32,
) -> Result<Vec<StoredLog>> {
    const ERR: &str = "invalid archive receipt container";
    let buf = &mut &bytes[..];
    item_header(buf, true, ERR)?;
    item_header(buf, false, ERR)?;
    let receipt = ReceiptEnvelope::decode(buf).map_err(|_| MonadChainDataError::Decode(ERR))?;
    let _starting_log_index = u64::decode(buf).map_err(|_| MonadChainDataError::Decode(ERR))?;
    expect_consumed(buf, ERR)?;
    receipt
        .logs()
        .iter()
        .enumerate()
        .map(|(i, log)| {
            let log_index = first_log_index
                .checked_add(u32::try_from(i).map_err(|_| MonadChainDataError::Decode(ERR))?)
                .ok_or(MonadChainDataError::Decode("log index overflow"))?;
            Ok(StoredLog {
                tx_index,
                log_index,
                address: log.address,
                topics: log.data.topics().to_vec(),
                data: log.data.data.clone(),
            })
        })
        .collect()
}

/// Decode-only mirror of monad-archive's `CallFrame` custom RLP (the
/// authoritative encoder is `monad_archive::model::block_data_archive`'s
/// `Encodable for CallFrame`; the cross-crate round-trip test guards
/// agreement): a bare field sequence (no per-frame list header), `to` encoded
/// as `0x80` when absent, trailing `logs` present whenever more bytes remain.
struct ArchiveCallFrame {
    typ: CallKind,
    from: Address,
    to: Option<Address>,
    value: U256,
    gas: u64,
    gas_used: u64,
    input: Bytes,
    output: Bytes,
    status: u8,
    depth: u32,
}

/// Mirror of the archive's `CallFrameLog` (decoded and discarded — stored
/// trace rows do not carry frame logs).
#[derive(alloy_rlp::RlpDecodable)]
struct ArchiveCallFrameLog {
    _log: alloy_primitives::Log,
    _position: U64,
}

impl ArchiveCallFrame {
    fn decode(buf: &mut &[u8], err: &'static str) -> Result<Self> {
        let decode_err = |_| MonadChainDataError::Decode(err);
        let typ = U8::decode(buf).map_err(decode_err)?;
        let flags = U64::decode(buf).map_err(decode_err)?;
        let from = Address::decode(buf).map_err(decode_err)?;
        let to = match buf.first() {
            Some(0x80) => {
                *buf = &buf[1..];
                None
            }
            Some(_) => Some(Address::decode(buf).map_err(decode_err)?),
            None => return Err(MonadChainDataError::Decode(err)),
        };
        let value = U256::decode(buf).map_err(decode_err)?;
        let gas = U64::decode(buf).map_err(decode_err)?;
        let gas_used = U64::decode(buf).map_err(decode_err)?;
        let input = Bytes::decode(buf).map_err(decode_err)?;
        let output = Bytes::decode(buf).map_err(decode_err)?;
        let status = U8::decode(buf).map_err(decode_err)?;
        let depth = U64::decode(buf).map_err(decode_err)?;
        if !buf.is_empty() {
            let _logs = Vec::<ArchiveCallFrameLog>::decode(buf).map_err(decode_err)?;
        }
        let typ = match typ.to::<u8>() {
            0 if flags == U64::from(1) => CallKind::StaticCall,
            0 => CallKind::Call,
            1 => CallKind::DelegateCall,
            2 => CallKind::CallCode,
            3 => CallKind::Create,
            4 => CallKind::Create2,
            5 => CallKind::SelfDestruct,
            _ => return Err(MonadChainDataError::Decode(err)),
        };
        Ok(Self {
            typ,
            from,
            to,
            value,
            gas: gas.to::<u64>(),
            gas_used: gas_used.to::<u64>(),
            input,
            output,
            status: status.to::<u8>(),
            depth: u32::try_from(depth.to::<u64>())
                .map_err(|_| MonadChainDataError::Decode(err))?,
        })
    }
}

/// Decodes one tx's trace archive item — the alloy-rlp encoding of a
/// `Vec<u8>` (a LIST of byte scalars, not an RLP string) whose reassembled
/// content is the archive's `Vec<Vec<CallFrame>>` — into stored trace rows:
/// frames flatten in DFS order, `trace_address` recomputed from depths,
/// exactly mirroring the native ingest conversion.
pub fn decode_external_trace_container(
    bytes: &[u8],
    tx_index: u32,
    tx_status: bool,
) -> Result<Vec<StoredTrace>> {
    const ERR: &str = "invalid archive trace container";
    let buf = &mut &bytes[..];
    let blob = Vec::<u8>::decode(buf).map_err(|_| MonadChainDataError::Decode(ERR))?;
    expect_consumed(buf, ERR)?;

    // The blob is `Vec<Vec<CallFrame>>`: an outer list of inner lists, each
    // inner list a concatenation of bare frame field sequences.
    let outer = &mut blob.as_ref();
    let outer_header = item_header(outer, true, ERR)?;
    if outer.len() != outer_header.payload_length {
        return Err(MonadChainDataError::Decode(ERR));
    }
    let mut frames: Vec<ArchiveCallFrame> = Vec::new();
    while !outer.is_empty() {
        let inner_header = item_header(outer, true, ERR)?;
        let (mut inner, rest) = outer
            .split_at_checked(inner_header.payload_length)
            .ok_or(MonadChainDataError::Decode(ERR))?;
        *outer = rest;
        while !inner.is_empty() {
            frames.push(ArchiveCallFrame::decode(&mut inner, ERR)?);
        }
    }
    if frames.is_empty() {
        return Ok(Vec::new());
    }

    let trace_addresses = compute_trace_addresses(frames.iter().map(|f| f.depth))?;
    Ok(frames
        .into_iter()
        .zip(trace_addresses)
        .map(|(frame, trace_address)| StoredTrace {
            typ: frame.typ,
            from: frame.from,
            to: frame.to,
            value: frame.value,
            gas: frame.gas,
            gas_used: frame.gas_used,
            input: frame.input,
            output: frame.output,
            status: frame.status,
            depth: frame.depth,
            tx_index,
            trace_address,
            tx_status,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Receipt, ReceiptWithBloom, SignableTransaction, TxEnvelope, TxLegacy};
    use alloy_primitives::{Log, LogData, Signature, TxKind, B256};
    use alloy_rlp::Encodable;

    use super::*;

    /// Test-only encoder mirroring `monad_eth_types::TxEnvelopeWithSender`:
    /// list of `[rlp-string(network tx), sender]`.
    pub(crate) fn encode_tx_with_sender(tx: &TxEnvelope, sender: Address) -> Vec<u8> {
        let mut network = Vec::new();
        tx.encode(&mut network);
        let mut out = Vec::new();
        let payload: &[&dyn Encodable] = &[&network.as_slice(), &sender];
        alloy_rlp::encode_list::<_, dyn Encodable>(payload, &mut out);
        out
    }

    /// Test-only encoder mirroring `monad_eth_types::ReceiptWithLogIndex`.
    pub(crate) fn encode_receipt_with_log_index(
        receipt: &ReceiptEnvelope,
        starting_log_index: u64,
    ) -> Vec<u8> {
        let mut network = Vec::new();
        receipt.encode(&mut network);
        let mut out = Vec::new();
        let payload: &[&dyn Encodable] = &[&network.as_slice(), &starting_log_index];
        alloy_rlp::encode_list::<_, dyn Encodable>(payload, &mut out);
        out
    }

    fn legacy_tx(nonce: u64) -> TxEnvelope {
        TxEnvelope::Legacy(
            TxLegacy {
                chain_id: Some(1),
                nonce,
                gas_price: 7,
                gas_limit: 21_000,
                to: TxKind::Call(Address::repeat_byte(0x22)),
                value: U256::from(5u64),
                input: Bytes::from(vec![1, 2, 3, 4, 5]),
            }
            .into_signed(Signature::test_signature()),
        )
    }

    fn receipt_with_logs(status: bool, log_count: usize) -> ReceiptEnvelope {
        let logs = (0..log_count)
            .map(|i| Log {
                address: Address::repeat_byte(0x33),
                data: LogData::new_unchecked(
                    vec![B256::repeat_byte(i as u8)],
                    Bytes::from(vec![i as u8; 3]),
                ),
            })
            .collect();
        ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt {
                status: status.into(),
                cumulative_gas_used: 21_000,
                logs,
            },
            Default::default(),
        ))
    }

    #[test]
    fn external_tx_decode_matches_native_fields() {
        let tx = legacy_tx(3);
        let sender = Address::repeat_byte(0x11);
        let item = encode_tx_with_sender(&tx, sender);

        let stored = decode_external_tx(&item).expect("decode tx container");
        assert_eq!(stored.tx_hash, *tx.tx_hash());
        assert_eq!(stored.sender, sender);
        assert_eq!(stored.signed_tx_bytes, Bytes::from(tx.encoded_2718()));
    }

    #[test]
    fn external_tx_decode_rejects_truncated_and_trailing_bytes() {
        let item = encode_tx_with_sender(&legacy_tx(0), Address::repeat_byte(0x11));
        assert!(decode_external_tx(&item[..item.len() - 1]).is_err());
        let mut padded = item;
        padded.push(0x00);
        assert!(decode_external_tx(&padded).is_err());
        assert!(decode_external_tx(&[]).is_err());
    }

    #[test]
    fn external_receipt_decode_emits_block_global_log_indices() {
        let receipt = receipt_with_logs(true, 2);
        // A divergent stored starting_log_index must NOT shift the emitted
        // indices: the header manifest is authoritative.
        let item = encode_receipt_with_log_index(&receipt, 999);

        let logs = decode_external_receipt_logs(&item, 4, 7).expect("decode receipt container");
        assert_eq!(logs.len(), 2);
        assert_eq!((logs[0].tx_index, logs[0].log_index), (4, 7));
        assert_eq!((logs[1].tx_index, logs[1].log_index), (4, 8));
        assert_eq!(logs[0].address, Address::repeat_byte(0x33));
    }

    #[test]
    fn external_receipt_decode_rejects_garbage() {
        assert!(decode_external_receipt_logs(b"nonsense", 0, 0).is_err());
        let item = encode_receipt_with_log_index(&receipt_with_logs(true, 1), 0);
        assert!(decode_external_receipt_logs(&item[..item.len() - 2], 0, 0).is_err());
    }

    /// Test-only encoder mirroring the archive's `CallFrame` Encodable.
    pub(crate) fn encode_call_frame(
        typ_byte: u8,
        flags: u64,
        from: Address,
        to: Option<Address>,
        value: u64,
        depth: u32,
        out: &mut Vec<u8>,
    ) {
        typ_byte.encode(out);
        U64::from(flags).encode(out);
        from.encode(out);
        match to {
            Some(to) => to.encode(out),
            None => out.push(0x80),
        }
        U256::from(value).encode(out);
        U64::from(100_000u64).encode(out);
        U64::from(21_000u64).encode(out);
        Bytes::from(vec![0xaa, 0xbb, 0xcc, 0xdd]).encode(out);
        Bytes::new().encode(out);
        U8::from(0u8).encode(out);
        U64::from(depth).encode(out);
        // logs: Some(vec![]) — always present so concatenated frames stay
        // self-delimiting.
        Vec::<u8>::new().encode(out);
    }

    /// Encodes one tx's trace archive item: `rlp(Vec<u8>)` (alloy-rlp's
    /// list-of-byte-scalars framing, exactly what `Vec<Vec<u8>>::encode`
    /// produces per item) wrapping `rlp(Vec<Vec<frame>>)`.
    pub(crate) fn encode_trace_item(frame_groups: &[Vec<Vec<u8>>]) -> Vec<u8> {
        let mut inner_lists = Vec::new();
        for group in frame_groups {
            let payload: Vec<u8> = group.concat();
            let header = alloy_rlp::Header {
                list: true,
                payload_length: payload.len(),
            };
            header.encode(&mut inner_lists);
            inner_lists.extend_from_slice(&payload);
        }
        let mut blob = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: inner_lists.len(),
        }
        .encode(&mut blob);
        blob.extend_from_slice(&inner_lists);
        let mut item = Vec::new();
        blob.encode(&mut item);
        item
    }

    #[test]
    fn external_trace_decode_flattens_frames_with_trace_addresses() {
        let from = Address::repeat_byte(0x44);
        let to = Address::repeat_byte(0x55);
        let mut root = Vec::new();
        encode_call_frame(0, 0, from, Some(to), 9, 0, &mut root);
        let mut child = Vec::new();
        encode_call_frame(0, 1, from, None, 0, 1, &mut child);
        let item = encode_trace_item(&[vec![root, child]]);

        let rows = decode_external_trace_container(&item, 2, true).expect("decode trace item");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].typ, CallKind::Call);
        assert_eq!(rows[0].to, Some(to));
        assert_eq!(rows[0].value, U256::from(9u64));
        assert_eq!(rows[0].trace_address, Vec::<u32>::new());
        assert_eq!(rows[1].typ, CallKind::StaticCall);
        assert_eq!(rows[1].to, None);
        assert_eq!(rows[1].trace_address, vec![0]);
        assert!(rows.iter().all(|r| r.tx_index == 2 && r.tx_status));
    }

    #[test]
    fn external_trace_decode_rejects_truncation_and_bad_kind() {
        let mut frame = Vec::new();
        encode_call_frame(0, 0, Address::ZERO, None, 0, 0, &mut frame);
        let item = encode_trace_item(&[vec![frame.clone()]]);
        assert!(decode_external_trace_container(&item[..item.len() - 1], 0, true).is_err());

        let mut bad = Vec::new();
        encode_call_frame(9, 0, Address::ZERO, None, 0, 0, &mut bad);
        let bad_item = encode_trace_item(&[vec![bad]]);
        assert!(decode_external_trace_container(&bad_item, 0, true).is_err());
    }

    #[test]
    fn in_memory_reader_clamps_eof_and_errors_past_it() {
        let reader = InMemoryExternalBlobReader::default();
        reader.insert(b"k".to_vec(), RawBytes::from_static(b"0123456789"));
        let read = |start, end| futures::executor::block_on(reader.read_range(b"k", start, end));
        assert_eq!(read(2, 5).unwrap().unwrap().as_ref(), b"234");
        assert_eq!(read(8, 64).unwrap().unwrap().as_ref(), b"89");
        assert_eq!(read(10, 12).unwrap().unwrap().as_ref(), b"");
        assert!(read(11, 12).is_err());
        assert!(
            futures::executor::block_on(reader.read_range(b"absent", 0, 1))
                .unwrap()
                .is_none()
        );
    }
}
