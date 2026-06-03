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

use alloy_primitives::{Address, Bytes, U256};
use alloy_rlp::{RlpDecodable, RlpEncodable};

use crate::{
    error::{MonadChainDataError, Result},
    family::{CallKind, Hash32, IngestTrace},
};

/// Public, owned per-trace view returned by queries. Block-level fields
/// (`block_number`, `block_hash`) are reconstructed from the
/// `BlockRecord` at read time, mirroring the log and tx materializers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub trace_address: Vec<u32>,
    pub typ: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub depth: u32,
    pub tx_status: bool,
}

impl TraceEntry {
    pub fn is_top_level(&self) -> bool {
        self.trace_address.is_empty()
    }

    /// 4-byte function selector; `None` when `input` is shorter than 4
    /// bytes (e.g. value-only transfers).
    pub fn selector(&self) -> Option<[u8; 4]> {
        if self.input.len() < 4 {
            None
        } else {
            let mut out = [0u8; 4];
            out.copy_from_slice(&self.input[..4]);
            Some(out)
        }
    }
}

/// Per-trace fields stored in the block trace blob. The RLP layout
/// encodes `typ` as a single byte (via `CallKind::as_u8`) and packs the
/// remaining fields positionally. Block-level fields are reconstructed
/// from the `BlockRecord` at read time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTrace {
    pub typ: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub depth: u32,
    pub tx_index: u32,
    pub trace_address: Vec<u32>,
    pub tx_status: bool,
}

/// On-disk shape. `to_bytes` is empty for contract-creation /
/// SelfDestruct-with-zero-beneficiary frames, otherwise a 20-byte
/// address. Encoding `Option<Address>` via the derive macros isn't
/// supported, so we marshal it through a `Bytes` field.
#[derive(Debug, RlpEncodable, RlpDecodable)]
struct StoredTraceRlp {
    typ_byte: u8,
    from: Address,
    to_bytes: Bytes,
    value: U256,
    gas: u64,
    gas_used: u64,
    input: Bytes,
    output: Bytes,
    status: u8,
    depth: u32,
    tx_index: u32,
    trace_address: Vec<u32>,
    tx_status_byte: u8,
}

impl StoredTrace {
    pub fn encode(&self) -> Vec<u8> {
        let to_bytes = match self.to {
            Some(addr) => Bytes::copy_from_slice(addr.as_slice()),
            None => Bytes::new(),
        };
        let rlp = StoredTraceRlp {
            typ_byte: self.typ.as_u8(),
            from: self.from,
            to_bytes,
            value: self.value,
            gas: self.gas,
            gas_used: self.gas_used,
            input: self.input.clone(),
            output: self.output.clone(),
            status: self.status,
            depth: self.depth,
            tx_index: self.tx_index,
            trace_address: self.trace_address.clone(),
            tx_status_byte: u8::from(self.tx_status),
        };
        alloy_rlp::encode(&rlp)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let rlp: StoredTraceRlp = alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid trace entry rlp"))?;
        let typ = CallKind::from_u8(rlp.typ_byte)
            .ok_or(MonadChainDataError::Decode("invalid trace call kind byte"))?;
        let to = if rlp.to_bytes.is_empty() {
            None
        } else if rlp.to_bytes.len() == 20 {
            Some(Address::from_slice(&rlp.to_bytes))
        } else {
            return Err(MonadChainDataError::Decode(
                "invalid trace `to` byte length",
            ));
        };
        Ok(Self {
            typ,
            from: rlp.from,
            to,
            value: rlp.value,
            gas: rlp.gas,
            gas_used: rlp.gas_used,
            input: rlp.input,
            output: rlp.output,
            status: rlp.status,
            depth: rlp.depth,
            tx_index: rlp.tx_index,
            trace_address: rlp.trace_address,
            tx_status: rlp.tx_status_byte != 0,
        })
    }

    pub fn into_trace_entry(self, block_number: u64, block_hash: Hash32) -> TraceEntry {
        TraceEntry {
            block_number,
            block_hash,
            tx_index: self.tx_index,
            trace_address: self.trace_address,
            typ: self.typ,
            from: self.from,
            to: self.to,
            value: self.value,
            gas: self.gas,
            gas_used: self.gas_used,
            input: self.input,
            output: self.output,
            status: self.status,
            depth: self.depth,
            tx_status: self.tx_status,
        }
    }
}

impl From<&IngestTrace> for StoredTrace {
    fn from(trace: &IngestTrace) -> Self {
        Self {
            typ: trace.typ,
            from: trace.from,
            to: trace.to,
            value: trace.value,
            gas: trace.gas,
            gas_used: trace.gas_used,
            input: trace.input.clone(),
            output: trace.output.clone(),
            status: trace.status,
            depth: trace.depth,
            tx_index: trace.tx_index,
            trace_address: trace.trace_address.clone(),
            tx_status: trace.tx_status,
        }
    }
}
