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

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, Bytes};
use alloy_rlp::{RlpDecodable, RlpEncodable};

use crate::{
    error::{MonadChainDataError, Result},
    family::Hash32,
};

/// Public, owned per-transaction view returned by queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_idx: u32,
    pub tx_hash: Hash32,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

impl TxEntry {
    /// Decodes the stored signed-tx bytes into the consensus envelope.
    /// Callers that need multiple fields should decode once and work
    /// with the `TxEnvelope`; the per-field accessors each re-parse.
    pub fn envelope(&self) -> Result<TxEnvelope> {
        decode_envelope(&self.signed_tx_bytes)
    }

    /// Recipient address; `None` for contract-creation transactions.
    pub fn to(&self) -> Result<Option<Address>> {
        Ok(self.envelope()?.to())
    }

    /// 4-byte function selector; `None` for contract-creation transactions
    /// or when the calldata is shorter than 4 bytes.
    pub fn selector(&self) -> Result<Option<[u8; 4]>> {
        Ok(selector_from_envelope(&self.envelope()?))
    }

    /// Assembles the queryX-spec `alloy_rpc_types_eth::Transaction` shape
    /// from the stored envelope, sender, and block context.
    ///
    /// `effective_gas_price` is left as `None`: it depends on the block's
    /// `base_fee_per_gas`, which is not carried on `TxEntry`. RPC layers
    /// returning this shape directly must load the block header and
    /// populate it via `envelope.effective_gas_price(Some(base_fee))`.
    #[cfg(feature = "alloy-rpc-types-eth")]
    pub fn to_rpc_transaction(&self) -> Result<alloy_rpc_types_eth::Transaction> {
        use alloy_consensus::transaction::Recovered;

        Ok(alloy_rpc_types_eth::Transaction {
            inner: Recovered::new_unchecked(self.envelope()?, self.sender),
            block_hash: Some(self.block_hash),
            block_number: Some(self.block_number),
            transaction_index: Some(u64::from(self.tx_idx)),
            effective_gas_price: None,
        })
    }
}

/// Fixed 12-byte address of a transaction in the block-tx blobs:
/// 8 bytes big-endian `block_number` followed by 4 bytes big-endian
/// `tx_idx`. Written at ingest, keyed by `tx_hash`, and resolved by
/// `get_transaction` into a `TxEntry` via the tx materializer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxLocation {
    pub block_number: u64,
    pub tx_idx: u32,
}

impl TxLocation {
    pub const ENCODED_LEN: usize = 12;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&self.block_number.to_be_bytes());
        out[8..].copy_from_slice(&self.tx_idx.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; Self::ENCODED_LEN] = bytes
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("invalid tx_location length"))?;
        let block_number = u64::from_be_bytes(bytes[..8].try_into().expect("8-byte prefix"));
        let tx_idx = u32::from_be_bytes(bytes[8..].try_into().expect("4-byte suffix"));
        Ok(Self {
            block_number,
            tx_idx,
        })
    }
}

/// Per-tx fields stored in the block blob. Block-level fields
/// (block_number, block_hash, tx_idx) are reconstructed from the
/// `BlockRecord` and `BlockTxHeader` at read time.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StoredTxEnvelope {
    pub tx_hash: Hash32,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

impl StoredTxEnvelope {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid tx envelope rlp"))
    }

    pub fn into_tx_entry(self, block_number: u64, block_hash: Hash32, tx_idx: u32) -> TxEntry {
        TxEntry {
            block_number,
            block_hash,
            tx_idx,
            tx_hash: self.tx_hash,
            sender: self.sender,
            signed_tx_bytes: self.signed_tx_bytes,
        }
    }
}

/// Byte offsets into `block_tx_blob` for each `tx_idx`. Length is
/// `tx_count + 1`; the final sentinel is the total blob length.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockTxHeader {
    pub offsets: Vec<u32>,
}

impl BlockTxHeader {
    pub fn tx_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid tx header rlp"))
    }
}

pub(crate) fn decode_envelope(signed_tx_bytes: &[u8]) -> Result<TxEnvelope> {
    TxEnvelope::decode_2718(&mut &signed_tx_bytes[..])
        .map_err(|_| MonadChainDataError::Decode("invalid signed tx envelope"))
}

pub(crate) fn selector_from_envelope(envelope: &TxEnvelope) -> Option<[u8; 4]> {
    envelope.function_selector().map(|s| s.0)
}
