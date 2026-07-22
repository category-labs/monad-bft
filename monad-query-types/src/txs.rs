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

use alloy_consensus::{
    transaction::RlpEcdsaDecodableTx, Signed, Transaction, TxEip1559, TxEip2930, TxEip4844Variant,
    TxEip7702, TxEnvelope, TxLegacy,
};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, Bytes};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_errors::{QueryError, Result};
use monad_query_primitives::Hash32;

/// Public, owned per-transaction view with pre-decoded envelope (hash pre-seeded from stored value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub tx_hash: Hash32,
    pub sender: Address,
    pub envelope: TxEnvelope,
}

impl TxEntry {
    /// Recipient address (None for contract creation).
    pub fn to(&self) -> Option<Address> {
        self.envelope.to()
    }

    /// Extracts 4-byte function selector from tx calldata.
    pub fn selector(&self) -> Option<[u8; 4]> {
        selector_from_envelope(&self.envelope)
    }

    /// Converts to RPC transaction format. `effective_gas_price` is None;
    /// caller must populate from block base fee.
    #[cfg(feature = "alloy-rpc-types-eth")]
    pub fn to_rpc_transaction(&self) -> alloy_rpc_types_eth::Transaction {
        use alloy_consensus::transaction::Recovered;

        alloy_rpc_types_eth::Transaction {
            inner: Recovered::new_unchecked(self.envelope.clone(), self.sender),
            block_hash: Some(self.block_hash),
            block_number: Some(self.block_number),
            transaction_index: Some(u64::from(self.tx_index)),
            effective_gas_price: None,
            block_timestamp: None,
        }
    }
}

/// Fixed 12-byte tx location (8-byte BE block_number + 4-byte BE tx_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxLocation {
    pub block_number: u64,
    pub tx_index: u32,
}

impl TxLocation {
    pub const ENCODED_LEN: usize = 12;

    /// Encodes to 12 big-endian bytes.
    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut encoded = [0u8; Self::ENCODED_LEN];
        encoded[..8].copy_from_slice(&self.block_number.to_be_bytes());
        encoded[8..].copy_from_slice(&self.tx_index.to_be_bytes());
        encoded
    }

    /// Decodes from 12 big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let encoded_bytes: [u8; Self::ENCODED_LEN] = bytes
            .try_into()
            .map_err(|_| QueryError::Decode("invalid tx_location length"))?;
        let block_number =
            u64::from_be_bytes(encoded_bytes[..8].try_into().expect("8-byte prefix"));
        let tx_index = u32::from_be_bytes(encoded_bytes[8..].try_into().expect("4-byte suffix"));
        Ok(Self {
            block_number,
            tx_index,
        })
    }
}

/// Per-tx fields stored in block blob (block-level fields reconstructed at read time).
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StoredTxEnvelope {
    pub tx_hash: Hash32,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

impl StoredTxEnvelope {
    /// RLP-encodes this tx envelope.
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    /// RLP-decodes a tx envelope from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes).map_err(|_| QueryError::Decode("invalid tx envelope rlp"))
    }

    pub fn into_tx_entry(
        self,
        block_number: u64,
        block_hash: Hash32,
        tx_index: u32,
    ) -> Result<TxEntry> {
        let envelope = decode_envelope_with_hash(&self.signed_tx_bytes, self.tx_hash)?;
        Ok(TxEntry {
            block_number,
            block_hash,
            tx_index,
            tx_hash: self.tx_hash,
            sender: self.sender,
            envelope,
        })
    }
}

pub fn decode_envelope(signed_tx_bytes: &[u8]) -> Result<TxEnvelope> {
    TxEnvelope::decode_2718(&mut &signed_tx_bytes[..])
        .map_err(|_| QueryError::Decode("invalid signed tx envelope"))
}

/// Decodes a signed-tx envelope using a pre-validated hash instead of re-hashing.
/// Avoids expensive keccak computation on dense transaction pages.
fn decode_envelope_with_hash(signed_tx_bytes: &[u8], tx_hash: Hash32) -> Result<TxEnvelope> {
    let mut buf = signed_tx_bytes
        .get(1..)
        .ok_or(QueryError::Decode("invalid signed tx envelope"))?;

    let tx_type = signed_tx_bytes[0];
    let err = || QueryError::Decode("invalid signed tx envelope");

    match tx_type {
        0x01 => {
            let (tx, sig) = TxEip2930::rlp_decode_with_signature(&mut buf).map_err(|_| err())?;
            Ok(TxEnvelope::Eip2930(Signed::new_unchecked(tx, sig, tx_hash)))
        }
        0x02 => {
            let (tx, sig) = TxEip1559::rlp_decode_with_signature(&mut buf).map_err(|_| err())?;
            Ok(TxEnvelope::Eip1559(Signed::new_unchecked(tx, sig, tx_hash)))
        }
        0x03 => {
            let (tx, sig) =
                TxEip4844Variant::rlp_decode_with_signature(&mut buf).map_err(|_| err())?;
            Ok(TxEnvelope::Eip4844(Signed::new_unchecked(tx, sig, tx_hash)))
        }
        0x04 => {
            let (tx, sig) = TxEip7702::rlp_decode_with_signature(&mut buf).map_err(|_| err())?;
            Ok(TxEnvelope::Eip7702(Signed::new_unchecked(tx, sig, tx_hash)))
        }
        // Legacy txs have no type-prefix byte — decode from the full bytes.
        b if b >= 0xc0 => {
            let (tx, sig) = TxLegacy::rlp_decode_with_signature(&mut &signed_tx_bytes[..])
                .map_err(|_| err())?;
            Ok(TxEnvelope::Legacy(Signed::new_unchecked(tx, sig, tx_hash)))
        }
        _ => Err(err()),
    }
}

/// Extracts 4-byte function selector from transaction envelope.
pub fn selector_from_envelope(envelope: &TxEnvelope) -> Option<[u8; 4]> {
    envelope.function_selector().map(|selector| selector.0)
}
