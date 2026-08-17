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

use crate::{
    error::{MonadChainDataError, Result},
    ingest_types::Hash32,
};

/// Public, owned per-transaction view returned by queries. Carries the
/// envelope already decoded (hash seeded from the stored `tx_hash`), so
/// filter checks and RPC conversion never re-parse or re-hash the raw bytes.
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
    /// Recipient address; `None` for contract-creation transactions.
    pub fn to(&self) -> Option<Address> {
        self.envelope.to()
    }

    /// 4-byte function selector; `None` for contract-creation transactions
    /// or when the calldata is shorter than 4 bytes.
    pub fn selector(&self) -> Option<[u8; 4]> {
        selector_from_envelope(&self.envelope)
    }

    /// Assembles the queryX-spec `alloy_rpc_types_eth::Transaction` shape.
    /// `effective_gas_price` is left `None` (needs the block's base fee,
    /// not carried here); RPC layers must populate it from the header via
    /// `envelope.effective_gas_price(Some(base_fee))`.
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

/// Fixed 12-byte tx location: 8-byte BE `block_number` then 4-byte BE
/// `tx_index`. Written at ingest into `tx_hash_index`, keyed by `tx_hash`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxLocation {
    pub block_number: u64,
    pub tx_index: u32,
}

impl TxLocation {
    pub const ENCODED_LEN: usize = 12;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&self.block_number.to_be_bytes());
        out[8..].copy_from_slice(&self.tx_index.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; Self::ENCODED_LEN] = bytes
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("invalid tx_location length"))?;
        let block_number = u64::from_be_bytes(bytes[..8].try_into().expect("8-byte prefix"));
        let tx_index = u32::from_be_bytes(bytes[8..].try_into().expect("4-byte suffix"));
        Ok(Self {
            block_number,
            tx_index,
        })
    }
}

/// Per-tx fields stored in the block blob. Block-level fields
/// (block_number, block_hash, tx_index) are reconstructed from the
/// `BlockRecord` and `BlockBlobHeader` at read time.
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

pub(crate) fn decode_envelope(signed_tx_bytes: &[u8]) -> Result<TxEnvelope> {
    TxEnvelope::decode_2718(&mut &signed_tx_bytes[..])
        .map_err(|_| MonadChainDataError::Decode("invalid signed tx envelope"))
}

/// Decodes a signed-tx envelope, seeding the signed hash from the stored
/// `tx_hash` (ingest-validated) instead of re-hashing the payload.
/// `TxEnvelope::decode_2718` keccaks the full byte span to recover the hash
/// we already carry — on dense queryTransactions pages that hashing alone
/// was ~18% of process CPU.
fn decode_envelope_with_hash(signed_tx_bytes: &[u8], tx_hash: Hash32) -> Result<TxEnvelope> {
    fn signed<T: RlpEcdsaDecodableTx>(buf: &mut &[u8], tx_hash: Hash32) -> Result<Signed<T>> {
        let (tx, signature) = T::rlp_decode_with_signature(buf)
            .map_err(|_| MonadChainDataError::Decode("invalid signed tx envelope"))?;
        Ok(Signed::new_unchecked(tx, signature, tx_hash))
    }

    let buf = &mut &signed_tx_bytes[..];
    match signed_tx_bytes.first() {
        Some(0x01) => {
            *buf = &buf[1..];
            Ok(TxEnvelope::Eip2930(signed::<TxEip2930>(buf, tx_hash)?))
        }
        Some(0x02) => {
            *buf = &buf[1..];
            Ok(TxEnvelope::Eip1559(signed::<TxEip1559>(buf, tx_hash)?))
        }
        Some(0x03) => {
            *buf = &buf[1..];
            Ok(TxEnvelope::Eip4844(signed::<TxEip4844Variant>(
                buf, tx_hash,
            )?))
        }
        Some(0x04) => {
            *buf = &buf[1..];
            Ok(TxEnvelope::Eip7702(signed::<TxEip7702>(buf, tx_hash)?))
        }
        // Legacy txs start with their RLP list header.
        Some(byte) if *byte >= 0xc0 => Ok(TxEnvelope::Legacy(signed::<TxLegacy>(buf, tx_hash)?)),
        _ => Err(MonadChainDataError::Decode("invalid signed tx envelope")),
    }
}

pub(crate) fn selector_from_envelope(envelope: &TxEnvelope) -> Option<[u8; 4]> {
    envelope.function_selector().map(|s| s.0)
}
