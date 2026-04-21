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

use std::{fmt::Debug, ops::Deref};

use ::serde::{Deserialize, Serialize};
use alloy_consensus::{
    crypto::{secp256k1, RecoveryError},
    transaction::{Recovered, SignerRecoverable, Transaction, TxHashRef},
    Header, ReceiptEnvelope, SignableTransaction, TxEnvelope,
};
use alloy_eips::{eip7702::RecoveredAuthorization, Decodable2718, Encodable2718, Typed2718};
use alloy_primitives::{keccak256, Address, Bytes, ChainId, FixedBytes, Signature, TxHash, TxKind, B256, U256};
use alloy_rlp::{
    encode_list, BytesMut, Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable,
    RlpEncodableWrapper,
};
use monad_crypto::NopPubKey;
use monad_types::{Balance, ExecutionProtocol, FinalizedHeader, LimitedVec, Nonce, SeqNum};
use serde_with::{serde_as, DisplayFromStr};

pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;
pub const MAX_TRANSACTIONS_PER_BLOCK: usize = 10000;
const MAX_OMMERS: usize = 0;
const MAX_WITHDRAWALS: usize = 0;

pub type EthAddress = [u8; 20];
pub type EthStorageKey = [u8; 32];
pub type EthCodeHash = [u8; 32];
pub type EthTxHash = [u8; 32];
pub type EthBlockHash = [u8; 32];
pub type EthStorageSlot = [u8; 32];
pub type EthCode = Vec<u8>;

pub const NAMESPACED_TX_PREFIX: u8 = 0x80;
pub const NAMESPACE_LENGTH: usize = 20;

#[derive(
    Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct AccountKey {
    pub namespace: Option<Address>,
    pub address: Address,
}

impl AccountKey {
    pub const fn global(address: Address) -> Self {
        Self {
            namespace: None,
            address,
        }
    }

    pub const fn namespaced(namespace: Address, address: Address) -> Self {
        Self {
            namespace: Some(namespace),
            address,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxEnvelope {
    Global(TxEnvelope),
    Namespaced {
        namespace: Address,
        tx: TxEnvelope,
        hash: TxHash,
    },
}

impl EthTxEnvelope {
    pub fn global(tx: TxEnvelope) -> Self {
        Self::Global(tx)
    }

    pub fn namespaced(namespace: Address, tx: TxEnvelope) -> Self {
        let hash = Self::compute_namespaced_tx_hash(namespace, &tx);
        Self::Namespaced {
            namespace,
            tx,
            hash,
        }
    }

    pub const fn namespace(&self) -> Option<Address> {
        match self {
            Self::Global(_) => None,
            Self::Namespaced { namespace, .. } => Some(*namespace),
        }
    }

    pub const fn is_namespaced(&self) -> bool {
        matches!(self, Self::Namespaced { .. })
    }

    pub const fn inner(&self) -> &TxEnvelope {
        match self {
            Self::Global(tx) | Self::Namespaced { tx, .. } => tx,
        }
    }

    pub fn into_inner(self) -> TxEnvelope {
        match self {
            Self::Global(tx) | Self::Namespaced { tx, .. } => tx,
        }
    }

    pub fn account_key(&self, signer: Address) -> AccountKey {
        AccountKey {
            namespace: self.namespace(),
            address: signer,
        }
    }

    pub fn signature_hash(&self) -> B256 {
        match self {
            Self::Global(tx) => tx.signature_hash(),
            Self::Namespaced { namespace, tx, .. } => {
                let mut payload = Vec::with_capacity(1 + NAMESPACE_LENGTH + tx.length());
                payload.push(NAMESPACED_TX_PREFIX);
                payload.extend_from_slice(namespace.as_ref());
                Self::encode_inner_for_signing(tx, &mut payload);
                keccak256(payload)
            }
        }
    }

    pub fn tx_hash(&self) -> &TxHash {
        match self {
            Self::Global(tx) => tx.tx_hash(),
            Self::Namespaced { hash, .. } => hash,
        }
    }

    pub fn hash(&self) -> &TxHash {
        self.tx_hash()
    }

    pub const fn signature(&self) -> &Signature {
        self.inner().signature()
    }

    pub const fn is_legacy(&self) -> bool {
        self.inner().is_legacy()
    }

    pub const fn is_eip2930(&self) -> bool {
        self.inner().is_eip2930()
    }

    pub const fn is_eip1559(&self) -> bool {
        self.inner().is_eip1559()
    }

    pub const fn is_eip4844(&self) -> bool {
        self.inner().is_eip4844()
    }

    pub const fn is_eip7702(&self) -> bool {
        self.inner().is_eip7702()
    }

    pub const fn as_eip7702(&self) -> Option<&alloy_consensus::Signed<alloy_consensus::TxEip7702>> {
        self.inner().as_eip7702()
    }

    pub fn eip2718_encoded_length(&self) -> usize {
        self.encode_2718_len()
    }

    pub fn encoded_2718(&self) -> Vec<u8> {
        Encodable2718::encoded_2718(self)
    }

    fn compute_namespaced_tx_hash(namespace: Address, tx: &TxEnvelope) -> TxHash {
        let mut encoded = Vec::with_capacity(1 + NAMESPACE_LENGTH + tx.encode_2718_len());
        encoded.push(NAMESPACED_TX_PREFIX);
        encoded.extend_from_slice(namespace.as_ref());
        tx.encode_2718(&mut encoded);
        keccak256(encoded)
    }

    fn encode_inner_for_signing(tx: &TxEnvelope, out: &mut dyn alloy_rlp::BufMut) {
        match tx {
            TxEnvelope::Legacy(tx) => tx.tx().encode_for_signing(out),
            TxEnvelope::Eip2930(tx) => tx.tx().encode_for_signing(out),
            TxEnvelope::Eip1559(tx) => tx.tx().encode_for_signing(out),
            TxEnvelope::Eip4844(tx) => tx.tx().encode_for_signing(out),
            TxEnvelope::Eip7702(tx) => tx.tx().encode_for_signing(out),
        }
    }
}

impl From<TxEnvelope> for EthTxEnvelope {
    fn from(value: TxEnvelope) -> Self {
        Self::global(value)
    }
}

impl Deref for EthTxEnvelope {
    type Target = TxEnvelope;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl Typed2718 for EthTxEnvelope {
    fn ty(&self) -> u8 {
        match self {
            Self::Global(tx) => tx.ty(),
            Self::Namespaced { .. } => NAMESPACED_TX_PREFIX,
        }
    }
}

impl Encodable2718 for EthTxEnvelope {
    fn encode_2718_len(&self) -> usize {
        match self {
            Self::Global(tx) => tx.encode_2718_len(),
            Self::Namespaced { tx, .. } => 1 + NAMESPACE_LENGTH + tx.encode_2718_len(),
        }
    }

    fn encode_2718(&self, out: &mut dyn alloy_rlp::BufMut) {
        match self {
            Self::Global(tx) => tx.encode_2718(out),
            Self::Namespaced { namespace, tx, .. } => {
                out.put_u8(NAMESPACED_TX_PREFIX);
                out.put_slice(namespace.as_ref());
                tx.encode_2718(out);
            }
        }
    }
}

impl Decodable2718 for EthTxEnvelope {
    fn typed_decode(ty: u8, buf: &mut &[u8]) -> alloy_eips::eip2718::Eip2718Result<Self> {
        TxEnvelope::typed_decode(ty, buf).map(Self::global)
    }

    fn fallback_decode(buf: &mut &[u8]) -> alloy_eips::eip2718::Eip2718Result<Self> {
        TxEnvelope::fallback_decode(buf).map(Self::global)
    }

    fn decode_2718(buf: &mut &[u8]) -> alloy_eips::eip2718::Eip2718Result<Self> {
        if buf.first().copied() == Some(NAMESPACED_TX_PREFIX) {
            if buf.len() < 1 + NAMESPACE_LENGTH {
                return Err(alloy_rlp::Error::InputTooShort.into());
            }

            *buf = &buf[1..];
            let namespace = Address::from_slice(&buf[..NAMESPACE_LENGTH]);
            *buf = &buf[NAMESPACE_LENGTH..];
            let tx = TxEnvelope::decode_2718(buf)?;
            return Ok(Self::namespaced(namespace, tx));
        }

        TxEnvelope::decode_2718(buf).map(Self::global)
    }
}

impl Encodable for EthTxEnvelope {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.encode_2718(out);
    }

    fn length(&self) -> usize {
        self.encode_2718_len()
    }
}

impl Decodable for EthTxEnvelope {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Self::decode_2718(buf).map_err(Into::into)
    }
}

impl TxHashRef for EthTxEnvelope {
    fn tx_hash(&self) -> &TxHash {
        self.tx_hash()
    }
}

impl SignerRecoverable for EthTxEnvelope {
    fn recover_signer(&self) -> Result<Address, RecoveryError> {
        match self {
            Self::Global(tx) => tx.recover_signer(),
            Self::Namespaced { .. } => {
                secp256k1::recover_signer(self.signature(), self.signature_hash())
            }
        }
    }

    fn recover_signer_unchecked(&self) -> Result<Address, RecoveryError> {
        match self {
            Self::Global(tx) => tx.recover_signer_unchecked(),
            Self::Namespaced { .. } => {
                secp256k1::recover_signer_unchecked(self.signature(), self.signature_hash())
            }
        }
    }
}

impl Transaction for EthTxEnvelope {
    fn chain_id(&self) -> Option<ChainId> {
        self.inner().chain_id()
    }

    fn nonce(&self) -> u64 {
        self.inner().nonce()
    }

    fn gas_limit(&self) -> u64 {
        self.inner().gas_limit()
    }

    fn gas_price(&self) -> Option<u128> {
        self.inner().gas_price()
    }

    fn max_fee_per_gas(&self) -> u128 {
        self.inner().max_fee_per_gas()
    }

    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        self.inner().max_priority_fee_per_gas()
    }

    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        self.inner().max_fee_per_blob_gas()
    }

    fn priority_fee_or_price(&self) -> u128 {
        self.inner().priority_fee_or_price()
    }

    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        self.inner().effective_gas_price(base_fee)
    }

    fn is_dynamic_fee(&self) -> bool {
        self.inner().is_dynamic_fee()
    }

    fn kind(&self) -> TxKind {
        self.inner().kind()
    }

    fn is_create(&self) -> bool {
        self.inner().is_create()
    }

    fn value(&self) -> U256 {
        self.inner().value()
    }

    fn input(&self) -> &Bytes {
        self.inner().input()
    }

    fn access_list(&self) -> Option<&alloy_eips::eip2930::AccessList> {
        self.inner().access_list()
    }

    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        self.inner().blob_versioned_hashes()
    }

    fn authorization_list(&self) -> Option<&[alloy_eips::eip7702::SignedAuthorization]> {
        self.inner().authorization_list()
    }
}

pub trait ExtractEthAddress {
    fn get_eth_address(&self) -> Address;
}

impl ExtractEthAddress for NopPubKey {
    fn get_eth_address(&self) -> Address {
        Address::new([0_u8; 20])
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct EthAccount {
    pub nonce: Nonce,
    pub balance: Balance,
    pub code_hash: Option<EthCodeHash>,
    pub is_delegated: bool,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProposedEthHeader {
    #[serde_as(as = "serde_with::hex::Hex")]
    pub ommers_hash: [u8; 32],
    pub beneficiary: Address,
    #[serde_as(as = "serde_with::hex::Hex")]
    pub transactions_root: [u8; 32],
    #[serde_as(as = "DisplayFromStr")]
    pub difficulty: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub number: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub gas_limit: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub timestamp: u64,
    #[serde_as(as = "serde_with::hex::Hex")]
    pub extra_data: [u8; 32],
    #[serde_as(as = "serde_with::hex::Hex")]
    pub mix_hash: [u8; 32],
    #[serde_as(as = "serde_with::hex::Hex")]
    pub nonce: [u8; 8],
    #[serde_as(as = "DisplayFromStr")]
    pub base_fee_per_gas: u64,
    #[serde_as(as = "serde_with::hex::Hex")]
    pub withdrawals_root: [u8; 32],
    // cancun
    #[serde_as(as = "DisplayFromStr")]
    pub blob_gas_used: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub excess_blob_gas: u64,
    #[serde_as(as = "serde_with::hex::Hex")]
    pub parent_beacon_block_root: [u8; 32],
    // eip-7685
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    pub requests_hash: Option<[u8; 32]>,
}

impl ProposedEthHeader {
    fn header_payload_length(&self) -> usize {
        let mut length = 0;
        length += self.ommers_hash.length();
        length += self.beneficiary.length();
        length += self.transactions_root.length();
        length += self.difficulty.length();
        length += self.number.length();
        length += self.gas_limit.length();
        length += self.timestamp.length();
        length += self.extra_data.length();
        length += self.mix_hash.length();
        length += self.nonce.length();
        length += self.base_fee_per_gas.length();
        length += self.withdrawals_root.length();
        length += self.blob_gas_used.length();
        length += self.excess_blob_gas.length();
        length += self.parent_beacon_block_root.length();

        if let Some(requests_hash) = &self.requests_hash {
            length += requests_hash.length();
        }

        length
    }
}

impl Encodable for ProposedEthHeader {
    fn length(&self) -> usize {
        let mut length = 0;
        length += self.header_payload_length();
        length += alloy_rlp::length_of_length(length);
        length
    }

    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let list_header = alloy_rlp::Header {
            list: true,
            payload_length: self.header_payload_length(),
        };
        list_header.encode(out);
        self.ommers_hash.encode(out);
        self.beneficiary.encode(out);
        self.transactions_root.encode(out);
        self.difficulty.encode(out);
        self.number.encode(out);
        self.gas_limit.encode(out);
        self.timestamp.encode(out);
        self.extra_data.encode(out);
        self.mix_hash.encode(out);
        self.nonce.encode(out);
        self.base_fee_per_gas.encode(out);
        self.withdrawals_root.encode(out);
        self.blob_gas_used.encode(out);
        self.excess_blob_gas.encode(out);
        self.parent_beacon_block_root.encode(out);

        if let Some(requests_hash) = &self.requests_hash {
            requests_hash.encode(out);
        }
    }
}

impl Decodable for ProposedEthHeader {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let rlp_header = alloy_rlp::Header::decode(buf)?;
        if !rlp_header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }
        let starting_len = buf.len();
        let mut this = Self {
            ommers_hash: Decodable::decode(buf)?,
            beneficiary: Decodable::decode(buf)?,
            transactions_root: Decodable::decode(buf)?,
            difficulty: Decodable::decode(buf)?,
            number: Decodable::decode(buf)?,
            gas_limit: Decodable::decode(buf)?,
            timestamp: Decodable::decode(buf)?,
            extra_data: Decodable::decode(buf)?,
            mix_hash: Decodable::decode(buf)?,
            nonce: Decodable::decode(buf)?,
            base_fee_per_gas: Decodable::decode(buf)?,
            withdrawals_root: Decodable::decode(buf)?,
            blob_gas_used: Decodable::decode(buf)?,
            excess_blob_gas: Decodable::decode(buf)?,
            parent_beacon_block_root: Decodable::decode(buf)?,
            requests_hash: None,
        };

        if starting_len - buf.len() < rlp_header.payload_length {
            this.requests_hash = Some(Decodable::decode(buf)?);
        }

        let consumed = starting_len - buf.len();
        if consumed != rlp_header.payload_length {
            return Err(alloy_rlp::Error::ListLengthMismatch {
                expected: rlp_header.payload_length,
                got: consumed,
            });
        }

        Ok(this)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper, Serialize, Deserialize,
)]
pub struct EthHeader(pub Header);

impl FinalizedHeader for EthHeader {
    fn seq_num(&self) -> SeqNum {
        SeqNum(self.0.number)
    }
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize, Default)]
pub struct EthBlockBody {
    // TODO consider storing recovered txs inline here
    pub transactions: LimitedVec<EthTxEnvelope, MAX_TRANSACTIONS_PER_BLOCK>,
    pub ommers: LimitedVec<Ommer, MAX_OMMERS>,
    pub withdrawals: LimitedVec<Withdrawal, MAX_WITHDRAWALS>,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
pub struct Ommer {}
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
pub struct Withdrawal {}

impl Debug for EthBlockBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthBlockBody")
            .field("num_txns", &format!("{}", self.transactions.len()))
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, RlpEncodable, RlpDecodable, Serialize)]
pub struct EthExecutionProtocol;
impl ExecutionProtocol for EthExecutionProtocol {
    type ProposedHeader = ProposedEthHeader;
    type FinalizedHeader = EthHeader;
    type Body = EthBlockBody;
}

#[derive(Clone, Debug)]
pub struct ValidatedTx {
    pub tx: Recovered<EthTxEnvelope>,
    pub authorizations_7702: Vec<RecoveredAuthorization>,
}

impl Deref for ValidatedTx {
    type Target = Recovered<EthTxEnvelope>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockHeader {
    pub hash: FixedBytes<32>,
    pub header: Header,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TransactionLocation {
    pub tx_index: u64,
    pub block_num: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptWithLogIndex {
    pub receipt: ReceiptEnvelope,
    pub starting_log_index: u64,
}

impl Encodable for ReceiptWithLogIndex {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let mut encoded_receipt: BytesMut = BytesMut::new();
        self.receipt.encode(&mut encoded_receipt);
        let enc: [&dyn Encodable; 2] = [&encoded_receipt, &self.starting_log_index];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for ReceiptWithLogIndex {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if !list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if list {
            return Err(alloy_rlp::Error::UnexpectedList);
        }
        let receipt = ReceiptEnvelope::decode(buf)?;
        let starting_log_index = u64::decode(buf)?;

        Ok(ReceiptWithLogIndex {
            receipt,
            starting_log_index,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxEnvelopeWithSender {
    pub tx: TxEnvelope,
    pub sender: Address,
}

impl Encodable for TxEnvelopeWithSender {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let mut encoded_tx: BytesMut = BytesMut::new();
        self.tx.encode(&mut encoded_tx);
        let enc: [&dyn Encodable; 2] = [&encoded_tx, &self.sender];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for TxEnvelopeWithSender {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if !list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if list {
            return Err(alloy_rlp::Error::UnexpectedList);
        }
        let tx = TxEnvelope::decode(buf)?;
        let sender = Address::decode(buf)?;

        Ok(TxEnvelopeWithSender { tx, sender })
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::{
        constants::{EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        proofs::calculate_transaction_root,
        transaction::SignerRecoverable,
        SignableTransaction, TxLegacy,
        EMPTY_OMMER_ROOT_HASH,
    };
    use alloy_eips::Decodable2718;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    use super::*;

    fn make_legacy_tx(sender: B256, nonce: u64) -> TxEnvelope {
        let transaction = TxLegacy {
            chain_id: Some(1337),
            nonce,
            gas_price: 100,
            gas_limit: 21_000,
            to: TxKind::Call(Address::repeat_byte(0x11)),
            value: U256::from(7_u64),
            input: vec![0x44, 0x55].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();

        transaction.into_signed(signature).into()
    }

    fn make_namespaced_legacy_tx(namespace: Address, sender: B256, nonce: u64) -> EthTxEnvelope {
        let transaction = TxLegacy {
            chain_id: Some(1337),
            nonce,
            gas_price: 100,
            gas_limit: 21_000,
            to: TxKind::Call(Address::repeat_byte(0x22)),
            value: U256::from(9_u64),
            input: vec![0x66, 0x77].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let mut payload = Vec::new();
        payload.push(NAMESPACED_TX_PREFIX);
        payload.extend_from_slice(namespace.as_ref());
        transaction.encode_for_signing(&mut payload);
        let signature = signer.sign_hash_sync(&keccak256(payload)).unwrap();

        EthTxEnvelope::namespaced(namespace, transaction.into_signed(signature).into())
    }

    #[derive(Debug, RlpEncodable, RlpDecodable)]
    struct ProposedEthHeaderCancun {
        pub ommers_hash: [u8; 32],
        pub beneficiary: Address,
        pub transactions_root: [u8; 32],
        pub difficulty: u64,
        pub number: u64,
        pub gas_limit: u64,
        pub timestamp: u64,
        pub extra_data: [u8; 32],
        pub mix_hash: [u8; 32],
        pub nonce: [u8; 8],
        pub base_fee_per_gas: u64,
        pub withdrawals_root: [u8; 32],
        // cancun
        pub blob_gas_used: u64,
        pub excess_blob_gas: u64,
        pub parent_beacon_block_root: [u8; 32],
    }

    #[test]
    fn test_proposed_eth_header_backward_compat() {
        let old_header = ProposedEthHeaderCancun {
            ommers_hash: *EMPTY_OMMER_ROOT_HASH,
            beneficiary: Address::new([0xff_u8; 20]),
            transactions_root: *EMPTY_TRANSACTIONS,
            difficulty: 0,
            number: 72,
            gas_limit: 150_000_000,
            timestamp: 1756656000,
            extra_data: [0_u8; 32],
            mix_hash: [0xff_u8; 32],
            nonce: [0_u8; 8],
            base_fee_per_gas: 100_000_000_000,
            withdrawals_root: *EMPTY_WITHDRAWALS,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
        };

        let encoded = alloy_rlp::encode(&old_header);
        let new_header: ProposedEthHeader = alloy_rlp::decode_exact(&encoded).unwrap();

        assert_eq!(new_header.ommers_hash, old_header.ommers_hash);
        assert_eq!(new_header.beneficiary, old_header.beneficiary);
        assert_eq!(new_header.transactions_root, old_header.transactions_root);
        assert_eq!(new_header.difficulty, old_header.difficulty);
        assert_eq!(new_header.number, old_header.number);
        assert_eq!(new_header.gas_limit, old_header.gas_limit);
        assert_eq!(new_header.timestamp, old_header.timestamp);
        assert_eq!(new_header.extra_data, old_header.extra_data);
        assert_eq!(new_header.mix_hash, old_header.mix_hash);
        assert_eq!(new_header.nonce, old_header.nonce);
        assert_eq!(new_header.base_fee_per_gas, old_header.base_fee_per_gas);
        assert_eq!(new_header.withdrawals_root, old_header.withdrawals_root);
        assert_eq!(new_header.blob_gas_used, old_header.blob_gas_used);
        assert_eq!(new_header.excess_blob_gas, old_header.excess_blob_gas);
        assert_eq!(
            new_header.parent_beacon_block_root,
            old_header.parent_beacon_block_root
        );
        assert_eq!(new_header.requests_hash, None);

        // new encoding with requests_hash == None can be decoded as old header

        let new_header = ProposedEthHeader {
            ommers_hash: *EMPTY_OMMER_ROOT_HASH,
            beneficiary: Address::new([0xff_u8; 20]),
            transactions_root: *EMPTY_TRANSACTIONS,
            difficulty: 0,
            number: 72,
            gas_limit: 150_000_000,
            timestamp: 1756656000,
            extra_data: [0_u8; 32],
            mix_hash: [0xff_u8; 32],
            nonce: [0_u8; 8],
            base_fee_per_gas: 100_000_000_000,
            withdrawals_root: *EMPTY_WITHDRAWALS,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
            requests_hash: None,
        };

        let encoded = alloy_rlp::encode(&new_header);
        let old_header: ProposedEthHeaderCancun = alloy_rlp::decode_exact(&encoded).unwrap();

        assert_eq!(new_header.ommers_hash, old_header.ommers_hash);
        assert_eq!(new_header.beneficiary, old_header.beneficiary);
        assert_eq!(new_header.transactions_root, old_header.transactions_root);
        assert_eq!(new_header.difficulty, old_header.difficulty);
        assert_eq!(new_header.number, old_header.number);
        assert_eq!(new_header.gas_limit, old_header.gas_limit);
        assert_eq!(new_header.timestamp, old_header.timestamp);
        assert_eq!(new_header.extra_data, old_header.extra_data);
        assert_eq!(new_header.mix_hash, old_header.mix_hash);
        assert_eq!(new_header.nonce, old_header.nonce);
        assert_eq!(new_header.base_fee_per_gas, old_header.base_fee_per_gas);
        assert_eq!(new_header.withdrawals_root, old_header.withdrawals_root);
        assert_eq!(new_header.blob_gas_used, old_header.blob_gas_used);
        assert_eq!(new_header.excess_blob_gas, old_header.excess_blob_gas);
        assert_eq!(
            new_header.parent_beacon_block_root,
            old_header.parent_beacon_block_root
        );
    }

    #[test]
    fn test_proposed_eth_header_toml_roundtrip_u64_max() {
        let header = ProposedEthHeader {
            ommers_hash: *EMPTY_OMMER_ROOT_HASH,
            beneficiary: Address::new([0xff_u8; 20]),
            transactions_root: *EMPTY_TRANSACTIONS,
            difficulty: u64::MAX,
            number: u64::MAX,
            gas_limit: u64::MAX,
            timestamp: u64::MAX,
            extra_data: [0_u8; 32],
            mix_hash: [0xff_u8; 32],
            nonce: [0_u8; 8],
            base_fee_per_gas: u64::MAX,
            withdrawals_root: *EMPTY_WITHDRAWALS,
            blob_gas_used: u64::MAX,
            excess_blob_gas: u64::MAX,
            parent_beacon_block_root: [0_u8; 32],
            requests_hash: None,
        };

        let encoded = toml::to_string_pretty(&header).unwrap();
        let decoded: ProposedEthHeader = toml::from_str(&encoded).unwrap();

        let re_encoded = toml::to_string_pretty(&decoded).unwrap();
        assert_eq!(re_encoded, encoded);
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_eth_tx_envelope_roundtrip_global_and_namespaced() {
        let global = EthTxEnvelope::global(make_legacy_tx(B256::repeat_byte(0x11), 0));
        let namespaced = make_namespaced_legacy_tx(Address::repeat_byte(0xaa), B256::repeat_byte(0x22), 1);

        for tx in [global, namespaced] {
            let encoded = tx.encoded_2718();
            let decoded = EthTxEnvelope::decode_2718_exact(&encoded).unwrap();
            assert_eq!(decoded, tx);
        }
    }

    #[test]
    fn test_namespaced_signature_recovery_is_namespace_bound() {
        let namespace = Address::repeat_byte(0xaa);
        let other_namespace = Address::repeat_byte(0xbb);
        let tx = make_namespaced_legacy_tx(namespace, B256::repeat_byte(0x33), 0);

        let signer = tx.recover_signer().unwrap();
        let rebound = EthTxEnvelope::namespaced(other_namespace, tx.inner().clone());
        let rebound_signer = rebound.recover_signer().unwrap();

        assert_ne!(tx.signature_hash(), rebound.signature_hash());
        assert_ne!(signer, rebound_signer);
    }

    #[test]
    fn test_namespaced_hash_and_transaction_root_include_namespace_prefix() {
        let namespace = Address::repeat_byte(0xcc);
        let inner = make_legacy_tx(B256::repeat_byte(0x44), 2);
        let global = EthTxEnvelope::global(inner.clone());
        let namespaced = EthTxEnvelope::namespaced(namespace, inner);
        let encoded = namespaced.encoded_2718();

        assert_eq!(encoded[0], NAMESPACED_TX_PREFIX);
        assert_eq!(&encoded[1..1 + NAMESPACE_LENGTH], namespace.0.as_slice());
        assert_eq!(*namespaced.tx_hash(), keccak256(encoded.clone()));
        assert_ne!(
            calculate_transaction_root(&[global]),
            calculate_transaction_root(&[namespaced.clone()])
        );

        let decoded = EthTxEnvelope::decode_2718_exact(&encoded).unwrap();
        assert_eq!(
            calculate_transaction_root(&[decoded]),
            calculate_transaction_root(&[namespaced])
        );
    }
}
