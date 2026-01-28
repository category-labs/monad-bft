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
use alloy_consensus::{transaction::Recovered, Header, TxEnvelope};
use alloy_eips::eip7702::RecoveredAuthorization;
use alloy_primitives::{b256, Address, FixedBytes, B256};
use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};
use monad_crypto::NopPubKey;
use monad_types::{Balance, ExecutionProtocol, FinalizedHeader, Nonce, SeqNum};
use serde_with::{serde_as, DisplayFromStr};
use tracing::warn;

pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

pub const EIP7702_DELEGATION_PREFIX: [u8; 3] = [0xef, 0x01, 0x00];
pub const NULL_HASH: B256 =
    b256!("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
const HASH_LENGTH: usize = 32;
const INLINE_CODE_LENGTH: usize = 23;

pub trait ExtractEthAddress {
    fn get_eth_address(&self) -> Address;
}

impl ExtractEthAddress for NopPubKey {
    fn get_eth_address(&self) -> Address {
        Address::new([0_u8; 20])
    }
}

#[derive(Debug, Copy, Clone)]
pub enum AccountCodeOrHash {
    InlineCode(FixedBytes<INLINE_CODE_LENGTH>), // code for delegated per EIP7702 accounts.
    CodeHash(B256),
    IsEmpty,
}

impl Decodable for AccountCodeOrHash {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        const RLP_HASH_LENGTH: usize = HASH_LENGTH + 1;
        const RLP_INLINE_CODE_LENGTH: usize = INLINE_CODE_LENGTH + 1;

        let account: AccountCodeOrHash = match buf.len() {
            0 => AccountCodeOrHash::IsEmpty,
            RLP_HASH_LENGTH => match <[u8; HASH_LENGTH]>::decode(buf) {
                Ok(x) => AccountCodeOrHash::CodeHash(B256::from(x)),
                Err(e) => {
                    warn!("rlp code_hash decode failed: {:?}", e);
                    return Err(alloy_rlp::Error::Custom("code_hash decode failed"));
                }
            },
            RLP_INLINE_CODE_LENGTH => match <[u8; INLINE_CODE_LENGTH]>::decode(buf) {
                Ok(x) => {
                    if x[..3] != EIP7702_DELEGATION_PREFIX {
                        return Err(alloy_rlp::Error::Custom("unexpected inline code prefix"));
                    }
                    AccountCodeOrHash::InlineCode(FixedBytes::<INLINE_CODE_LENGTH>::from(x))
                }
                Err(e) => {
                    warn!("rlp inline code decode failed: {:?}", e);
                    return Err(alloy_rlp::Error::Custom("inline code decode failed"));
                }
            },
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "stored code_hash length is unexpected",
                ));
            }
        };

        Ok(account)
    }
}

impl Default for AccountCodeOrHash {
    fn default() -> Self {
        AccountCodeOrHash::IsEmpty
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EthAccount {
    pub nonce: Nonce,
    pub balance: Balance,
    pub code_or_hash: AccountCodeOrHash,
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
    pub transactions: Vec<TxEnvelope>,
    pub ommers: Vec<Ommer>,
    pub withdrawals: Vec<Withdrawal>,
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
    pub tx: Recovered<TxEnvelope>,
    pub authorizations_7702: Vec<RecoveredAuthorization>,
}

impl Deref for ValidatedTx {
    type Target = Recovered<TxEnvelope>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::{
        constants::{EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        EMPTY_OMMER_ROOT_HASH,
    };

    use super::*;

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
}
