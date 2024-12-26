use ::serde::{Deserialize, Serialize};
use alloy_primitives::{Address, FixedBytes, TxHash, B256};
use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use reth_primitives::{TransactionSigned, TransactionSignedEcRecovered};

pub mod serde;

// TODO: currently base fee is hardcoded
// this will not be needed anymore when base fee is calculated according to EIP1559
pub const BASE_FEE_PER_GAS: u64 = 1000;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

pub type Nonce = u64;
pub type Balance = u128;

// FIXME reth types shouldn't be leaked
/// A 20-byte Eth address
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub struct EthAddress(pub Address);

impl EthAddress {
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(Address(FixedBytes(bytes)))
    }
}

impl AsRef<[u8]> for EthAddress {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsRef<[u8; 20]> for EthAddress {
    fn as_ref(&self) -> &[u8; 20] {
        &self.0 .0 .0
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EthAccount {
    pub nonce: Nonce,
    pub balance: Balance,
    pub code_hash: Option<B256>,
}

pub type EthTxHash = TxHash;
// FIXME reth types shouldn't be leaked
pub type EthTransaction = TransactionSignedEcRecovered;
pub type EthSignedTransaction = TransactionSigned;

/// A list of Eth transaction hash
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EthTransactionList(pub Vec<EthTxHash>);

impl EthTransactionList {
    /// rlp encode EthTransactionList as a rlp list
    pub fn rlp_encode(self) -> Bytes {
        let mut buf = BytesMut::new();

        self.0.encode(&mut buf);

        buf.into()
    }

    pub fn rlp_decode(rlp_data: Bytes) -> Result<Self, alloy_rlp::Error> {
        Vec::<EthTxHash>::decode(&mut rlp_data.as_ref()).map(Self)
    }
}

/// A list of signed Eth transaction with recovered signer
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EthFullTransactionList(pub Vec<EthTransaction>);

impl EthFullTransactionList {
    /// rlp encode EthFullTransactionList as a rlp list
    pub fn rlp_encode(self) -> Bytes {
        let mut buf = BytesMut::default();

        self.0.encode(&mut buf);

        buf.into()
    }

    pub fn rlp_decode(rlp_data: Bytes) -> Result<Self, alloy_rlp::Error> {
        Vec::<EthTransaction>::decode(&mut rlp_data.as_ref()).map(Self)
    }

    /// Get a list of tx hashes of all the transactions in this list
    pub fn get_hashes(self) -> Vec<EthTxHash> {
        self.0.iter().map(|x| x.hash()).collect()
    }
}
