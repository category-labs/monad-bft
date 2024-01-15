use alloy_primitives::{Address, FixedBytes, TxHash};
use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use reth_primitives::TransactionSignedEcRecovered;

#[cfg(feature = "serde")]
pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

pub type EthTxHash = TxHash;
pub type EthTransaction = TransactionSignedEcRecovered;

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

/// A 20-byte Eth address
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
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
