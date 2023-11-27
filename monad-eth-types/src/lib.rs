use reth_primitives::{Address, TransactionSignedEcRecovered, TxHash, H160};
use reth_rlp::{Decodable, Encodable};

#[cfg(feature = "serde")]
pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EthTransactionList(pub Vec<TxHash>);

impl EthTransactionList {
    pub fn rlp_encode(self) -> Vec<u8> {
        let mut buf = Vec::default();

        self.0.encode(&mut buf);

        buf
    }

    // FIXME-2 does this need to take ownership?
    pub fn rlp_decode(rlp_data: Vec<u8>) -> Result<Self, reth_rlp::DecodeError> {
        Vec::<TxHash>::decode(&mut rlp_data.as_slice()).map(Self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EthFullTransactionList(pub Vec<TransactionSignedEcRecovered>);

impl EthFullTransactionList {
    pub fn rlp_encode(self) -> Vec<u8> {
        let mut buf = Vec::default();

        self.0.encode(&mut buf);

        buf
    }

    pub fn rlp_decode(rlp_data: Vec<u8>) -> Result<Self, reth_rlp::DecodeError> {
        Vec::<TransactionSignedEcRecovered>::decode(&mut rlp_data.as_slice()).map(Self)
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct EthAddress(pub Address);

impl EthAddress {
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(H160(bytes))
    }
}
