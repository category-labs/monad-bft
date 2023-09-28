use reth_primitives::{TransactionSignedEcRecovered, TxHash};
use reth_rlp::{Decodable, Encodable};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EthTransactionList(pub Vec<TxHash>);

impl EthTransactionList {
    pub fn rlp_encode(self) -> Vec<u8> {
        let mut buf = Vec::default();

        self.0.encode(&mut buf);

        buf
    }

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
