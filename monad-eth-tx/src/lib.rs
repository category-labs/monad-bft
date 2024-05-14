use std::collections::BTreeMap;

use alloy_primitives::TxHash;
use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use reth_primitives::{Address, TransactionSignedEcRecovered};

// FIXME reth types shouldn't be leaked
pub type EthTxHash = TxHash;
// FIXME reth types shouldn't be leaked
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

    /// Get a map of the latest account nonces in this transactions list
    pub fn get_account_nonces(self) -> BTreeMap<Address, u64> {
        BTreeMap::from_iter(self.0.iter().map(|tx| (tx.signer(), tx.nonce())))
    }
}
