use std::error::Error;

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::TxHash;
use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub type EthTxHash = TxHash;
// FIXME reth types shouldn't be leaked
pub type EthTransaction = Recovered<TxEnvelope>;

pub type EthSignedTransaction = TxEnvelope;

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

    pub fn rlp_decode(rlp_data: Bytes) -> Result<Self, Box<dyn Error>> {
        let txs = Vec::<EthSignedTransaction>::decode(&mut rlp_data.as_ref())?;
        let recovered_txs = txs
            .into_par_iter()
            .map(|tx| {
                let signer = tx.recover_signer()?;
                Ok(Recovered::new_unchecked(tx, signer))
            })
            .collect::<Result<_, alloy_primitives::SignatureError>>()?;
        Ok(Self(recovered_txs))
    }

    /// Get a list of tx hashes of all the transactions in this list
    pub fn get_hashes(self) -> Vec<EthTxHash> {
        self.0.iter().map(|x| x.signature_hash()).collect()
    }
}
