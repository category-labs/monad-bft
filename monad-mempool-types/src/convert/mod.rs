use std::time::SystemTime;

use monad_mempool_proto::{error::MempoolProtoError, tx::UnverifiedEthTxBatch};
use reth_primitives::{TransactionSigned, TransactionSignedEcRecovered};
use reth_rlp::{Decodable, Encodable};

use crate::EthTxBatch;

const TX_MAX_SIZE: usize = 128 * 1024;

impl TryFrom<UnverifiedEthTxBatch> for EthTxBatch {
    type Error = MempoolProtoError;

    fn try_from(value: UnverifiedEthTxBatch) -> Result<Self, Self::Error> {
        let txs = Vec::<TransactionSigned>::decode(&mut value.txs.as_slice())?
            .into_iter()
            .map(|signed_tx| {
                let size = signed_tx.size();

                if size >= TX_MAX_SIZE {
                    return Err(Self::Error::TooLarge(size));
                }

                // TODO-2: Intrinsic gas check
                // geth checks that gas limit is above intrinsic gas cost for tx
                // reth doesn't do this (yet?)

                signed_tx
                    .try_into_ecrecovered()
                    .map_err(Self::Error::InvalidSignature)
            })
            .collect::<Result<Vec<TransactionSignedEcRecovered>, Self::Error>>()?;

        Ok(Self {
            txs,
            time: SystemTime::now(),
        })
    }
}

impl From<EthTxBatch> for UnverifiedEthTxBatch {
    fn from(value: EthTxBatch) -> Self {
        let mut txs = Vec::default();

        Vec::<TransactionSigned>::encode(
            &value
                .txs
                .into_iter()
                .map(|tx| tx.into_signed())
                .collect::<Vec<TransactionSigned>>(),
            &mut txs,
        );

        UnverifiedEthTxBatch { txs }
    }
}
