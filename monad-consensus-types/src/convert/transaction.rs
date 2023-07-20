use monad_proto::{
    error::ProtoError,
    proto::transaction::{
        proto_transaction_collection::Txns, ProtoEthereumTransactions, ProtoMockTransactions,
        ProtoTransactionCollection,
    },
};

use crate::transaction::{EthereumTransactions, MockTransactions};

impl From<MockTransactions> for ProtoMockTransactions {
    fn from(_: MockTransactions) -> Self {
        ProtoMockTransactions {}
    }
}

impl From<MockTransactions> for ProtoTransactionCollection {
    fn from(value: MockTransactions) -> Self {
        Self {
            txns: Some(Txns::MockTxns(value.into())),
        }
    }
}

impl From<ProtoMockTransactions> for MockTransactions {
    fn from(_: ProtoMockTransactions) -> Self {
        Self {}
    }
}

impl TryFrom<ProtoTransactionCollection> for MockTransactions {
    type Error = ProtoError;

    fn try_from(value: ProtoTransactionCollection) -> Result<Self, Self::Error> {
        let txns = value.txns.ok_or(Self::Error::MissingRequiredField(
            "ProtoTransactionCollection.txns".to_owned(),
        ))?;

        match txns {
            Txns::MockTxns(txns) => Ok(txns.into()),
            _ => Err(Self::Error::MissingRequiredField(
                "ProtoTransactionCollection.txns.mock_txns".to_owned(),
            )),
        }
    }
}

impl From<EthereumTransactions> for ProtoEthereumTransactions {
    fn from(_value: EthereumTransactions) -> Self {
        Self {}
    }
}

impl From<EthereumTransactions> for ProtoTransactionCollection {
    fn from(value: EthereumTransactions) -> Self {
        Self {
            txns: Some(Txns::EthTxns(value.into())),
        }
    }
}

impl From<ProtoEthereumTransactions> for EthereumTransactions {
    fn from(_: ProtoEthereumTransactions) -> Self {
        Self {}
    }
}

impl TryFrom<ProtoTransactionCollection> for EthereumTransactions {
    type Error = ProtoError;

    fn try_from(value: ProtoTransactionCollection) -> Result<Self, Self::Error> {
        let txns = value.txns.ok_or(Self::Error::MissingRequiredField(
            "ProtoTransactionCollection.txns".to_owned(),
        ))?;

        match txns {
            Txns::EthTxns(txns) => Ok(txns.into()),
            _ => Err(Self::Error::MissingRequiredField(
                "ProtoTransactionCollection.txns.eth_txns".to_owned(),
            )),
        }
    }
}
