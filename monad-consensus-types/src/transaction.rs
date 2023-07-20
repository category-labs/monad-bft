use core::fmt::Debug;

use monad_proto::{error::ProtoError, proto::transaction::ProtoTransactionCollection};

pub trait TransactionCollection:
    Copy
    + Clone
    + Eq
    + Default
    + Send
    + Sync
    + std::fmt::Debug
    + Into<ProtoTransactionCollection>
    + TryFrom<ProtoTransactionCollection, Error = ProtoError>
    + 'static
{
    fn validate(self) -> bool;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockTransactions;

impl TransactionCollection for MockTransactions {
    fn validate(self) -> bool {
        true
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthereumTransactions;

impl TransactionCollection for EthereumTransactions {
    fn validate(self) -> bool {
        unimplemented!()
    }
}
