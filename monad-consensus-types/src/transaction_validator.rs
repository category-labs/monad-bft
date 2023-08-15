use core::fmt::Debug;

use crate::payload::FullTransactionList;

pub trait TransactionValidator {
    fn validate(&self, txs: &FullTransactionList) -> bool;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl TransactionValidator for MockValidator {
    fn validate(&self, _txs: &FullTransactionList) -> bool {
        true
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthereumValidator;

impl TransactionValidator for EthereumValidator {
    fn validate(&self, _txs: &FullTransactionList) -> bool {
        unimplemented!()
    }
}
