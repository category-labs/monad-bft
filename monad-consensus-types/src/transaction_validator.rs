use core::fmt::Debug;

use crate::payload::{FullTransactionList, TransactionList};

pub trait TransactionValidator: Clone + Default {
    fn validate(&self, txs: &TransactionList, full_txs: &FullTransactionList) -> bool;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl TransactionValidator for MockValidator {
    fn validate(&self, _txs: &TransactionList, _full_txs: &FullTransactionList) -> bool {
        true
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthereumValidator;

impl TransactionValidator for EthereumValidator {
    fn validate(&self, _txs: &TransactionList, _full_txs: &FullTransactionList) -> bool {
        unimplemented!()
    }
}
