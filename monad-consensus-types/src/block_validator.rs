use core::fmt::Debug;

use crate::payload::FullTransactionList;

pub trait BlockValidator: Clone + Default {
    fn validate(&self, full_txs: &FullTransactionList) -> bool;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl BlockValidator for MockValidator {
    fn validate(&self, _full_txs: &FullTransactionList) -> bool {
        true
    }
}
