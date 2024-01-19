use core::fmt::Debug;

use crate::payload::FullTransactionList;

pub trait BlockValidator {
    fn validate(&self, full_txs: &FullTransactionList) -> bool;
}

impl<T: BlockValidator + ?Sized> BlockValidator for Box<T> {
    fn validate(&self, full_txs: &FullTransactionList) -> bool {
        (**self).validate(full_txs)
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl BlockValidator for MockValidator {
    fn validate(&self, _full_txs: &FullTransactionList) -> bool {
        true
    }
}
