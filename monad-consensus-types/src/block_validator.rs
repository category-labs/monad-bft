use core::fmt::Debug;

use crate::payload::FullTransactionList;

pub trait BlockValidator {
    fn validate(&self, full_txs: &FullTransactionList) -> bool;

    fn update_account_nonces(&mut self);
}

impl<T: BlockValidator + ?Sized> BlockValidator for Box<T> {
    fn validate(&self, full_txs: &FullTransactionList) -> bool {
        (**self).validate(full_txs)
    }

    fn update_account_nonces(&mut self) {
        (**self).update_account_nonces()
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl BlockValidator for MockValidator {
    fn validate(&self, _full_txs: &FullTransactionList) -> bool {
        true
    }

    fn update_account_nonces(&mut self) {}
}
