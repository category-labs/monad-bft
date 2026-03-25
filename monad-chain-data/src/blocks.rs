use crate::{core::header::EvmBlockHeader, txs::view::TxRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub header: EvmBlockHeader,
    pub txs: Vec<TxRef>,
}

impl core::ops::Deref for Block {
    type Target = EvmBlockHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}
