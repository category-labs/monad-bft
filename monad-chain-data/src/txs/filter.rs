use crate::{
    core::clause::Clause,
    txs::types::{Address20, Selector4},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxFilter {
    pub from: Option<Clause<Address20>>,
    pub to: Option<Clause<Address20>>,
    pub selector: Option<Clause<Selector4>>,
}
