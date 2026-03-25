use crate::{
    core::clause::Clause,
    traces::types::{Address20, Selector4},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceFilter {
    pub from: Option<Clause<Address20>>,
    pub to: Option<Clause<Address20>>,
    pub selector: Option<Clause<Selector4>>,
    pub is_top_level: Option<bool>,
    pub has_value: Option<bool>,
}
