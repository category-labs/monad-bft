use crate::{
    core::clause::Clause,
    logs::types::{Address20, Topic32},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<Clause<Address20>>,
    pub topic0: Option<Clause<Topic32>>,
    pub topic1: Option<Clause<Topic32>>,
    pub topic2: Option<Clause<Topic32>>,
    pub topic3: Option<Clause<Topic32>>,
}
