use super::block::TransactionList;

pub trait Mempool {
    fn new() -> Self;
    fn get_transactions(&self, num_tx: u32) -> TransactionList;
}

#[derive(Copy, Clone, Debug, Default)]
pub struct SimulationMempool {}

impl Mempool for SimulationMempool {
    fn new() -> Self {
        SimulationMempool {}
    }
    fn get_transactions(&self, _num_tx: u32) -> TransactionList {
        Default::default()
    }
}
