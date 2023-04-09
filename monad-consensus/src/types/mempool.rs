use super::block::TransactionList;

pub trait Mempool {
    fn get_transactions(num_tx: u32) -> TransactionList;
}

#[derive(Copy, Clone, Debug, Default)]
pub struct SimulationMempool {}

impl Mempool for SimulationMempool {
    fn get_transactions(num_tx: u32) -> TransactionList {
        todo!()
    }
}
