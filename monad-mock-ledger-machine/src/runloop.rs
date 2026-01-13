use alloy_primitives::{Address, U256};

pub trait Runloop {
    fn run(&mut self, n_blocks: u64);
    fn set_balance(&mut self, address: Address, balance: U256);
    fn get_balance(&mut self, address: Address) -> U256;
    fn get_state_root(&mut self) -> U256;
    fn dump(&mut self);
}
