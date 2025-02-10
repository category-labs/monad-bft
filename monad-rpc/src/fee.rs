/// This trait represents algorithms for calculating the base fee of a block so that users can set
/// their maxFeePerGas appropriately and rpc can check submitted transactions
pub trait BaseFeePerGas: std::fmt::Debug {
    /// Calculates the base fee at a specific block number
    /// returns None if the calculation is not possible, ie the fee calculation for N depends on
    /// data that is not yet available
    fn base_fee_at(&self, block_num: u64) -> Option<u128>;

    /// Returns the minimum base fee that could be calculated for the end_block if there is enough
    /// data from the range of block numbers. The minimum base fee is the lower bound on what the
    /// value could be. If that lower bound is unknown for this algorithm it should return
    /// Some(0)
    fn min_potential_base_fee_in_range(&self, start_block: u64, end_block: u64) -> Option<u128>;
}

/// The base fee never changes
#[derive(Debug, Clone)]
pub struct FixedFee {
    fixed_base_fee: u128,
}

impl FixedFee {
    pub fn new(base_fee: u128) -> Self {
        Self {
            fixed_base_fee: base_fee,
        }
    }
}

impl BaseFeePerGas for FixedFee {
    fn base_fee_at(&self, _block_num: u64) -> Option<u128> {
        Some(self.fixed_base_fee)
    }

    fn min_potential_base_fee_in_range(&self, _start_block: u64, _end_block: u64) -> Option<u128> {
        Some(self.fixed_base_fee)
    }
}
