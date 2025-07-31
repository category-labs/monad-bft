// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
