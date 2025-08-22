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

use alloy_primitives::{I256, U256};

pub const GENESIS_BASE_FEE: u64 = 0;
pub const GENESIS_BASE_FEE_TREND: u64 = 0;
pub const GENESIS_BASE_FEE_MOMENT: u64 = 0;
pub const MIN_BASE_FEE: u64 = 100_000_000_000; // 100 gwei

// impl TFM section 3
// return value: (base_fee, base_fee_trend, base_fee_moment)
// trend is an i64 in 2's complement representation
// base_fee unit: MON-wei
pub fn compute_base_fee(
    block_gas_limit: u64,  // ChainParams
    parent_gas_usage: u64, // sum of gas limits of all transactions in the parent block
    parent_base_fee: u64,
    parent_trend: u64,
    parent_moment: u64,
) -> (u64, u64, u64) {
    let (base_fee, trend, moment) = compute_base_fee_math(
        block_gas_limit,
        parent_gas_usage,
        parent_base_fee,
        parent_trend.cast_signed(),
        parent_moment,
    );

    // Convert trend to unsigned representation (2's complement bit pattern)
    let trend_u64 = trend.cast_unsigned();

    (base_fee, trend_u64, moment)
}

/// https://eips.ethereum.org/EIPS/eip-4844#helpers)
#[inline]
fn fake_exponential(factor: u64, numerator: i64, denominator: u64) -> u64 {
    assert_ne!(denominator, 0, "division by zero");

    let factor = U256::from(factor);
    let numerator = I256::try_from(numerator).expect("i64 always fit in i256");
    let denominator = U256::from(denominator);

    let mut i = U256::from(1);
    let mut output = I256::ZERO;
    let mut numerator_accum =
        I256::try_from(factor * denominator).expect("product of two 64 bit numbers fits in i256");
    while !numerator_accum.is_zero() {
        output += numerator_accum;

        // denominator is not zero, asserted at the beginning of the function
        // denominator * i is never (-1) so this division never overflows
        numerator_accum = (numerator_accum * numerator)
            .checked_div(
                I256::try_from(denominator * i).expect("u64 times small i should never overflow"),
            )
            .expect("denominator is always positive, div will not overflow");
        i += U256::from(1);
    }
    output
        .checked_div(I256::try_from(denominator).expect("u64 fits in i256"))
        .expect("denominator is always positive, div will not overflow")
        .as_u64()
}

fn compute_base_fee_math(
    block_gas_limit: u64,  // ChainParams
    parent_gas_usage: u64, // sum of gas limits of all transactions in the parent block
    parent_base_fee: u64,
    parent_trend: i64,
    parent_moment: u64,
) -> (u64, i64, u64) {
    const MAX_STEP_SIZE_DENOM: u64 = 28;
    const BETA: u64 = 96; // 100*BETA - smoothing factor for accumulator
    const C: u64 = 1;

    // block_gas_target = 80% of block_gas_limit
    let block_gas_target = block_gas_limit * 8 / 10;

    // parent_delta = parent_gas_usage - block_gas_target
    let parent_delta = parent_gas_usage as i64 - block_gas_target as i64;

    // epsilon = 1 * block_gas_target
    // eta_k = (max_step_size * epsilon) / (epsilon + sqrt(moment_k - C * trend^2))
    // base_fee{k+1} = max(MIN_BASE_FEE, parent_base_fee * exp(eta_k * (parent_gas_usage - block_gas_target) / (block_gas_limit - block_gas_target)))
    let numerator = block_gas_target as i64 * parent_delta;
    let sqrt_inner = (parent_moment as i64 - C as i64 * parent_trend * parent_trend).max(0) as u64;
    let sqrt_term: u64 = U256::from(sqrt_inner).root(2).to();

    let denominator =
        MAX_STEP_SIZE_DENOM * (block_gas_target + sqrt_term) * (block_gas_limit - block_gas_target);

    let base_fee = fake_exponential(parent_base_fee, numerator, denominator);

    // trend is defined as -trend
    // trend_{k+1} = beta * trend_k + (1 - beta) * (block_gas_target - parent_gas_usage)
    let trend = (BETA as i64 * parent_trend
        + (100 - BETA) as i64 * (block_gas_target as i64 - parent_gas_usage as i64))
        / 100;
    // moment_{k+1} = beta * moment_k + (1 - beta) * (block_gas_target - parent_gas_usage)^2
    let moment = (BETA * parent_moment + (100 - BETA) * (parent_delta * parent_delta) as u64) / 100;

    (base_fee, trend, moment)
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::{compute_base_fee_math, fake_exponential};
    use crate::base_fee::MIN_BASE_FEE;

    proptest! {
        #[test]
        fn test_compute_base_fee_math(
            block_gas_limit in 100_000_000u64..=1_000_000_000u64, // 100M to 1000M
            parent_gas_usage in 0u64..=300_000_000u64, // 100M to 300M
            parent_base_fee in prop_oneof![Just(0u64), MIN_BASE_FEE..=(MIN_BASE_FEE * 1000u64)], // 0 in the genesis case; then from MIN_BASE_FEE up to 1000x MIN_BASE_FEE
            parent_trend in -60_000_000i64..=240_000_000i64, // trend is bounded by (target (==80% of limit) - usage). Upper bound is 240M (empty block with 300M limit). Lower bound is -60M (full block with 300M limit)
            parent_moment in 0u64..=57600000000000000u64 // 240M^2
        ) {
            let _ = compute_base_fee_math(
                block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment
            );
            // overflow didn't happen
        }
    }

    #[test]
    fn test_fake_exponential() {
        assert_eq!(fake_exponential(1, -1, 1), 0);
        assert_eq!(fake_exponential(1000, -1, 1), 368);
    }
}
