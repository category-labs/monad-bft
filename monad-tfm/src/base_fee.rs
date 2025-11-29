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

use crate::arithmetic::{CheckedI64, CheckedU64};

pub const GENESIS_BASE_FEE: u64 = 0;
// trend is a signed value in 2's complement representation
pub const GENESIS_BASE_FEE_TREND: u64 = (0i64).cast_unsigned();
pub const GENESIS_BASE_FEE_MOMENT: u64 = 0;
pub const PRE_TFM_BASE_FEE: u64 = 50_000_000_000; // 50 gwei
pub const MIN_BASE_FEE: u64 = 100_000_000_000; // 100 gwei
pub const MAX_BASE_FEE: u64 = 100_000_000_000_000_000; // 1e6 MIN_BASE_FEE

/// Implements TFM section 3 Base Fee Update Rule
///
/// # Parameters
/// - `block_gas_limit`: The maximum gas limit for the current block. Unit: MON.
///   Range [100M, 1000M]
/// - `parent_gas_usage`: Sum of all transaction gas limit in the parent block.
///   Range [0, block_gas_limit]
/// - `parent_base_fee`: The base fee of the parent block. Range `[MIN_BASE_FEE,
///   MAX_BASE_FEE]`.
/// - `parent_trend`: Encodes 2's complement representation of signed trend.
///   When block_gas_limit is constant, trend is implicitly bound by [target -
///   block_gas_limit, target]. When block_gas_limit varies, trend is implicitly
///   bound by [max_target - max_block_gas_limit, max_target]
/// - `parent_moment`: Since we set target to be above 50% block_gas_limit,
///   moment is implicitly bound by [0, trend_ceil**2]
///
/// # Returns
/// A tuple containing:
/// - `base_fee`: The computed base fee for the current block, constrained to
///   the range `[MIN_BASE_FEE, MAX_BASE_FEE]`. Unit: Mon-wei
/// - `trend`: The computed trend for the base fee
/// - `moment`: The computed moment for the base fee
///
/// # Panics
/// This function will panic if:
/// - `parent_gas_usage` is greater than `block_gas_limit`.
/// - Any of the input parameters are outside their defined ranges.
pub fn compute_base_fee(
    parent_block_gas_limit: u64, // ChainParams
    parent_gas_usage: u64,       // sum of gas limits of all transactions in the parent block
    parent_base_fee: u64,
    parent_trend: u64,
    parent_moment: u64,
) -> (u64, u64, u64) {
    assert!(
        parent_gas_usage <= parent_block_gas_limit,
        "Parent block is valid, gas usage must be <= gas limit"
    );
    let (base_fee, trend, moment) = compute_base_fee_math(
        parent_block_gas_limit,
        parent_gas_usage,
        parent_base_fee,
        parent_trend.cast_signed(),
        parent_moment,
    );

    // Convert trend to unsigned representation (2's complement bit pattern)
    let trend_u64 = trend.cast_unsigned();

    (base_fee, trend_u64, moment)
}

/// Computes an approximate `factor * e ** (numerator / denominator)` using
/// taylor expansion
///
/// based on https://eips.ethereum.org/EIPS/eip-4844#helpers and modified to
/// accept negative numerator
///
/// # Parameters
/// - `factor`: A scaling factor that must be less than or equal to
///   `MAX_BASE_FEE`.
/// - `numerator`: A signed integer representing the numerator in the
///   exponential calculation.
/// - `denominator`: An unsigned integer representing the denominator in the
///   exponential calculation.
///
/// # Panics
/// - If `denominator == 0`
/// - If `numerator.abs() > denominator`
///
/// # Input Limits
/// - `factor` must be less than or equal to `MAX_BASE_FEE`.
#[inline]
fn fake_exponential(factor: u64, numerator: i64, denominator: u64) -> u64 {
    assert_ne!(denominator, 0, "attempt to divide by zero");
    assert!(
        numerator.unsigned_abs() <= denominator,
        "|numerator| <= denominator"
    );
    assert!(factor <= MAX_BASE_FEE);

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

/// See `compute_base_fee` for parameter and return value documentation. Only
/// difference is that `parent_trend` is casted to signed value
fn compute_base_fee_math(
    block_gas_limit: u64,  // ChainParams
    parent_gas_usage: u64, // sum of gas limits of all transactions in the parent block
    parent_base_fee: u64,
    parent_trend: i64,
    parent_moment: u64,
) -> (u64, i64, u64) {
    const MAX_STEP_SIZE_DENOM: CheckedU64 = CheckedU64::from_u64(28);
    const BETA: CheckedU64 = CheckedU64::from_u64(96); // 100 * 0.96 - smoothing factor for accumulator

    let block_gas_limit = CheckedU64::from_u64(block_gas_limit);
    let parent_gas_usage = CheckedU64::from_u64(parent_gas_usage);
    let parent_base_fee = CheckedU64::from_u64(parent_base_fee);
    let parent_trend = CheckedI64::from_i64(parent_trend);
    let parent_moment = CheckedU64::from_u64(parent_moment);

    // block_gas_target = 80% of block_gas_limit
    let block_gas_target = block_gas_limit * 8 / 10;

    // parent_delta = parent_gas_usage - block_gas_target
    let parent_delta = parent_gas_usage.to_checked_i64() - block_gas_target.to_checked_i64();

    // epsilon = 1 * block_gas_target
    // eta_k = (max_step_size * epsilon) / (epsilon + sqrt(moment_k - C * trend^2))
    // base_fee{k+1} = max(MIN_BASE_FEE, parent_base_fee * exp(eta_k * (parent_gas_usage - block_gas_target) / (block_gas_limit - block_gas_target)))
    let numerator = block_gas_target.to_checked_i64() * parent_delta;
    let sqrt_inner =
        (parent_moment.to_checked_i64() - parent_trend * parent_trend).saturating_to_checked_u64();
    let sqrt_term = sqrt_inner.isqrt();

    let denominator =
        MAX_STEP_SIZE_DENOM * (block_gas_target + sqrt_term) * (block_gas_limit - block_gas_target);

    // cap base_fee to [MIN_BASE_FEE, MAX_BASE_FEE]
    let base_fee = fake_exponential(
        parent_base_fee.to_u64(),
        numerator.to_i64(),
        denominator.to_u64(),
    )
    .clamp(MIN_BASE_FEE, MAX_BASE_FEE);

    // trend is defined as -trend
    // trend_{k+1} = beta * trend_k + (1 - beta) * (block_gas_target - parent_gas_usage)
    let trend = (BETA.to_checked_i64() * parent_trend
        + (CheckedI64::from_i64(100) - BETA.to_checked_i64()) * (-parent_delta))
        / CheckedI64::from_i64(100);
    // moment_{k+1} = beta * moment_k + (1 - beta) * (block_gas_target - parent_gas_usage)^2
    let moment_decay = (BETA.to_u64() as u128)
        .checked_mul(parent_moment.to_u64() as u128)
        .expect("no overflow");

    let beta_complement: u128 = (100 - BETA.to_u64()).into();
    let parent_delta_i128: i128 = parent_delta.to_i64().into();
    let parent_delta_sqr: u128 = parent_delta_i128
        .checked_mul(parent_delta_i128)
        .expect("no overflow")
        .try_into()
        .expect("non negative");
    let current_contrib = beta_complement
        .checked_mul(parent_delta_sqr)
        .expect("no overflow");

    let moment = moment_decay
        .checked_add(current_contrib)
        .expect("no overflow")
        .checked_div(100)
        .expect("no overflow");

    (
        base_fee,
        trend.to_i64(),
        moment.try_into().expect("no overflow"),
    )
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    const BLOCK_GAS_LIMIT_FLOOR: u64 = 100_000_000; // 100M
    const BLOCK_GAS_LIMIT_CEIL: u64 = 1_000_000_000; // 1000M

    // (block_gas_limit, parent_gas_usage, parent_base_fee, parent_trend, parent_moment)
    fn base_strategy() -> impl Strategy<Value = (u64, u64, u64, i64, u64)> {
        (BLOCK_GAS_LIMIT_FLOOR..=BLOCK_GAS_LIMIT_CEIL).prop_flat_map(|block_limit| {
            // trend is bound by (target (==80% of limit) - usage)
            // upper bound when usage is 0
            // lower bound when usage is at the limit
            let trend_ceil = (block_limit as i64 * 8) / 10;
            let trend_floor = -((block_limit as i64 * 2) / 10);
            (
                Just(block_limit),                                    // block_gas_limit
                0u64..=block_limit, // parent_gas_usage: 0 to block_gas_limit
                prop_oneof![Just(0u64), MIN_BASE_FEE..=MAX_BASE_FEE], // parent_base_fee: 0 in the genesis case; then from MIN_BASE_FEE up to 1000x MIN_BASE_FEE
                trend_floor..trend_ceil,                              // parent_trend
                0u64..=(trend_ceil * trend_ceil) as u64, // parent_moment: max is trend_ceil^2
            )
        })
    }

    fn gas_usage_strategy(block_gas_limit: u64) -> impl Strategy<Value = u64> {
        prop_oneof![
            5 => Just(0u64), // empty blocks are common
            10 => 1u64..block_gas_limit,
            1 => Just(block_gas_limit), // full blocks
        ]
    }

    // simulate 1000 sequential blocks with random gas usage
    // (block_gas_limit, vec![parent_gas_usage,1000], parent_base_fee, parent_trend, parent_moment)
    fn sequential_1000_blocks_strategy() -> impl Strategy<Value = (u64, Vec<u64>, u64, i64, u64)> {
        (BLOCK_GAS_LIMIT_FLOOR..=BLOCK_GAS_LIMIT_CEIL).prop_flat_map(|block_limit| {
            // trend is bound by (target (==80% of limit) - usage)
            // upper bound when usage is 0
            // lower bound when usage is at the limit
            let trend_ceil = (block_limit as i64 * 8) / 10;
            let trend_floor = -((block_limit as i64 * 2) / 10);
            (
                Just(block_limit),                                            // block_gas_limit
                prop::collection::vec(gas_usage_strategy(block_limit), 1000), // parent_gas_usage: random values between 0 to block_gas_limit for 1000 blocks, with a bias towards empty blocks
                prop_oneof![Just(0u64), MIN_BASE_FEE..=MAX_BASE_FEE], // parent_base_fee: 0 in the genesis case; then from MIN_BASE_FEE up to 1000x MIN_BASE_FEE
                trend_floor..trend_ceil,                              // parent_trend
                0u64..=(trend_ceil * trend_ceil) as u64, // parent_moment: max is trend_ceil^2
            )
        })
    }

    // emits (block_gas_limit, parent_gas_usage) where block_gas_limit is random
    // and usage is always below the limit
    fn random_limit_usage() -> impl Strategy<Value = (u64, u64)> {
        (BLOCK_GAS_LIMIT_FLOOR..=BLOCK_GAS_LIMIT_CEIL)
            .prop_flat_map(|block_limit| (Just(block_limit), gas_usage_strategy(block_limit)))
    }

    // simulate sequential blocks, varying block_limit in each step to simulate
    // parameter updates
    // (vec![(block_gas_limit, parent_gas_usage),1000], genesis_block_usage, genesis_block_gas_limit, genesis_base_fee, genesis_trend, genesis_moment)
    fn varying_block_limit_strategy()
    -> impl Strategy<Value = (Vec<(u64, u64)>, u64, u64, u64, i64, u64)> {
        (BLOCK_GAS_LIMIT_FLOOR..=BLOCK_GAS_LIMIT_CEIL).prop_flat_map(|genesis_block_limit| {
            let trend_ceil = (genesis_block_limit as i64 * 8) / 10;
            let trend_floor = -((genesis_block_limit as i64 * 2) / 10);
            (
                prop::collection::vec(random_limit_usage(), 1000), // vec![(block_gas_limit, parent_gas_usage),1000]
                Just(genesis_block_limit),                         // genesis_block_gas_limit
                gas_usage_strategy(genesis_block_limit),           // genesis_block_usage
                prop_oneof![Just(0u64), MIN_BASE_FEE..=MAX_BASE_FEE], // genesis_base_fee: 0 in the genesis case; then from MIN_BASE_FEE up to 1000x MIN_BASE_FEE
                trend_floor..trend_ceil,                              // genesis_trend
                0u64..=(trend_ceil * trend_ceil) as u64, // genesis_moment: max is trend_ceil^2
            )
        })
    }

    // (parent_gas_limit, parent_gas_usage)
    fn genesis_strategy() -> impl Strategy<Value = (u64, u64)> {
        (BLOCK_GAS_LIMIT_FLOOR..=BLOCK_GAS_LIMIT_CEIL)
            .prop_flat_map(|block_limit| (Just(block_limit), gas_usage_strategy(block_limit)))
    }

    // (block_gas_limit, parent_base_fee, parent_trend, parent_moment)
    fn usage_strategy() -> impl Strategy<Value = (u64, u64, i64, u64)> {
        (BLOCK_GAS_LIMIT_FLOOR..=BLOCK_GAS_LIMIT_CEIL).prop_flat_map(|block_limit| {
            // trend is bound by (target (==80% of limit) - usage)
            // upper bound when usage is 0
            // lower bound when usage is at the limit
            let trend_ceil = (block_limit as i64 * 8) / 10;
            let trend_floor = -((block_limit as i64 * 2) / 10);
            (
                Just(block_limit),                       // block_gas_limit
                MIN_BASE_FEE..=(MAX_BASE_FEE), // parent_base_fee: from MIN_BASE_FEE up to MAX_BASE_FEE
                trend_floor..trend_ceil,       // parent_trend
                0u64..=(trend_ceil * trend_ceil) as u64, // parent_moment: max is trend_ceil^2
            )
        })
    }

    proptest! {
        #[test]
        fn test_compute_base_fee_math(
            values in base_strategy()
        ) {
            let (block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment) = values;
            let (base_fee, trend, moment) = compute_base_fee_math(
                block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment
            );
            assert!((MIN_BASE_FEE..=MAX_BASE_FEE).contains(&base_fee));
            // assert on implicit bounds on trend and moment
            let trend_ceil = (block_gas_limit as i64 * 8) / 10;
            let trend_floor = -((block_gas_limit as i64 * 2) / 10);
            assert!(trend >= trend_floor && trend <= trend_ceil);
            assert!(moment <= (trend_ceil * trend_ceil) as u64);
        }

        // Run a sequential test for 1000 blocks
        #[test]
        fn test_sequential_updates(
            values in sequential_1000_blocks_strategy()
        ) {
            let (block_gas_limit,
                block_gas_usages,
                parent_base_fee,
                parent_trend,
                parent_moment) = values;

            // Initialize current fees based off of the proptest's parent block
            let mut current_base_fee = parent_base_fee;
            let mut current_trend = parent_trend;
            let mut current_moment = parent_moment;

            // Iterate over the block gas usage range provided in the test
            for &block_gas_usage in &block_gas_usages {
                // Calculate the base fee for the next block
                let (next_base_fee, next_trend, next_moment) = compute_base_fee_math(
                    block_gas_limit,
                    block_gas_usage,
                    current_base_fee,
                    current_trend,
                    current_moment,
                );
                assert!((MIN_BASE_FEE..=MAX_BASE_FEE).contains(&next_base_fee));
                // assert on implicit bounds on trend and moment
                let trend_ceil = (block_gas_limit as i64 * 8) / 10;
                let trend_floor = -((block_gas_limit as i64 * 2) / 10);
                assert!(next_trend >= trend_floor && next_trend <= trend_ceil);
                assert!(next_moment <= (trend_ceil * trend_ceil) as u64);

                // Update the state for the next iteration
                current_base_fee = next_base_fee;
                current_trend = next_trend;
                current_moment = next_moment;
            }
        }

        // sequential test with varying block gas limit
        #[test]
        fn test_varying_block_limit(
            values in varying_block_limit_strategy()
        ) {
            let (
                limits_and_usages,
                genesis_block_gas_limit,
                genesis_block_usage,
                genesis_base_fee,
                genesis_trend,
                genesis_moment) = values;

            let (mut current_base_fee, mut current_trend, mut current_moment) = compute_base_fee_math(
                genesis_block_gas_limit,
                genesis_block_usage,
                genesis_base_fee,
                genesis_trend,
                genesis_moment,
            );
            assert!(current_base_fee >= MIN_BASE_FEE);


            // simulate blocks with varying block_gas_limit and usages
            for (block_gas_limit, block_gas_usage) in limits_and_usages {
                let (next_base_fee, next_trend, next_moment) = compute_base_fee_math(
                    block_gas_limit,
                    block_gas_usage,
                    current_base_fee,
                    current_trend,
                    current_moment,
                );
                assert!((MIN_BASE_FEE..=MAX_BASE_FEE).contains(&next_base_fee));
                // assert on implicit bounds on trend and moment
                let trend_ceil = (BLOCK_GAS_LIMIT_CEIL as i64 * 8) / 10;
                let trend_floor = -((BLOCK_GAS_LIMIT_CEIL as i64 * 2) / 10);
                assert!(next_trend >= trend_floor && next_trend <= trend_ceil);
                assert!(next_moment <= (trend_ceil * trend_ceil) as u64);

                current_base_fee = next_base_fee;
                current_trend = next_trend;
                current_moment = next_moment;
            }
        }

        #[test]
        fn test_constant_usage(values in usage_strategy()) {
            let (block_gas_limit,
                parent_base_fee,
                parent_trend,
                parent_moment) = values;
            let parent_gas_usage = block_gas_limit * 8 / 10; // usage == target
            let (base_fee, _, _) = compute_base_fee_math(
                block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment
            );
            assert_eq!(base_fee, parent_base_fee);
        }

        #[test]
        fn test_higher_usage(values in usage_strategy()) {
            let (block_gas_limit,
                parent_base_fee,
                parent_trend,
                parent_moment) = values;
            let parent_gas_usage = block_gas_limit * 9 / 10; // usage > target
            let (base_fee, _, _) = compute_base_fee_math(
                block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment
            );
            assert!(base_fee > parent_base_fee);
        }

        #[test]
        fn test_lower_usage(values in usage_strategy()) {
            let (block_gas_limit,
                parent_base_fee,
                parent_trend,
                parent_moment) = values;
            let parent_gas_usage = block_gas_limit * 7 / 10; // usage < target
            let (base_fee, _, _) = compute_base_fee_math(
                block_gas_limit,
                parent_gas_usage,
                parent_base_fee,
                parent_trend,
                parent_moment
            );
            assert!(base_fee < parent_base_fee);
        }

        #[test]
        fn test_tfm_activation(
            values in genesis_strategy()
        ) {
            let (block_gas_limit, parent_gas_usage) = values;
            let (base_fee, trend, moment) = compute_base_fee(
                block_gas_limit,
                parent_gas_usage,
                GENESIS_BASE_FEE,
                GENESIS_BASE_FEE_TREND,
                GENESIS_BASE_FEE_MOMENT,
            );
            assert!((MIN_BASE_FEE..=MAX_BASE_FEE).contains(&base_fee));
            // assert on implicit bounds on trend and moment
            let trend_ceil = (block_gas_limit as i64 * 8) / 10;
            let trend_floor = -((block_gas_limit as i64 * 2) / 10);
            assert!(trend.cast_signed() >= trend_floor && trend.cast_signed() <= trend_ceil);
            assert!(moment <= (trend_ceil * trend_ceil) as u64);

        }
    }

    #[test]
    fn test_fake_exponential() {
        // e**-1 ~= 0.368
        let result = fake_exponential(1, -1, 1);
        assert!(result <= 1);

        // 1000*e**(-1) ~= 367.88
        let result = fake_exponential(1000, -1, 1);
        assert!((367..=368).contains(&result));
    }

    // necessary condition for numerator_accum to always converge to zero
    #[test]
    fn test_integer_division_round_toward_zero() {
        let a = I256::try_from(-3).unwrap();
        let b = I256::try_from(2).unwrap();
        assert_eq!(a.checked_div(b).unwrap(), I256::try_from(-1).unwrap());

        let a = I256::try_from(-3).unwrap();
        let b = I256::try_from(4).unwrap();
        assert_eq!(a.checked_div(b).unwrap(), I256::try_from(0).unwrap());
    }
}
