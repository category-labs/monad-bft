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

pub const GENESIS_BASE_FEE: u64 = 0;
pub const GENESIS_BASE_FEE_TREND: u64 = 0;
pub const GENESIS_BASE_FEE_MOMENT: u64 = 0;
pub const MIN_BASE_FEE: u64 = 100_000_000_000; // 100 gwei

// FIXME: this is for PoC purpose only. The formula will be replaced with
// integer arithmetic

// impl TFM section 3
// return value: (base_fee, base_fee_trend, base_fee_moment)
// base_fee unit: MON-wei
pub fn compute_base_fee(
    block_gas_limit: u64,     // ChainParams
    parent_tx_gas_limit: u64, // sum of gas limits of all transactions in the parent block
    parent_base_fee: u64,
    parent_trend: u64,
    parent_moment: u64,
) -> (u64, u64, u64) {
    let (base_fee, trend, moment) = compute_base_fee_float(
        block_gas_limit,
        parent_tx_gas_limit,
        parent_base_fee,
        parent_trend,
        parent_moment,
    );

    // Convert the float value back to u64
    // FIXME: PoC imp.
    if base_fee.is_nan() || base_fee.is_infinite() || base_fee < 0.0 || base_fee > u64::MAX as f64 {
        panic!("base fee out of u64 range");
    }

    let base_fee_u64 = base_fee as u64;
    let trend_u64 = trend.to_bits();
    let moment_u64 = moment.to_bits();

    (base_fee_u64, trend_u64, moment_u64)
}

fn compute_base_fee_float(
    block_gas_limit: u64,     // ChainParams
    parent_tx_gas_limit: u64, // sum of gas limits of all transactions in the parent block
    parent_base_fee: u64,
    parent_trend: u64,
    parent_moment: u64,
) -> (f64, f64, f64) {
    const MAX_STEP_SIZE: f64 = 0.036;
    const BETA: f64 = 0.96; // smoothing factor for accumulator
    const C: u64 = 1;

    // block_gas_target = 80% of block_gas_limit
    let block_gas_target = block_gas_limit * 8 / 10;
    // epsilon = 1 * block_gas_target
    let epsilon = block_gas_target;

    // TODO: swap formula with portable arithmetic
    // parent_delta = parent_tx_gas_limit - block_gas_target
    let parent_delta = parent_tx_gas_limit as f64 - block_gas_target as f64;

    // FIXME: these conversions are meant for testing purposes only
    let parent_base_fee_f64 = f64::from_bits(parent_base_fee);
    let parent_trend_f64 = f64::from_bits(parent_trend);
    let parent_moment_f64 = f64::from_bits(parent_moment);

    // eta_k = (max_step_size * epsilon) / (epsilon + sqrt(moment_k - C * trend^2))
    let parent_eta = (MAX_STEP_SIZE * epsilon as f64)
        / (epsilon as f64
            + (parent_moment_f64 - C as f64 * parent_trend_f64 * parent_trend_f64).sqrt());

    // base_fee{k+1} = max(MIN_BASE_FEE, parent_base_fee * exp(eta_k * (parent_tx_gas_limit - block_gas_target) / (block_gas_limit - block_gas_target)))
    let base_fee = (MIN_BASE_FEE as f64).max(
        parent_base_fee_f64
            * (parent_eta * parent_delta / (block_gas_limit - block_gas_target) as f64).exp(),
    );

    // trend_{k+1} = beta * trend_k + (1 - beta) * (parent_tx_gas_limit - block_gas_target)
    let trend = BETA * parent_trend_f64 + (1.0 - BETA) * parent_delta as f64;
    // moment_{k+1} = beta * moment_k + (1 - beta) * (block_gas_limit - parent_tx_gas_limit)^2
    let moment = BETA * parent_moment_f64 + (1.0 - BETA) * parent_delta * parent_delta;

    (base_fee, trend, moment)
}
