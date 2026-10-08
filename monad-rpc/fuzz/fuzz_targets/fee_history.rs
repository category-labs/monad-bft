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

//! Fuzzes eth_feeHistory end to end on a mock triedb. Each input builds a
//! short chain whose blocks hold signed transactions of every fee type with
//! receipts that agree with them, as a real chain's do, then sends a fuzzed
//! request (any block count, any tag or number, any percentile list) in the
//! positional form clients use. The handler must not panic, and an answer
//! must describe the chain it was given: one gas-used ratio per block of
//! the range and one more base fee, every array sized to that range, each
//! block's base fee and ratio, zero blob fees, and per block the
//! gas-weighted reward of each percentile. A zero block count gets an
//! empty history.

#![no_main]

use std::sync::{Arc, LazyLock};

use alloy_consensus::{
    Block, Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction, TxEip1559,
    TxEip2930, TxEnvelope, TxLegacy,
};
use alloy_primitives::{Address, Bloom, Bytes, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use monad_eth_types::ReceiptWithLogIndex;
use monad_rpc::{
    data::DataProvider,
    handlers::eth::gas::{monad_eth_feeHistory, MonadEthHistoryParams},
};
use monad_triedb_utils::mock_triedb::MockTriedb;
use monad_types::SeqNum;
use serde_json::json;

const MAX_BLOCKS: usize = 24;
const MAX_TXS: usize = 6;

static SIGNER: LazyLock<PrivateKeySigner> =
    LazyLock::new(|| PrivateKeySigner::from_bytes(&[7u8; 32].into()).unwrap());

#[derive(Arbitrary, Debug)]
struct TxSpec {
    kind: u8,
    max_fee: u128,
    priority_fee: u128,
    gas_limit: u64,
    gas_used: u32,
}

#[derive(Arbitrary, Debug)]
struct BlockSpec {
    base_fee: Option<u64>,
    gas_limit: u64,
    txs: Vec<TxSpec>,
}

#[derive(Arbitrary, Debug)]
enum Newest {
    Latest,
    Safe,
    Finalized,
    BehindLatest(u8),
    Number(u64),
}

#[derive(Arbitrary, Debug)]
enum Percentiles {
    /// Left out of the request: `[blockCount, newestBlock]`.
    Omitted,
    /// An explicit `null`.
    Null,
    /// Valid: bytes mapped onto [0, 100] and sorted.
    Valid(Vec<u8>),
    /// Anything, to exercise validation.
    Raw(Vec<f64>),
}

#[derive(Arbitrary, Debug)]
struct Input {
    blocks: Vec<BlockSpec>,
    block_count: u64,
    newest: Newest,
    percentiles: Percentiles,
}

fn sign(signer: &PrivateKeySigner, spec: &TxSpec, nonce: u64) -> TxEnvelope {
    let to = TxKind::Call(Address::ZERO);
    match spec.kind % 3 {
        0 => {
            let tx = TxLegacy {
                chain_id: Some(1),
                nonce,
                gas_price: spec.max_fee,
                gas_limit: spec.gas_limit,
                to,
                value: U256::ZERO,
                input: Bytes::new(),
            };
            let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            TxEnvelope::from(tx.into_signed(sig))
        }
        1 => {
            let tx = TxEip2930 {
                chain_id: 1,
                nonce,
                gas_price: spec.max_fee,
                gas_limit: spec.gas_limit,
                to,
                value: U256::ZERO,
                access_list: Default::default(),
                input: Bytes::new(),
            };
            let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            TxEnvelope::from(tx.into_signed(sig))
        }
        _ => {
            let tx = TxEip1559 {
                chain_id: 1,
                nonce,
                gas_limit: spec.gas_limit,
                max_fee_per_gas: spec.max_fee,
                max_priority_fee_per_gas: spec.priority_fee,
                to,
                value: U256::ZERO,
                access_list: Default::default(),
                input: Bytes::new(),
            };
            let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            TxEnvelope::from(tx.into_signed(sig))
        }
    }
}

/// The tip a transaction pays per gas at `base_fee`, as eth_feeHistory
/// ranks it (0 when it cannot pay the base fee).
fn tip(spec: &TxSpec, base_fee: u64) -> u128 {
    let base = base_fee as u128;
    match spec.kind % 3 {
        0 | 1 => spec.max_fee.saturating_sub(base),
        _ => {
            if spec.max_fee < base {
                0
            } else {
                spec.priority_fee.min(spec.max_fee - base)
            }
        }
    }
}

/// One block's rewards as the handler computes them: transactions sorted by
/// tip, and per percentile the tip of the transaction just past that share
/// of the block's gas (geth reports the one that reaches it).
fn expected_rewards(spec: &BlockSpec, percentiles: &[f64]) -> Vec<u128> {
    let base_fee = spec.base_fee.unwrap_or_default();
    let mut txs: Vec<(u64, u128)> = spec
        .txs
        .iter()
        .take(MAX_TXS)
        .map(|t| (t.gas_used as u64, tip(t, base_fee)))
        .collect();
    if txs.is_empty() {
        return vec![0; percentiles.len()];
    }
    txs.sort_by_key(|&(_, tip)| tip);
    let used: u64 = txs.iter().map(|&(gas, _)| gas).sum();
    let mut idx = 0;
    let mut cumulative = 0u64;
    percentiles
        .iter()
        .map(|p| {
            let threshold = (used as f64 * p / 100.0).round() as u64;
            while cumulative < threshold && idx < txs.len() {
                cumulative += txs[idx].0;
                idx += 1;
            }
            txs[idx.min(txs.len() - 1)].1
        })
        .collect()
}

fn receipt(kind: u8, cumulative_gas_used: u64) -> ReceiptWithLogIndex {
    let inner = ReceiptWithBloom::new(
        Receipt {
            logs: vec![],
            status: Eip658Value::Eip658(true),
            cumulative_gas_used,
        },
        Bloom::default(),
    );
    ReceiptWithLogIndex {
        receipt: match kind % 3 {
            0 => ReceiptEnvelope::Legacy(inner),
            1 => ReceiptEnvelope::Eip2930(inner),
            _ => ReceiptEnvelope::Eip1559(inner),
        },
        starting_log_index: 0,
    }
}

fuzz_target!(|input: Input| {
    let blocks: Vec<&BlockSpec> = input.blocks.iter().take(MAX_BLOCKS).collect();
    if blocks.is_empty() {
        return;
    }
    let latest = (blocks.len() - 1) as u64;

    let newest_tag = match input.newest {
        Newest::Latest => json!("latest"),
        Newest::Safe => json!("safe"),
        Newest::Finalized => json!("finalized"),
        Newest::BehindLatest(n) => json!(format!("0x{:x}", latest.saturating_sub(n as u64))),
        Newest::Number(n) => json!(format!("0x{n:x}")),
    };
    let percentiles: Option<Vec<f64>> = match &input.percentiles {
        Percentiles::Omitted | Percentiles::Null => None,
        Percentiles::Valid(bytes) => {
            let mut p: Vec<f64> = bytes
                .iter()
                .take(100)
                .map(|&b| b as f64 * 100.0 / 255.0)
                .collect();
            p.sort_by(f64::total_cmp);
            Some(p)
        }
        // NaN and infinities are not JSON; clients cannot send them.
        Percentiles::Raw(raw) => Some(
            raw.iter()
                .take(120)
                .map(|x| if x.is_finite() { *x } else { 0.0 })
                .collect(),
        ),
    };
    let block_count = json!(format!("0x{:x}", input.block_count));
    let request = match input.percentiles {
        Percentiles::Omitted => json!([block_count, newest_tag]),
        _ => json!([block_count, newest_tag, percentiles]),
    };

    // Requests the handler must answer: a block count it accepts, a newest
    // block that exists, and percentiles that are few, in range and sorted.
    let newest = match input.newest {
        Newest::Number(n) => n,
        Newest::BehindLatest(n) => latest.saturating_sub(n as u64),
        _ => latest,
    };
    let valid = (1..=1024).contains(&input.block_count)
        && newest <= latest
        && percentiles.as_ref().is_none_or(|p| {
            p.len() <= 100
                && p.iter().all(|x| (0.0..=100.0).contains(x))
                && p.windows(2).all(|w| w[0] <= w[1])
        });

    let params = serde_json::from_value::<MonadEthHistoryParams>(request);
    assert!(
        params.is_ok() || !valid,
        "valid request rejected: {params:?}"
    );
    let Ok(params) = params else {
        return;
    };

    // Only requests that reach the blocks need signed transactions.
    let mut triedb = MockTriedb::default();
    triedb.set_latest_block(latest);
    let mut nonce = 0u64;
    for (number, spec) in blocks.iter().enumerate() {
        let mut block = Block::<TxEnvelope>::default();
        let mut receipts = Vec::new();
        let mut cumulative = 0u64;
        for tx in spec.txs.iter().take(if valid { MAX_TXS } else { 0 }) {
            block.body.transactions.push(sign(&SIGNER, tx, nonce));
            nonce += 1;
            cumulative += tx.gas_used as u64;
            receipts.push(receipt(tx.kind, cumulative));
        }
        block.header.number = number as u64;
        block.header.base_fee_per_gas = spec.base_fee;
        block.header.gas_limit = spec.gas_limit;
        block.header.gas_used = cumulative;
        triedb.set_finalized_block(SeqNum(number as u64), block);
        triedb.set_receipts(SeqNum(number as u64), receipts);
    }

    let provider = DataProvider::new(None, Arc::new(triedb), None);
    let answer = futures::executor::block_on(monad_eth_feeHistory(&provider, params));
    if input.block_count == 0 {
        let Ok(answer) = answer else {
            panic!("zero block count failed: {:?}", answer.err());
        };
        assert_eq!(answer.0, Default::default(), "zero block count");
        return;
    }
    if !valid {
        return;
    }
    let Ok(answer) = answer else {
        panic!("valid request failed: {:?}", answer.err());
    };
    let history = answer.0;
    let oldest = newest.saturating_sub(input.block_count - 1);
    let n = (newest - oldest + 1) as usize;
    assert_eq!(history.oldest_block, oldest, "oldest block");
    assert_eq!(
        history.gas_used_ratio.len(),
        n,
        "one gas-used ratio per block"
    );
    assert_eq!(
        history.base_fee_per_gas.len(),
        n + 1,
        "one base fee per block and the next"
    );
    assert_eq!(
        history.blob_gas_used_ratio,
        vec![0.0; n],
        "one zero blob gas-used ratio per block"
    );
    assert_eq!(
        history.base_fee_per_blob_gas,
        vec![0; n + 1],
        "one zero blob base fee per block and the next"
    );

    for (i, number) in (oldest..=newest).enumerate() {
        let spec = blocks[number as usize];
        assert_eq!(
            history.base_fee_per_gas[i],
            spec.base_fee.unwrap_or_default() as u128,
            "base fee of block {number}"
        );
        let used: u64 = spec
            .txs
            .iter()
            .take(MAX_TXS)
            .map(|t| t.gas_used as u64)
            .sum();
        let ratio = used as f64 / spec.gas_limit as f64;
        let got = history.gas_used_ratio[i];
        assert!(
            got.to_bits() == ratio.to_bits() || (got.is_nan() && ratio.is_nan()),
            "gas-used ratio of block {number}: {got} vs {ratio}"
        );
    }

    // The next block's base fee: the block after `newest` when one exists
    // and was named by number, otherwise the newest block's own.
    let base_fee_of = |number: u64| blocks[number as usize].base_fee.unwrap_or_default() as u128;
    let next = match input.newest {
        Newest::Number(_) | Newest::BehindLatest(_) if newest < latest => base_fee_of(newest + 1),
        _ => base_fee_of(newest),
    };
    assert_eq!(history.base_fee_per_gas[n], next, "next block's base fee");

    let wants_rewards = percentiles.as_ref().is_some_and(|p| !p.is_empty());
    let rewards = history.reward.expect("reward is always present");
    if !wants_rewards {
        assert!(rewards.is_empty(), "rewards without percentiles");
        return;
    }
    let percentiles = percentiles.unwrap();
    assert_eq!(rewards.len(), n, "one reward row per block");
    for (i, number) in (oldest..=newest).enumerate() {
        assert_eq!(
            rewards[i],
            expected_rewards(blocks[number as usize], &percentiles),
            "rewards of block {number}"
        );
    }
});
