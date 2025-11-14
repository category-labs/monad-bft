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

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use rand::Rng;

use crate::{
    prelude::*,
    shared::{eip7702::mutate_eip7702_transaction, private_key::PrivateKey},
};

/// Batched transformations configured for a generator workload.
#[derive(Debug, Clone)]
pub struct TransformOptions {
    pub mutation_percentage: f64,
    pub drop_percentage: f64,
    pub convert_eip1559_to_legacy: bool,
}

impl TransformOptions {
    pub fn new(
        mutation_percentage: f64,
        drop_percentage: u64,
        convert_eip1559_to_legacy: bool,
    ) -> Self {
        Self {
            mutation_percentage: mutation_percentage.clamp(0.0, 100.0),
            drop_percentage: (drop_percentage.min(100)) as f64,
            convert_eip1559_to_legacy,
        }
    }

    pub fn is_mutation_enabled(&self) -> bool {
        self.mutation_percentage > 0.0
    }

    pub fn is_drop_enabled(&self) -> bool {
        self.drop_percentage > 0.0
    }
}

impl Default for TransformOptions {
    fn default() -> Self {
        Self::new(0.0, 0, false)
    }
}

pub fn transform_batch(
    txs: Vec<(TxEnvelope, Address, PrivateKey)>,
    opts: &TransformOptions,
) -> Vec<(TxEnvelope, Address, PrivateKey)> {
    let txs = if opts.is_mutation_enabled() {
        mutate_transactions(txs, opts.mutation_percentage)
    } else {
        txs
    };

    let txs = if opts.convert_eip1559_to_legacy {
        convert_batch_to_legacy(txs)
    } else {
        txs
    };

    if opts.is_drop_enabled() {
        drop_transactions(txs, opts.drop_percentage)
    } else {
        txs
    }
}

fn mutate_transactions(
    txs: Vec<(TxEnvelope, Address, PrivateKey)>,
    mutation_percentage: f64,
) -> Vec<(TxEnvelope, Address, PrivateKey)> {
    if txs.is_empty() || mutation_percentage <= 0.0 {
        return txs;
    }

    let mut rng = rand::thread_rng();
    let mut mutated = Vec::with_capacity(txs.len());
    let mut mutation_count = 0usize;

    for tx_triple in txs {
        let random_value = rng.gen_range(0.0..100.0);

        if random_value < mutation_percentage {
            mutated.push(mutate_transaction(&tx_triple));
            mutation_count += 1;
        } else {
            mutated.push(tx_triple);
        }
    }

    debug!(
        total_txs = mutated.len(),
        mutated_txs = mutation_count,
        mutation_percentage,
        "Mutated transactions in batch"
    );

    mutated
}

fn drop_transactions(
    txs: Vec<(TxEnvelope, Address, PrivateKey)>,
    drop_percentage: f64,
) -> Vec<(TxEnvelope, Address, PrivateKey)> {
    if txs.is_empty() || drop_percentage <= 0.0 {
        return txs;
    }

    let mut rng = rand::thread_rng();
    let mut kept = Vec::with_capacity(txs.len());
    let mut dropped = 0usize;

    for tx_triple in txs {
        let random_value = rng.gen_range(0.0..100.0);
        if random_value < drop_percentage {
            dropped += 1;
            continue;
        }
        kept.push(tx_triple);
    }

    debug!(
        total_txs = dropped + kept.len(),
        dropped_txs = dropped,
        drop_percentage,
        "Dropped transactions from batch"
    );

    kept
}

fn convert_batch_to_legacy(
    txs: Vec<(TxEnvelope, Address, PrivateKey)>,
) -> Vec<(TxEnvelope, Address, PrivateKey)> {
    let mut converted = Vec::with_capacity(txs.len());
    let mut conversions = 0usize;

    for (tx, addr, key) in txs {
        if let Some((legacy_tx, signer)) = convert_eip1559_to_legacy(&tx, &key) {
            conversions += 1;
            converted.push((legacy_tx, addr, signer));
        } else {
            converted.push((tx, addr, key));
        }
    }

    if conversions > 0 {
        debug!(
            total_txs = converted.len(),
            converted_txs = conversions,
            "Converted EIP-1559 transactions to legacy"
        );
    }

    converted
}

fn mutate_transaction(
    tx_triple: &(TxEnvelope, Address, PrivateKey),
) -> (TxEnvelope, Address, PrivateKey) {
    let (tx, addr, original_key) = tx_triple;
    let mutated_tx = match tx {
        TxEnvelope::Eip7702(_) => mutate_eip7702_transaction(tx, original_key),
        TxEnvelope::Eip1559(_) => mutate_eip1559_transaction(tx, original_key),
        _ => tx.clone(),
    };

    (mutated_tx, *addr, original_key.clone())
}

fn convert_eip1559_to_legacy(
    tx: &TxEnvelope,
    original_key: &PrivateKey,
) -> Option<(TxEnvelope, PrivateKey)> {
    let TxEnvelope::Eip1559(signed_tx) = tx else {
        return None;
    };

    let original = signed_tx.tx();

    let legacy = TxLegacy {
        chain_id: Some(original.chain_id),
        nonce: original.nonce,
        gas_price: original.max_fee_per_gas,
        gas_limit: original.gas_limit,
        to: original.to,
        value: original.value,
        input: original.input.clone(),
    };

    let sig = original_key.sign_transaction(&legacy);
    Some((
        TxEnvelope::Legacy(legacy.into_signed(sig)),
        original_key.clone(),
    ))
}

fn mutate_eip1559_transaction(tx: &TxEnvelope, original_key: &PrivateKey) -> TxEnvelope {
    let mut rng = rand::thread_rng();

    let TxEnvelope::Eip1559(signed_tx) = tx else {
        error!("mutate_eip1559_transaction called with non-EIP1559 transaction");
        return tx.clone();
    };

    let original_tx = &signed_tx.tx();

    let mut new_tx = TxEip1559 {
        chain_id: original_tx.chain_id,
        nonce: original_tx.nonce,
        gas_limit: original_tx.gas_limit,
        max_fee_per_gas: original_tx.max_fee_per_gas,
        max_priority_fee_per_gas: original_tx.max_priority_fee_per_gas,
        to: original_tx.to,
        value: original_tx.value,
        access_list: original_tx.access_list.clone(),
        input: original_tx.input.clone(),
    };

    // 8 fields total: 7 transaction fields + 1 signature field
    const FIELD_MUTATION_PROB: f64 = 1.0 / 8.0;

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.nonce = rng.gen_range(0..=u64::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.gas_limit = rng.gen_range(0..=u64::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.max_fee_per_gas = rng.gen_range(0..=u128::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.max_priority_fee_per_gas = rng.gen_range(0..=u128::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.to = TxKind::Call(Address::from(rng.gen::<[u8; 20]>()));
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.value = U256::from(rng.gen::<u128>());
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        let input_len = rng.gen_range(0..=1000);
        new_tx.input = Bytes::from((0..input_len).map(|_| rng.gen::<u8>()).collect::<Vec<_>>());
    }

    // Mutate signature (sign with wrong key) if selected
    if rng.gen_bool(FIELD_MUTATION_PROB) {
        // Mutate signature by signing with a random key (invalid signature)
        let (_random_addr, random_key) = PrivateKey::new_with_random(&mut rng);
        let sig = random_key.sign_transaction(&new_tx);
        TxEnvelope::Eip1559(new_tx.into_signed(sig))
    } else {
        // Sign with original key (valid signature, but mutated fields)
        let sig = original_key.sign_transaction(&new_tx);
        TxEnvelope::Eip1559(new_tx.into_signed(sig))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_eip1559_tx() -> (TxEnvelope, Address, PrivateKey) {
        let mut rng = rand::thread_rng();
        let (addr, key) = PrivateKey::new_with_random(&mut rng);
        let tx = TxEip1559 {
            chain_id: 1,
            nonce: 0,
            gas_limit: 21_000,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(addr),
            value: U256::from(1),
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let sig = key.sign_transaction(&tx);
        (TxEnvelope::Eip1559(tx.into_signed(sig)), addr, key)
    }

    #[test]
    fn transform_batch_noop_when_all_disabled() {
        let tx = sample_eip1559_tx();
        let original = vec![tx.clone()];
        let opts = TransformOptions::new(0.0, 0, false);
        let result = transform_batch(original.clone(), &opts);
        assert_eq!(result.len(), original.len());
        for (orig, new) in original.iter().zip(result.iter()) {
            assert_eq!(orig.0, new.0);
            assert_eq!(orig.1, new.1);
        }
    }

    #[test]
    fn transform_batch_drops_all_when_percentage_is_100() {
        let tx = sample_eip1559_tx();
        let opts = TransformOptions::new(0.0, 100, false);
        let result = transform_batch(vec![tx], &opts);
        assert!(result.is_empty());
    }

    #[test]
    fn transform_batch_converts_eip1559_to_legacy() {
        let tx = sample_eip1559_tx();
        let opts = TransformOptions::new(0.0, 0, true);
        let result = transform_batch(vec![tx], &opts);
        assert_eq!(result.len(), 1);
        match &result[0].0 {
            TxEnvelope::Legacy(_) => {}
            other => panic!("expected legacy tx, got {other:?}"),
        }
    }
}
