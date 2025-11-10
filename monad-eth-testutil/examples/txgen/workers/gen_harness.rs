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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

use super::*;
use crate::{
    config::GenMode,
    generators::{mutate_eip1559_transaction, native_transfer_priority_fee},
    prelude::*,
    shared::eip7702::mutate_eip7702_transaction,
};

pub trait Generator {
    // todo: come up with a way to mint too
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, PrivateKey)>;
}

pub struct GenCtx {
    pub base_fee: u128,
    pub chain_id: u64,
    pub gas_limit_contract_deployment: Option<u64>,
    pub set_tx_gas_limit: Option<u64>,
    pub priority_fee: Option<u128>,
    pub random_priority_fee_range: Option<(u128, u128)>,
}

impl GenCtx {
    pub fn base_fee(&self) -> u128 {
        let mut rng = rand::thread_rng();
        let base = self.base_fee as i128;
        let eps = rng.gen_range(-base / 100..=base / 100);
        (base + eps).max(0) as u128
    }
}

pub struct GeneratorHarness {
    pub generator: Box<dyn Generator + Send + Sync>,

    pub refresh_rx: async_channel::Receiver<Accounts>,
    pub rpc_sender: mpsc::Sender<AccountsWithTxs>,

    pub client: ReqwestClient,
    pub root_accts: VecDeque<SimpleAccount>,
    pub min_native: U256,
    pub seed_native_amt: U256,
    pub metrics: Arc<Metrics>,
    pub base_fee: Arc<Mutex<u128>>,
    pub chain_id: u64,
    pub gen_mode: GenMode,

    // New config fields
    pub gas_limit_contract_deployment: Option<u64>,
    pub set_tx_gas_limit: Option<u64>,
    pub priority_fee: Option<u128>,
    pub random_priority_fee_range: Option<(u128, u128)>,

    pub mutation_percentage: f64,

    pub shutdown: Arc<AtomicBool>,
}

impl GeneratorHarness {
    pub fn new(
        generator: Box<dyn Generator + Send + Sync>,
        refresh_rx: async_channel::Receiver<Accounts>,
        rpc_sender: mpsc::Sender<AccountsWithTxs>,
        client: &ReqwestClient,
        min_native: U256,
        seed_native_amt: U256,
        metrics: &Arc<Metrics>,
        base_fee: &Arc<Mutex<u128>>,
        chain_id: u64,
        gen_mode: GenMode,
        gas_limit_contract_deployment: Option<u64>,
        set_tx_gas_limit: Option<u64>,
        priority_fee: Option<u128>,
        random_priority_fee_range: Option<(u128, u128)>,
        mutation_percentage: f64,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            generator,
            refresh_rx,
            rpc_sender,
            client: client.clone(),
            root_accts: VecDeque::with_capacity(10),
            min_native,
            metrics: Arc::clone(metrics),
            seed_native_amt,
            base_fee: Arc::clone(base_fee),
            chain_id,
            gen_mode,
            gas_limit_contract_deployment,
            set_tx_gas_limit,
            priority_fee,
            random_priority_fee_range,
            mutation_percentage: mutation_percentage.clamp(0.0, 100.0),
            shutdown,
        }
    }

    pub async fn run(mut self) {
        info!("Starting main gen loop with gen_mode: {:?}", self.gen_mode);
        while let Ok(accts) = self.refresh_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            info!(
                gen_mode = ?self.gen_mode,
                num_accts = accts.len(),
                channel_len = self.refresh_rx.len(),
                "Gen received accounts"
            );
            if let Some(root) = accts.root {
                self.root_accts.push_back(root);
            }
            let mut accts = accts.accts;
            let seeded_idx = itertools::partition(&mut accts, |a: &SimpleAccount| {
                a.native_bal < self.min_native
            });

            let base_fee = *self.base_fee.lock().unwrap();
            let mut txs = self.generator.handle_acct_group(
                &mut accts[seeded_idx..],
                &GenCtx {
                    base_fee,
                    chain_id: self.chain_id,
                    gas_limit_contract_deployment: self.gas_limit_contract_deployment,
                    set_tx_gas_limit: self.set_tx_gas_limit,
                    priority_fee: self.priority_fee,
                    random_priority_fee_range: self.random_priority_fee_range,
                },
            );

            // handle low native bals
            let root = if seeded_idx != 0 {
                let mut root = self.root_accts.pop_front();
                if let Some(root) = root.as_mut() {
                    info!("Root {root}");
                    for acct in &accts[0..seeded_idx] {
                        let tx = native_transfer_priority_fee(
                            root,
                            acct.addr,
                            self.seed_native_amt,
                            1000,
                            &GenCtx {
                                base_fee,
                                chain_id: self.chain_id,
                                gas_limit_contract_deployment: self.gas_limit_contract_deployment,
                                set_tx_gas_limit: self.set_tx_gas_limit,
                                priority_fee: self.priority_fee,
                                random_priority_fee_range: self.random_priority_fee_range,
                            },
                        );
                        txs.push((tx, acct.addr, root.key.clone()));
                    }
                    info!("Root2 {root}");
                }
                info!(
                    seeded_idx,
                    num_accts = accts.len(),
                    root_available = root.is_some(),
                    "Found accounts that need seeding"
                );
                root
            } else {
                None
            };

            // Apply mutation if configured
            let txs = if self.mutation_percentage > 0.0 {
                self.mutate_transactions(txs)
            } else {
                txs
            };

            let accts_with_txs = AccountsWithTxs {
                accts: Accounts { accts, root },
                txs,
            };

            let num_txs: usize = accts_with_txs.txs.len();

            if let Err(e) = self.rpc_sender.send(accts_with_txs).await {
                if self.shutdown.load(Ordering::Relaxed) {
                    debug!(
                        "Failed to send accounts with txs to rpc sender during shutdown: {}",
                        e
                    );
                } else {
                    error!(
                        "Failed to send accounts with txs to rpc sender unexpectedly: {}",
                        e
                    );
                }
                break;
            }

            debug!(num_txs, "Gen pushed txs to rpc sender");
        }
        warn!("GeneratorHarness shutting down");
    }

    fn mutate_transactions(
        &self,
        txs: Vec<(TxEnvelope, Address, PrivateKey)>,
    ) -> Vec<(TxEnvelope, Address, PrivateKey)> {
        if txs.is_empty() || self.mutation_percentage <= 0.0 {
            return txs;
        }

        let mut rng = rand::thread_rng();
        let mut mutated = Vec::with_capacity(txs.len());
        let mut mutation_count = 0;

        for tx_triple in txs {
            let random_value = rng.gen_range(0.0..100.0);

            if random_value < self.mutation_percentage {
                mutated.push(self.mutate_transaction(&tx_triple));
                mutation_count += 1;
            } else {
                mutated.push(tx_triple);
            }
        }

        debug!(
            total_txs = mutated.len(),
            mutated_txs = mutation_count,
            mutation_percentage = self.mutation_percentage,
            "Mutated transactions in batch"
        );

        mutated
    }

    fn mutate_transaction(
        &self,
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
}
