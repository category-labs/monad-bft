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

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, Ordering},
};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::hex;
use tokio::time::MissedTickBehavior;

use super::*;
use crate::config::{Config, GenMode};

const MAX_BATCH_PAYLOAD_SIZE: usize = 2_000_000; // RPC server default max payload

pub struct RpcSender {
    pub gen_rx: mpsc::Receiver<AccountsWithTxs>,
    pub refresh_sender: mpsc::UnboundedSender<AccountsWithTime>,

    pub clients: Vec<ReqwestClient>,
    pub round_robin_idx: usize,
    pub target_tps: u64,
    pub metrics: Arc<Metrics>,
    pub sent_txs: Arc<DashMap<TxHash, Instant>>,
    pub gen_mode: GenMode,

    // Fields for dynamic adjustment
    pub tx_history: VecDeque<(Instant, u64)>,
    pub last_adjustment_time: Instant,
    pub adjustment_interval: Duration,
    pub use_dynamic_adjustment: bool,
    pub window_duration: Duration,

    pub shutdown: Arc<AtomicBool>,
}

impl RpcSender {
    pub fn new(
        gen_rx: mpsc::Receiver<AccountsWithTxs>,
        refresh_sender: mpsc::UnboundedSender<AccountsWithTime>,
        clients: Vec<ReqwestClient>,
        metrics: Arc<Metrics>,
        sent_txs: Arc<DashMap<TxHash, Instant>>,
        config: &Config,
        traffic_gen: &TrafficGen,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            gen_rx,
            refresh_sender,
            clients,
            round_robin_idx: 0,
            metrics,
            sent_txs,
            gen_mode: traffic_gen.gen_mode.clone(),
            // Initialize fields
            tx_history: VecDeque::new(),
            last_adjustment_time: Instant::now(),
            adjustment_interval: Duration::from_secs(1),
            window_duration: Duration::from_secs(300),

            use_dynamic_adjustment: !config.use_static_tps_interval,
            target_tps: traffic_gen.tps,
            shutdown,
        }
    }

    pub async fn run(mut self) {
        // Calculate initial interval using BATCH_SIZE as our starting estimate
        let mut interval = tokio::time::interval(Duration::from_millis(
            BATCH_SIZE as u64 * 1000 / self.target_tps,
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!(
            gen_mode = ?self.gen_mode,
            use_dynamic_adjustment = self.use_dynamic_adjustment,
            batch_size = BATCH_SIZE as u64,
            interval_ms = interval.period().as_millis(),
            "Starting rpc sender loop"
        );

        while let Some(AccountsWithTxs { accts, txs }) = self.gen_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            info!(
                gen_mode = ?self.gen_mode,
                num_accts = accts.len(),
                num_txs = txs.len(),
                channel_len = self.gen_rx.len(),
                "RpcSender received accounts with txs"
            );

            for batch in txs.chunks(BATCH_SIZE) {
                let now = Instant::now();
                let batch_size = batch.len() as u64;

                if self.use_dynamic_adjustment {
                    // Track this batch and update interval if needed
                    self.track_batch(batch_size, now);
                    interval = self.maybe_update_interval(interval, now);
                }

                // Server only accepts payloads under 2MB - split into multiple smaller batches if needed
                let size_aware_batches = self.create_size_aware_batches(batch);
                for sub_batch in size_aware_batches {
                    self.spawn_send_batch(&sub_batch);
                }

                // limit sending batch by interval
                interval.tick().await;
            }

            debug!("Sending accts to refresher...");
            if let Err(e) = self.refresh_sender.send(AccountsWithTime {
                accts,
                sent: Instant::now(),
            }) {
                if self.shutdown.load(Ordering::Relaxed) {
                    debug!(
                        "Failed to send accounts to refresher during shutdown: {}",
                        e
                    );
                } else {
                    error!("Failed to send accounts to refresher unexpectedly: {}", e);
                }
                break;
            }
            debug!("Accts sent to refresher...");
        }
        warn!("RpcSender shutting down");
    }

    // Track a batch of transactions and clean up history
    fn track_batch(&mut self, batch_size: u64, now: Instant) {
        // Record the actual batch size being sent
        self.tx_history.push_back((now, batch_size));

        // Remove history entries older than window_duration
        let window_cutoff = now - self.window_duration;
        while let Some((timestamp, _)) = self.tx_history.front() {
            if *timestamp < window_cutoff {
                self.tx_history.pop_front();
            } else {
                break;
            }
        }
    }

    // Check if we need to update the interval, and do so if necessary
    fn maybe_update_interval(
        &mut self,
        current_interval: tokio::time::Interval,
        now: Instant,
    ) -> tokio::time::Interval {
        if now - self.last_adjustment_time >= self.adjustment_interval {
            let avg_batch_size = self.calculate_average_batch_size();
            self.last_adjustment_time = now;

            // Calculate ideal new interval
            let ideal_interval = Duration::from_millis(avg_batch_size * 1000 / self.target_tps);

            // Get current interval duration
            let current_duration = current_interval.period();

            // Limit change to ±10% of current interval
            let max_increase = current_duration.mul_f64(1.1);
            let min_decrease = current_duration.mul_f64(0.9);

            let new_interval = if ideal_interval > max_increase {
                max_increase
            } else if ideal_interval < min_decrease {
                min_decrease
            } else {
                ideal_interval
            };

            info!(
                avg_batch_size,
                ideal_interval_ms = ideal_interval.as_millis(),
                new_interval_ms = new_interval.as_millis(),
                current_interval_ms = current_duration.as_millis(),
                capped = ideal_interval != new_interval,
                "Adjusted interval based on average batch size"
            );

            let mut interval = tokio::time::interval(new_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        } else {
            current_interval
        }
    }

    fn calculate_average_batch_size(&self) -> u64 {
        if self.tx_history.is_empty() {
            return BATCH_SIZE as u64; // Default if no history
        }

        let mut total_txs = 0;
        let mut count = 0;

        for (_, batch_size) in &self.tx_history {
            total_txs += batch_size;
            count += 1;
        }

        if count == 0 {
            return BATCH_SIZE as u64;
        }

        // Calculate average, rounded to nearest integer
        (total_txs as f64 / count as f64).round() as u64
    }

    fn create_size_aware_batches(
        &self,
        txs: &[(TxEnvelope, Address, crate::shared::private_key::PrivateKey)],
    ) -> Vec<Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)>> {
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_batch_size = 0;

        for (tx, addr, _key) in txs {
            let tx_size = self.calculate_tx_payload_size(tx);

            // Start new batch if this would exceed max payload
            if !current_batch.is_empty() && current_batch_size + tx_size > MAX_BATCH_PAYLOAD_SIZE {
                batches.push(current_batch);
                current_batch = Vec::new();
                current_batch_size = 0;
            }

            current_batch.push((tx.clone(), *addr, _key.clone()));
            current_batch_size += tx_size;
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        info!(
            total_txs = txs.len(),
            num_batches = batches.len(),
            "Created size-aware batches"
        );

        batches
    }

    fn calculate_tx_payload_size(&self, tx: &TxEnvelope) -> usize {
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        // Currently encoded as binary - get length as hex string prepended with "0x"
        rlp_encoded_tx.len() * 2 + 2
    }

    fn spawn_send_batch(
        &mut self,
        batch: &[(TxEnvelope, Address, crate::shared::private_key::PrivateKey)],
    ) {
        if batch.is_empty() {
            return;
        }

        // round robin through clients
        self.round_robin_idx = (self.round_robin_idx + 1) % self.clients.len();
        let client = self.clients[self.round_robin_idx].clone();

        trace!(batch_size = batch.len(), "Sending batch of txs...");

        let metrics = self.metrics.clone();
        let sent_txs = self.sent_txs.clone();
        let batch = Vec::from_iter(batch.iter().cloned()); // todo: make more performant

        tokio::spawn(async move {
            let now = Instant::now();
            for (tx, _to, _key) in &batch {
                let _ = sent_txs.insert(*tx.tx_hash(), now);
            }

            send_batch(&client, batch.iter().map(|(tx, _, _)| tx), &metrics).await;

            trace!("Tx batch sent");
        });
    }
}

pub async fn send_batch(
    client: &ReqwestClient,
    txs: impl Iterator<Item = &TxEnvelope>,
    metrics: &Metrics,
) {
    let now = Instant::now();

    let mut batch_req = client.new_batch();

    let mut futs = txs
        .filter_map(|tx| {
            let mut rlp_encoded_tx = Vec::new();
            tx.encode_2718(&mut rlp_encoded_tx);
            batch_req
                .add_call::<_, TxHash>(
                    "eth_sendRawTransaction",
                    &[format!("0x{}", hex::encode(rlp_encoded_tx))],
                )
                .ok() // todo: handle better
        })
        .collect::<FuturesUnordered<_>>();

    if let Err(e) = batch_req.send().await {
        error!("Failed to send batch: {e}");
        return;
    }

    let num_txs = futs.len();
    metrics.total_txs_sent.fetch_add(num_txs, SeqCst);

    while let Some(resp) = futs.next().await {
        if let Err(e) = resp {
            error!("Failed to send tx: {e}");
        }
    }

    trace!(elapsed_ms = now.elapsed().as_millis(), "send_batch latency");
}
