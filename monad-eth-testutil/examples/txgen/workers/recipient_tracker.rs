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

use super::*;

pub struct RecipientTracker {
    pub client: ReqwestClient,
    pub rpc_sender_rx: mpsc::UnboundedReceiver<AddrsWithTime>,

    pub delay: Duration,

    pub non_zero: Arc<DashSet<Address>>,

    pub metrics: Arc<Metrics>,
}

impl RecipientTracker {
    pub async fn run(mut self) {
        let mut fetch_interval = tokio::time::interval(Duration::from_millis(10));
        fetch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        while let Some(AddrsWithTime { mut addrs, sent }) = self.rpc_sender_rx.recv().await {
            debug!(
                num_accts = addrs.len(),
                channel_len = self.rpc_sender_rx.len(),
                "Recipient tracker received accts from rpc sender"
            );

            // remove addresses we've already seen
            addrs.retain(|a| !self.non_zero.contains(a));
            if addrs.is_empty() {
                continue;
            }

            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
                debug!(
                    num_recipients = addrs.len(),
                    "Recipient tracker waited delay, refreshing batch..."
                );
            }

            // todo: should we group these up more?
            // for batch in addrs.chunks_mut(BATCH_SIZE) {
            if addrs.len() > BATCH_SIZE {
                error!(
                    addrs_len = addrs.len(),
                    BATCH_SIZE, "Addrs len should be less than batch size"
                );
            }

            self.handle_batch(addrs, self.non_zero.clone());
            const LIMIT: usize = 100_000_000;
            if self.non_zero.len() > LIMIT {
                info!("Non-zero addresses limit reached: {LIMIT}. Resetting recipient tracker");
                self.non_zero.clear();
            }

            fetch_interval.tick().await;
        }
    }

    fn handle_batch(&self, addrs: HashSet<Address>, seen_non_zero: Arc<DashSet<Address>>) {
        let client = self.client.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let now = Instant::now();
            trace!("before recipient refresh");

            match client.batch_get_balance(addrs.iter()).await {
                Ok(bals) => {
                    Self::process_bals_vec(bals, &seen_non_zero, &metrics);
                }
                Err(e) => {
                    warn!("Recipient tracker failed to refresh batch: {e}");
                }
            };

            trace!(
                elapsed_ms = now.elapsed().as_millis(),
                "after recipient refresh"
            );
        });
    }

    fn process_bals_vec(
        bals: Vec<Result<(Address, U256)>>,
        seen_non_zero: &DashSet<Address>,
        metrics: &Metrics,
    ) {
        metrics.total_rpc_calls.fetch_add(bals.len(), SeqCst);

        for (addr, b) in bals.into_iter().flatten() {
            if !b.is_zero() {
                seen_non_zero.insert(addr);
            }
        }

        metrics
            .accts_with_nonzero_bal
            .store(seen_non_zero.len(), SeqCst);
    }
}
