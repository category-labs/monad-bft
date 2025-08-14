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

use eyre::bail;
use tokio::time::MissedTickBehavior;

use super::*;
use crate::{config::DeployedContract, shared::erc20::ERC20};

pub struct Refresher {
    pub rpc_rx: mpsc::UnboundedReceiver<AccountsWithTime>,
    pub gen_sender: async_channel::Sender<Accounts>,

    pub client: ReqwestClient,
    pub metrics: Arc<Metrics>,
    pub erc20: Option<ERC20>,
    pub workload_group_name: String,
    pub base_fee: Arc<Mutex<u128>>,

    pub delay: Duration,
    pub shutdown: Arc<AtomicBool>,
}

impl Refresher {
    pub fn new(
        rpc_rx: mpsc::UnboundedReceiver<AccountsWithTime>,
        gen_sender: async_channel::Sender<Accounts>,

        client: ReqwestClient,
        metrics: Arc<Metrics>,
        base_fee: Arc<Mutex<u128>>,

        delay: Duration,

        deployed_contract: DeployedContract,
        refresh_erc20_balance: bool,
        workload_group_name: String,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Refresher> {
        let erc20 = if refresh_erc20_balance {
            let DeployedContract::ERC20(erc20) = deployed_contract else {
                bail!("Cannot construct Refresher: refresh_erc20_balance arg requires erc20 contract be deployed or loaded");
            };
            Some(erc20)
        } else {
            None
        };
        Ok(Refresher {
            rpc_rx,
            gen_sender,
            client,
            metrics,
            base_fee,
            delay,
            erc20,
            workload_group_name,
            shutdown,
        })
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(5));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!(
            "Starting refresher loop for workload group: {}",
            self.workload_group_name
        );
        while let Some(AccountsWithTime { accts, sent }) = self.rpc_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            info!(
                workload_group_name = self.workload_group_name,
                num_accts = accts.len(),
                channel_len = self.rpc_rx.len(),
                "Refresher received accts"
            );
            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
                debug!("Refresher waited delay, refreshing batch...");
            }

            interval.tick().await;

            self.handle_batch(accts);
        }
        warn!("Refresher shutting down");
    }

    fn handle_batch(&self, mut accts: Accounts) {
        let client = self.client.clone();
        let metrics = self.metrics.clone();
        let gen_sender = self.gen_sender.clone();
        let deployed_erc20 = self.erc20;
        let base_fee = self.base_fee.clone();
        tokio::spawn(async move {
            let mut times_sent = 0;

            while let Err(e) =
                refresh_batch(&client, &mut accts, &metrics, deployed_erc20, &base_fee).await
            {
                if times_sent > 5 {
                    error!("Exhausted retries refreshing account, oh well! {e}");
                } else {
                    times_sent += 1;
                    warn!(
                        times_sent,
                        "Encountered error refreshing accts, retrying..., {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            debug!("Completed batch refresh, sending to gen...");
            if let Err(e) = gen_sender.send(accts).await {
                debug!(
                    "Failed to send accounts to gen (likely during shutdown): {}",
                    e
                );
                return;
            }
            debug!("Refresher sent batch to gen");
        });
    }
}

pub async fn refresh_batch(
    client: &ReqwestClient,
    accts: &mut Accounts,
    metrics: &Metrics,
    deployed_erc20: Option<ERC20>,
    base_fee: &Arc<Mutex<u128>>,
) -> Result<()> {
    trace!("Refreshing batch...");

    let iter = accts.iter().map(|a| &a.addr);
    let (new_gas_price, native_bals, nonces, erc20_bals): (
        _,
        _,
        _,
        Option<Result<Vec<Result<(Address, U256)>>>>,
    ) = tokio::join!(
        client.get_base_fee(),
        client.batch_get_balance(iter.clone()),
        client.batch_get_transaction_count(iter.clone()),
        async {
            match deployed_erc20 {
                Some(erc20) => Some(client.batch_get_erc20_balance(iter.clone(), erc20).await),
                None => None,
            }
        }
    );

    match new_gas_price {
        Ok(new_gas_price) => {
            *base_fee.lock().unwrap() = new_gas_price;
        }
        Err(e) => {
            let base_fee = *base_fee.lock().unwrap();
            error!(
                "Failed to get gas price: {e}. Falling back to previous gas price. {} wei, {} gwei",
                base_fee,
                base_fee / 1_000_000_000
            );
        }
    }

    let native_bals = native_bals?;
    let nonces = nonces?;

    let erc20_bals = match erc20_bals {
        Some(b) => Some(b?),
        None => None,
    };

    metrics
        .total_rpc_calls
        .fetch_add(accts.iter().len() * 2, SeqCst);

    for (i, acct) in accts.iter_mut().enumerate() {
        if let Ok((_, b)) = &native_bals[i] {
            acct.native_bal = *b;
        }
        if let Ok((_, n)) = &nonces[i] {
            acct.nonce = *n;
        }
        if let Some(bals) = &erc20_bals {
            if let Ok((_, b)) = &bals[i] {
                acct.erc20_bal = *b;
            }
        }
    }
    trace!("Batch refreshed");

    Ok(())
}
