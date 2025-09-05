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

use alloy_primitives::Uint;
use alloy_rlp::Encodable;
use alloy_rpc_types::{Block, FilterChanges, TransactionReceipt};
use eyre::Context;
use serde_json::json;

use super::*;
use crate::{config::Config, shared::blockstream::BlockStream};

pub struct CommittedTxWatcher {
    sent_txs: Arc<DashMap<TxHash, Instant>>,
    metrics: Arc<Metrics>,
    delay: Duration,
    blockstream: BlockStream,
    client: ReqwestClient,

    // extra rpc flags
    use_receipts: bool,
    use_receipts_by_block: bool,
    use_get_logs: bool,
    // use_by_hash: bool,
}

impl CommittedTxWatcher {
    pub async fn new(
        client: &ReqwestClient,
        sent_txs: &Arc<DashMap<TxHash, Instant>>,
        metrics: &Arc<Metrics>,
        delay: Duration,
        config: &Config,
    ) -> Self {
        Self {
            client: client.clone(),
            sent_txs: Arc::clone(sent_txs),
            metrics: Arc::clone(metrics),
            delay,
            blockstream: BlockStream::new(client.clone(), Duration::from_millis(50), false)
                .await
                .expect("Failed to fetch initial block number for blockstream"),

            use_receipts: config.use_receipts,
            use_get_logs: config.use_get_logs,
            use_receipts_by_block: config.use_receipts_by_block,
            // use_by_hash,
        }
    }

    pub async fn run(mut self) {
        while let Some(block) = self.blockstream.next().await {
            let block = match block {
                Ok(b) => b,
                Err(e) => {
                    warn!("Blockstream returned error: {e}");
                    continue;
                }
            };

            let mut ours = 0;
            for hash in block.transactions.hashes() {
                if self.sent_txs.remove(&hash).is_some() {
                    ours += 1;
                }
            }

            self.metrics.total_committed_txs.fetch_add(ours, SeqCst);

            let now = Instant::now();
            self.sent_txs.retain(|_, v| *v + self.delay > now);

            if self.use_receipts || self.use_receipts_by_block {
                // prefer by block
                if !self.use_receipts_by_block {
                    if let Err(e) = Self::receipts_for_block_slow(
                        self.client.clone(),
                        self.metrics.clone(),
                        &block,
                    )
                    .await
                    {
                        error!("Failed to get receipts for block: {e}");
                    }
                } else if let Err(e) =
                    Self::receipts_for_block(self.client.clone(), self.metrics.clone(), &block)
                        .await
                {
                    error!("Failed to get receipts for block: {e}");
                }
            }
            if self.use_get_logs {
                if let Err(e) =
                    Self::logs_for_block(self.client.clone(), self.metrics.clone(), &block).await
                {
                    error!("Failed to get logs for block: {e}");
                }
            }
        }
    }

    async fn logs_for_block(
        client: ReqwestClient,
        metrics: Arc<Metrics>,
        block: &Block,
    ) -> Result<()> {
        let mut num_logs = 0;
        // let mut erc20_transfers = 0;
        // let mut erc20_value_transfered = U256::ZERO;

        let block_num = block.header.number;

        let params = json! {{
            "toBlock": format!("0x{:x}", block_num),
            "fromBlock": format!("0x{:x}", block_num),
        }};

        metrics.logs_rpc_calls.fetch_add(1, SeqCst);
        let logs_resp: FilterChanges = client
            .request("eth_getLogs", [&params])
            .await
            .inspect_err(|_| {
                metrics.logs_rpc_calls_error.fetch_add(1, SeqCst);
            })
            .wrap_err(format!("params: {}", params))?;

        match logs_resp {
            FilterChanges::Logs(logs) => {
                num_logs += logs.len();

                // todo: figure out how to parse logs into IERC20::Transfer event
                // for log in logs {
                //     if let Ok(transfer_data) = IERC20::Transfer::abi_decode_data(&log.data, false) {
                //         erc20_transfers += 1;
                //         erc20_value_transfered += transfer_data.0;
                //     }
                // }
            }
            FilterChanges::Empty => {
                debug!("No logs in block");
            }
            FilterChanges::Hashes(_) | FilterChanges::Transactions(_) => {
                warn!("Unexpected response from eth_getLogs")
            }
        }

        metrics.logs_total.fetch_add(num_logs, SeqCst);
        // metrics
        //     .logs_erc20_transfers
        //     .fetch_add(erc20_transfers, SeqCst);
        // if let Ok(mut val) = metrics.logs_erc20_total_value_transfered.write() {
        //     *val += erc20_value_transfered;
        // }
        Ok(())
    }

    async fn receipts_for_block(
        client: ReqwestClient,
        metrics: Arc<Metrics>,
        block: &Block,
    ) -> Result<()> {
        let mut tx_success = 0;
        let mut tx_failure = 0;
        let mut gas_consumed = U256::ZERO;
        let mut contract_addresses = Vec::new();

        let rxs: Vec<TransactionReceipt> = {
            let method = "eth_getBlockReceipts";
            let block_num = block.header.number;
            let mut block_num_bytes = [0u8; 32];
            block_num.encode(&mut (&mut block_num_bytes as &mut [u8]));

            metrics.receipts_rpc_calls.fetch_add(1, SeqCst);
            client
                .request(method, [format!("0x{:x}", block_num)])
                .await
                .map_err(|e| {
                    metrics.logs_rpc_calls_error.fetch_add(1, SeqCst);
                    // todo: properly wrap error
                    eyre::eyre!("Failed to get logs for block {block_num} {e}")
                })?
        };

        for rx in rxs {
            match rx.status() {
                true => tx_success += 1,
                false => tx_failure += 1,
            };

            gas_consumed += Uint::from(rx.gas_used);

            if let Some(contract_address) = rx.contract_address {
                contract_addresses.push(contract_address);
            }
        }

        metrics.receipts_tx_success.fetch_add(tx_success, SeqCst);
        metrics.receipts_tx_failure.fetch_add(tx_failure, SeqCst);
        if let Ok(mut x) = metrics.receipts_gas_consumed.write() {
            *x += gas_consumed;
        }
        Ok(())
    }

    async fn receipts_for_block_slow(
        client: ReqwestClient,
        metrics: Arc<Metrics>,
        block: &Block,
    ) -> Result<()> {
        let mut rpc_calls = 0;
        let mut rpc_calls_error = 0;
        let mut tx_success = 0;
        let mut tx_failure = 0;
        let mut gas_consumed = U256::ZERO;
        let mut contract_addresses = Vec::new();

        for hash in block.transactions.hashes() {
            rpc_calls += 1;
            let rx: TransactionReceipt =
                match client.request("eth_getTransactionReceipt", [hash]).await {
                    Ok(rx) => rx,
                    Err(e) => {
                        error!(tx_hash = hash.to_string(), "Failed to get rx for tx: {e}");
                        rpc_calls_error += 1;
                        continue;
                    }
                };

            match rx.status() {
                true => tx_success += 1,
                false => tx_failure += 1,
            };

            gas_consumed += Uint::from(rx.gas_used);

            if let Some(contract_address) = rx.contract_address {
                contract_addresses.push(contract_address);
            }
        }

        metrics.receipts_rpc_calls.fetch_add(rpc_calls, SeqCst);
        metrics
            .receipts_rpc_calls_error
            .fetch_add(rpc_calls_error, SeqCst);
        metrics.receipts_tx_success.fetch_add(tx_success, SeqCst);
        metrics.receipts_tx_failure.fetch_add(tx_failure, SeqCst);
        if let Ok(mut x) = metrics.receipts_gas_consumed.write() {
            *x += gas_consumed;
        }
        Ok(())
    }
}
