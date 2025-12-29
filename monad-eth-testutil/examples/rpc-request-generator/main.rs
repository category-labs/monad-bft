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
    collections::{BTreeMap, BTreeSet},
    iter::Iterator,
};

use alloy_json_rpc::RpcError;
use alloy_network::TransactionResponse;
use alloy_primitives::{Address, BlockNumber, Bytes, LogData, U256, U64};
use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use alloy_rpc_types::{Filter, TransactionRequest};
use alloy_rpc_types_eth::BlockTransactionHashes;
use alloy_rpc_types_trace::geth::{
    GethDebugBuiltInTracerType, GethDebugTracerType, GethDebugTracingOptions, GethTrace,
};
use clap::Parser;
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use rand::seq::IteratorRandom;
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Registry,
};
use url::Url;

// bytecode for eth_call contract deployment simulation (test contract in flexnet)
const CONTRACT_BYTECODE: &[u8] = &[
    0x60, 0x45, 0x80, 0x60, 0x0e, 0x60, 0x00, 0x39, 0x80, 0x60, 0x00, 0xf3, 0x50, 0xfe, 0x7f, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0x03, 0x60, 0x16, 0x00, 0x81, 0x60, 0x20,
    0x82, 0x37, 0x80, 0x35, 0x82, 0x34, 0xf5, 0x80, 0x15, 0x56, 0x03, 0x95, 0x78, 0x18, 0x2f, 0x5b,
    0x80, 0x82, 0x52, 0x50, 0x50, 0x60, 0x14, 0x60, 0x0c, 0xf3,
];

pub struct DropTimer<F>
where
    F: Fn(Duration),
{
    start: Instant,
    threshold: Duration,
    trip: F,
}

impl<F> DropTimer<F>
where
    F: Fn(Duration),
{
    pub fn start(threshold: Duration, trip: F) -> Self {
        Self {
            start: Instant::now(),
            threshold,
            trip,
        }
    }
}

impl<F> Drop for DropTimer<F>
where
    F: Fn(Duration),
{
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if elapsed <= self.threshold {
            return;
        }
        (self.trip)(elapsed)
    }
}

#[derive(Debug, Parser)]
#[command(name = "rpc-request-generator", about, long_about = None)]
pub struct Config {
    #[arg(long, global = true, default_value = "http://localhost:8080")]
    pub rpc_url: Url,
    #[arg(long, global = true, default_value_t = 5)]
    pub requests_per_new_block: u64,
    #[arg(long, global = true, default_value_t = 5)]
    pub historical_blocks_per_round: u64,
    #[arg(long, global = true, default_value_t = 200)]
    pub historical_block_window: u64,
    #[arg(long, global = true, default_value_t = 15)]
    pub max_distance_from_tip: u64,
}

struct TipRefresher {
    tip: BlockNumber,
    tip_sender: tokio::sync::mpsc::Sender<(BlockNumber, BlockNumber)>,
    client: ReqwestClient,
}

impl TipRefresher {
    fn new(rpc_url: Url, sender: tokio::sync::mpsc::Sender<(BlockNumber, BlockNumber)>) -> Self {
        Self {
            tip: 0,
            tip_sender: sender,
            client: ClientBuilder::default().http(rpc_url),
        }
    }

    async fn run(mut self) {
        self.tip = self
            .client
            .request_noparams::<U64>("eth_blockNumber")
            .map_resp(|res| res.to())
            .await
            .unwrap();
        loop {
            let resp = self
                .client
                .request_noparams::<U64>("eth_blockNumber")
                .map_resp(|res| res.to())
                .await;

            let Ok(tip) = resp else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            if tip > self.tip {
                let Ok(()) = self.tip_sender.send((self.tip + 1, tip)).await else {
                    warn!("tip sender channel full");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                self.tip = tip;
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

struct RpcRequestGenerator {
    rpc_url: Url,
    requests_per_new_block: u64,
    historical_blocks_per_round: u64,
    historical_block_window: u64,
    max_distance_from_tip: u64,
    pending_indexing: BTreeMap<BlockNumber, JoinHandle<()>>,
    uniq_addrs: BTreeSet<Address>,
    total_indexed: u64,
    total_failed: u64,
}

#[derive(Debug)]
struct BlockIndexError {
    block_number: BlockNumber,
    error: RpcError<alloy_transport::TransportErrorKind>,
}

impl RpcRequestGenerator {
    fn new(
        rpc_url: Url,
        requests_per_new_block: u64,
        historical_blocks_per_round: u64,
        historical_block_window: u64,
        max_distance_from_tip: u64,
    ) -> Self {
        Self {
            rpc_url,
            requests_per_new_block,
            historical_blocks_per_round,
            historical_block_window,
            max_distance_from_tip,
            pending_indexing: Default::default(),
            uniq_addrs: Default::default(),
            total_indexed: 0,
            total_failed: 0,
        }
    }

    async fn process_new_block(
        &mut self,
        block_number: BlockNumber,
        index_done_sender: tokio::sync::mpsc::Sender<
            Result<(BlockNumber, Vec<Address>), BlockIndexError>,
        >,
    ) {
        let url = self.rpc_url.clone();
        let sender = index_done_sender.clone();
        let requests_per_block = self.requests_per_new_block;
        let join_handle = tokio::spawn(async move {
            Self::index_block(url, requests_per_block, block_number, sender).await
        });
        self.pending_indexing.insert(block_number, join_handle);

        let mut rng = rand::thread_rng();
        let start_block = (block_number.saturating_sub(self.historical_block_window)).max(1);
        let historical_blocks = (start_block..block_number)
            .choose_multiple(&mut rng, self.historical_blocks_per_round as usize);

        for block_number in historical_blocks {
            let url = self.rpc_url.clone();
            let sender = index_done_sender.clone();
            let requests_per_block = self.requests_per_new_block;
            let join_handle = tokio::spawn(async move {
                Self::index_block(url, requests_per_block, block_number, sender).await
            });
            self.pending_indexing.insert(block_number, join_handle);
        }
    }

    async fn run(&mut self) {
        // start block tip refresher task
        let (tip_sender, mut tip_receiver) = tokio::sync::mpsc::channel(8);
        // let (mut index_sender, index_receiver) = tokio::sync
        let (index_done_sender, mut index_done_receiver) = tokio::sync::mpsc::channel(32);

        let refresher = TipRefresher::new(self.rpc_url.clone(), tip_sender);
        tokio::spawn(async move { refresher.run().await });

        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // select on tip refresher channel and completion channel
        loop {
            tokio::select! {
                Some((index_from, index_to)) = tip_receiver.recv() => {
                    debug!(from=index_from, to=index_to, "index range");

                    for block_number in index_from..=index_to {
                        self.process_new_block(block_number, index_done_sender.clone()).await;
                    }
                }

                Some(index_result) = index_done_receiver.recv() => {
                    let block_number = match index_result {
                        Ok((num, addrs)) => {
                            self.total_indexed += 1;
                            self.uniq_addrs.extend(addrs.into_iter());
                            num
                        },
                        Err(err) => {
                            self.total_failed += 1;
                            warn!(?err.block_number, ?err.error,"failed to index block");
                            err.block_number
                        }
                    };
                    self.pending_indexing.remove(&block_number);
                }

                _ = status_interval.tick() => {
                    let diff = if !self.pending_indexing.is_empty() {
                        self.pending_indexing.last_key_value().unwrap().0 - self.pending_indexing.first_key_value().unwrap().0
                    } else {
                        0
                    };
                    info!(pressure = self.pending_indexing.len(), ?diff,indexed=?self.total_indexed, failed=?self.total_failed, addrs=self.uniq_addrs.len(), "indexer state");
                    if self.pending_indexing.len() as u64 > self.max_distance_from_tip {
                        warn!("Indexer has fallen too far behind");
                        std::process::exit(1);
                    }
                    // cleanup old indexing task that will never succeed
                    while !self.pending_indexing.is_empty() {
                        let last_index = *(self.pending_indexing.last_key_value().unwrap().0);
                        let first_index = *(self.pending_indexing.first_key_value().unwrap().0);
                        if last_index - first_index >= 1000 {
                            self.total_failed += 1;
                            let (_, join_handle) = self.pending_indexing.pop_first().unwrap();
                            join_handle.abort();
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    // FIXME: fix rpc error handling
    fn handle_rpc_error(
        rpc_error: RpcError<alloy_transport::TransportErrorKind>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        match rpc_error {
            // retry on transport error
            RpcError::Transport(_) => Ok(()),
            _ => Err(rpc_error),
        }
    }

    async fn get_block_by_number(
        client: &ReqwestClient,
        block_number: BlockNumber,
    ) -> Result<alloy_rpc_types_eth::Block, BlockIndexError> {
        loop {
            match client
                .request::<_, alloy_rpc_types_eth::Block>(
                    "eth_getBlockByNumber",
                    (U64::from(block_number), true),
                )
                .await
            {
                Ok(block) => return Ok(block),
                Err(err) => {
                    Self::handle_rpc_error(err).map_err(|e| BlockIndexError {
                        block_number,
                        error: e,
                    })?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    }

    async fn get_transaction_by_hash<T: TransactionResponse>(
        client: &ReqwestClient,
        txn_hashes: BlockTransactionHashes<'_, T>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        let txn_hashes = txn_hashes.into_iter().collect::<Vec<_>>();
        for chunk in txn_hashes.chunks(1000) {
            let futs = loop {
                let mut batch = client.new_batch();

                let futs = chunk
                    .iter()
                    .map(|txn| {
                        let params = [txn];
                        batch
                            .add_call::<_, alloy_rpc_types_eth::Transaction>(
                                "eth_getTransactionByHash",
                                &params,
                            )
                            .map(|w| w)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        Self::handle_rpc_error(err)?;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            };
            for fut in futs {
                fut.await?;
            }
        }
        Ok(())
    }

    async fn get_transaction_receipts_by_hash<T: TransactionResponse>(
        client: &ReqwestClient,
        txn_hashes: BlockTransactionHashes<'_, T>,
    ) -> Result<
        Vec<alloy_rpc_types_eth::TransactionReceipt>,
        RpcError<alloy_transport::TransportErrorKind>,
    > {
        let mut receipts = Vec::with_capacity(txn_hashes.size_hint().0);
        let txn_hashes = txn_hashes.into_iter().collect::<Vec<_>>();
        for chunk in txn_hashes.chunks(1000) {
            let futs = loop {
                let mut batch = client.new_batch();

                let futs = chunk
                    .iter()
                    .map(|txn| {
                        let params = [txn];
                        batch
                            .add_call::<_, alloy_rpc_types_eth::TransactionReceipt>(
                                "eth_getTransactionReceipt",
                                &params,
                            )
                            .map(|w| w)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        Self::handle_rpc_error(err)?;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            };

            for fut in futs {
                receipts.push(fut.await?);
            }
        }
        Ok(receipts)
    }

    async fn get_block_receipts(
        client: &ReqwestClient,
        block_number: BlockNumber,
    ) -> Result<
        Vec<alloy_rpc_types_eth::TransactionReceipt>,
        RpcError<alloy_transport::TransportErrorKind>,
    > {
        loop {
            let resp = client
                .request::<_, Vec<alloy_rpc_types_eth::TransactionReceipt>>(
                    "eth_getBlockReceipts",
                    (U64::from(block_number),),
                )
                .await;
            match resp {
                Ok(_) => return resp,
                Err(err) => {
                    Self::handle_rpc_error(err)?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    }

    async fn get_logs(
        client: &ReqwestClient,
        block_number: BlockNumber,
    ) -> Result<Vec<alloy_rpc_types_eth::Log<LogData>>, RpcError<alloy_transport::TransportErrorKind>>
    {
        loop {
            let filter = Filter::new();
            let filter = filter.from_block(block_number);
            let filter = filter.to_block(block_number);
            let resp = client
                .request::<_, Vec<alloy_rpc_types_eth::Log<LogData>>>("eth_getLogs", (filter,))
                .await;
            match resp {
                Ok(_) => return resp,
                Err(err) => {
                    Self::handle_rpc_error(err)?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    }

    async fn eth_call(
        client: &ReqwestClient,
        block_number: BlockNumber,
        addrs: Vec<Address>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        for chunk in addrs.chunks(1000) {
            let futs = loop {
                let mut batch = client.new_batch();

                let futs = chunk
                    .iter()
                    .map(|addr| {
                        let call_request = TransactionRequest {
                            from: Some(*addr),
                            input: Some(Bytes::from(CONTRACT_BYTECODE)).into(),
                            ..Default::default()
                        };
                        let params = (call_request, U64::from(block_number));
                        batch
                            .add_call::<_, Bytes>("eth_call", &params)
                            .map(|w| w)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        debug!(?err, "eth_call error");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            };

            for fut in futs {
                fut.await?;
            }
        }
        Ok(())
    }

    async fn get_balances(
        client: &ReqwestClient,
        block_number: BlockNumber,
        addrs: Vec<Address>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        for chunk in addrs.chunks(1000) {
            let futs = loop {
                let mut batch = client.new_batch();
                let futs = chunk
                    .iter()
                    .map(|addr| {
                        let params = (addr, U64::from(block_number));
                        batch
                            .add_call::<_, U256>("eth_getBalance", &params)
                            .map(|w| w)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        debug!(?err, "eth_getBalance error");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            };

            for fut in futs {
                fut.await?;
            }
        }
        Ok(())
    }

    async fn debug_trace_transaction<T: TransactionResponse>(
        client: &ReqwestClient,
        txn_hashes: BlockTransactionHashes<'_, T>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        let txn_hashes = txn_hashes.into_iter().collect::<Vec<_>>();
        for chunk in txn_hashes.chunks(1000) {
            let futs = loop {
                let mut batch = client.new_batch();

                let futs = chunk
                    .iter()
                    .map(|txn| {
                        let config = GethDebugTracingOptions {
                            tracer: Some(GethDebugTracerType::BuiltInTracer(
                                GethDebugBuiltInTracerType::CallTracer,
                            )),
                            ..Default::default()
                        };
                        let params = (txn, config);
                        batch
                            .add_call::<_, GethTrace>("debug_traceTransaction", &params)
                            .map(|w| w)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        Self::handle_rpc_error(err)?;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            };

            for fut in futs {
                fut.await?;
            }
        }
        Ok(())
    }

    async fn debug_trace_block_by_number(
        client: &ReqwestClient,
        block_number: BlockNumber,
    ) -> Result<
        Vec<alloy_rpc_types_trace::geth::GethTrace>,
        RpcError<alloy_transport::TransportErrorKind>,
    > {
        loop {
            let resp = client
                .request::<_, Vec<alloy_rpc_types_trace::geth::GethTrace>>(
                    "debug_traceBlockByNumber",
                    (U64::from(block_number),),
                )
                .await;
            match resp {
                Ok(_) => return resp,
                Err(err) => {
                    Self::handle_rpc_error(err)?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    }

    async fn index_block(
        rpc_url: Url,
        requests_per_block: u64,
        block_number: BlockNumber,
        result_sender: tokio::sync::mpsc::Sender<
            Result<(BlockNumber, Vec<Address>), BlockIndexError>,
        >,
    ) {
        let _drop_timer = DropTimer::start(Duration::from_millis(300), |elapsed| {
            info!(?block_number, ?elapsed, "Indexing duration")
        });
        debug!(?block_number, "indexing..");
        let client = ClientBuilder::default().http(rpc_url);

        let start = Instant::now();
        let block = match Self::get_block_by_number(&client.clone(), block_number).await {
            Ok(header) => header,
            Err(err) => {
                assert!(result_sender.send(Err(err)).await.is_ok());
                return;
            }
        };
        let duration = start.elapsed();
        info!(?block_number, ?duration, "eth_getBlockByNumber duration");

        assert!(block.transactions.is_full() || block.transactions.is_empty());
        info!(
            len = block.transactions.len(),
            block = block_number,
            "transactions"
        );

        let uniq_addrs = if !block.transactions.is_empty() {
            // generate requests for block receipts and traces
            let start = Instant::now();
            let (receipts_results, _, _) = tokio::join!(
                async {
                    let mut futures = (0..requests_per_block)
                        .map(|_| Self::get_block_receipts(&client, block_number))
                        .collect::<FuturesUnordered<_>>();
                    let mut receipts = Vec::new();
                    while let Some(result) = futures.next().await {
                        match result {
                            Ok(r) => receipts.push(r),
                            Err(err) => {
                                let _ = result_sender
                                    .send(Err(BlockIndexError {
                                        block_number,
                                        error: err,
                                    }))
                                    .await;
                                return Err(());
                            }
                        }
                    }
                    Ok(receipts.pop().unwrap())
                },
                async {
                    let mut futures = (0..requests_per_block)
                        .map(|_| Self::get_logs(&client, block_number))
                        .collect::<FuturesUnordered<_>>();

                    let mut logs = Vec::new();
                    while let Some(result) = futures.next().await {
                        match result {
                            Ok(t) => logs.push(t),
                            Err(err) => {
                                let _ = result_sender
                                    .send(Err(BlockIndexError {
                                        block_number,
                                        error: err,
                                    }))
                                    .await;
                                return Err(());
                            }
                        }
                    }
                    Ok(logs.pop().unwrap())
                },
                async {
                    let mut futures = (0..requests_per_block)
                        .map(|_| Self::debug_trace_block_by_number(&client, block_number))
                        .collect::<FuturesUnordered<_>>();

                    let mut traces = Vec::new();
                    while let Some(result) = futures.next().await {
                        match result {
                            Ok(t) => traces.push(t),
                            Err(err) => {
                                let _ = result_sender
                                    .send(Err(BlockIndexError {
                                        block_number,
                                        error: err,
                                    }))
                                    .await;
                                return Err(());
                            }
                        }
                    }
                    Ok(traces.pop().unwrap())
                },
            );
            let receipts = match receipts_results {
                Ok(r) => r,
                Err(_) => {
                    warn!(?block_number, "Unable to retrieve block receipts");
                    return;
                }
            };
            let txn_hashes = block.transactions.hashes();
            let duration = start.elapsed();
            info!(
                ?block_number,
                ?duration,
                "eth_getBlockReceipts, eth_getLogs and debug_traceBlockByNumber duration"
            );

            // account balances and eth call
            let start = Instant::now();
            let addrs = receipts
                .into_iter()
                .map(|receipt| receipt.from)
                .collect::<Vec<_>>();
            debug!(n = addrs.len(), "reading account balances");
            let (balances_res, eth_call_res, trace_res) = tokio::join!(
                Self::get_balances(&client, block_number, addrs.clone()),
                Self::eth_call(&client, block_number, addrs.clone()),
                Self::debug_trace_transaction(&client, txn_hashes),
            );
            if let Err(ref err) = balances_res {
                warn!(?block_number, ?err, "Error fetching balances");
            }
            if let Err(ref err) = eth_call_res {
                warn!(?block_number, ?err, "Error in eth_call");
            }
            if let Err(ref err) = trace_res {
                warn!(?block_number, ?err, "Error tracing transaction");
            }
            let duration = start.elapsed();
            let num_addr = addrs.len();
            info!(
                ?block_number,
                ?duration,
                ?num_addr,
                "eth_getBalance, eth_call and debug_traceTransaction"
            );
            addrs.into_iter().unique().collect()
        } else {
            Vec::new()
        };

        assert!(result_sender
            .send(Ok((block_number, uniq_addrs)))
            .await
            .is_ok());
    }
}

#[tokio::main]
async fn main() {
    let config = Config::parse();

    let subscriber = Registry::default()
        .with(
            FmtLayer::default()
                .json()
                .with_span_events(FmtSpan::NONE)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(std::io::stdout)
                .with_ansi(false),
        )
        .with(EnvFilter::from_default_env());
    tracing::subscriber::set_global_default(subscriber).expect("failed to set logger");

    let mut indexer = RpcRequestGenerator::new(
        config.rpc_url,
        config.requests_per_new_block,
        config.historical_blocks_per_round,
        config.historical_block_window,
        config.max_distance_from_tip,
    );
    indexer.run().await
}
