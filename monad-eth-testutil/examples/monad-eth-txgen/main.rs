use std::error::Error;

use alloy_json_rpc::RpcError;
use alloy_rpc_client::{ClientBuilder, ReqwestClient, Waiter};
use alloy_transport::TransportErrorKind;
use clap::{CommandFactory, FromArgMatches};
use futures::{stream::FuturesUnordered, StreamExt};
use reth_primitives::TransactionSigned;
use tokio::time::Instant;

use crate::{
    cli::EthTxGenCli,
    generator::{EthTxGenerator, EthTxGeneratorConfig},
    state::ChainState,
};

mod account;
mod cli;
mod generator;
mod state;

fn main() {
    let mut cmd = EthTxGenCli::command();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| cmd.error(clap::error::ErrorKind::Io, e).exit());

    if let Err(e) = runtime.block_on(run(cmd)) {
        panic!("monad eth txgen crashed: {:?}", e);
    }
}

async fn run(mut cmd: clap::Command) -> Result<(), Box<dyn Error>> {
    let EthTxGenCli { rpc_url, config } =
        EthTxGenCli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

    let tx_generator_config = EthTxGeneratorConfig::new_from_file(config)?;

    let client: ReqwestClient = ClientBuilder::default().http(rpc_url);

    let (mut chain_state_manager_handle, chain_state) =
        ChainState::new_with_manager(client.clone()).await;

    let mut tx_generator = EthTxGenerator::new(tx_generator_config, chain_state.clone()).await;

    let mut next_batch_time = Instant::now();

    let mut batch_futs = FuturesUnordered::new();

    loop {
        tokio::select! {
            () = tokio::time::sleep_until(next_batch_time) => {
                let (tx_batch, _next_batch_time) = tx_generator.generate().await;

                next_batch_time = _next_batch_time;

                send_batch(&client, &mut batch_futs, tx_batch).await.expect("send batch");
            }

            result = batch_futs.select_next_some(), if !batch_futs.is_empty() => {
                let tx_hash = result.expect("tx hash valid");

                let _ = tx_hash;
            }

            error = &mut chain_state_manager_handle => {
                println!("chain state manager handle terminated, error: {error:?}");
                break;
            }
        }
    }

    Ok(())
}

async fn send_batch(
    client: &ReqwestClient,
    batch_futs: &mut FuturesUnordered<Waiter<String>>,
    tx_batch: Vec<TransactionSigned>,
) -> Result<(), RpcError<TransportErrorKind>> {
    if tx_batch.is_empty() {
        return Ok(());
    }

    let mut batch_req = client.new_batch();

    for tx in tx_batch {
        batch_futs.push(batch_req.add_call("eth_sendRawTransaction", &[tx.envelope_encoded()])?);
    }

    batch_req.send().await
}
