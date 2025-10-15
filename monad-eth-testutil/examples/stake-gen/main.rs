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
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use alloy_primitives::{
    hex::{self},
    Address, Bytes, TxHash, U256, U64,
};
use alloy_rlp::{BytesMut, Encodable};
use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use alloy_rpc_types::TransactionReceipt;
use alloy_sol_types::SolValue;
use alloy_transport::TransportError;
use clap::Parser;
use eyre::{Context, Result};
use faucet::Faucet;
use monad_types::{Epoch, Stake};
use node_data::{Delegator, NodeData};
use serde::Deserialize;
use staking_contract::{StakingContract, ACTIVE_VALIDATOR_STAKE_MON, MIN_STAKE_MON, MON};
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use url::Url;

pub mod faucet;
pub mod node_data;
pub mod staking_contract;

#[derive(Debug, Parser)]
#[command(name = "stake-gen", about, long_about = None)]
pub struct CliConfig {
    #[arg(long, global = true, default_value = "http://localhost:8080")]
    pub rpc_url: Url,
    #[arg(long, global = true)]
    pub config_file: String,
    #[arg(long, global = true, default_value_t = 20143)]
    pub chain_id: u64,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub known_nodes: Vec<NodeData>,
    pub faucets: Vec<String>,
    pub switch_interval_secs: u64,
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();

        let content = std::fs::read_to_string(path)?;
        if path.extension().unwrap_or_default() == "json" {
            serde_json::from_str(&content)
                .wrap_err_with(|| format!("Failed to parse JSON config: {}", path.display()))
        } else {
            toml::from_str(&content)
                .wrap_err_with(|| format!("Failed to parse TOML config: {}", path.display()))
        }
    }
}

async fn get_transaction_count(client: &ReqwestClient, address: &Address) -> u64 {
    let address = address.to_string();
    let nonce = client
        .request::<_, U64>("eth_getTransactionCount", [&address, "latest"])
        .await
        .unwrap();

    nonce.to()
}

async fn send_raw_tx(
    client: &ReqwestClient,
    signed_tx: TxEnvelope,
) -> Result<TxHash, TransportError> {
    let mut rlp_encoded_tx = BytesMut::new();
    signed_tx.encode(&mut rlp_encoded_tx);

    client
        .request::<_, TxHash>(
            "eth_sendRawTransaction",
            &[format!("0x{}", hex::encode(rlp_encoded_tx))],
        )
        .await
}

async fn wait_for_tx_receipt(
    client: &ReqwestClient,
    tx_hash: TxHash,
    poll_interval: Duration,
) -> Result<TransactionReceipt> {
    loop {
        match client
            .request::<_, Option<TransactionReceipt>>("eth_getTransactionReceipt", [tx_hash])
            .await?
        {
            Some(receipt) => return Ok(receipt),
            None => {
                sleep(poll_interval).await;
            }
        }
    }
}

async fn split_known_nodes(
    client: &ReqwestClient,
    staking_contract: &StakingContract,
    known_nodes: Vec<NodeData>,
) -> (VecDeque<NodeData>, VecDeque<NodeData>) {
    let active_validator_stake = U256::from(MON) * U256::from(ACTIVE_VALIDATOR_STAKE_MON);

    let mut active_validators = VecDeque::new();
    let mut inactive_validators = VecDeque::new();

    for mut node in known_nodes {
        node.nonce = get_transaction_count(client, &node.eth_address).await;

        let val_id = staking_contract
            .get_validator_id_from_pubkey(&node.secp_pubkey)
            .await;

        if val_id != 0 {
            assert!(
                node.val_id.is_none_or(|known_id| val_id == known_id),
                "incorrect known node id"
            );

            let val_data = staking_contract.get_validator(val_id).await;

            let known_bls_key = Bytes::from(node.bls_pubkey.compress());
            let contract_bls_key = val_data.blsPubkey;
            assert_eq!(known_bls_key, contract_bls_key, "incorrect bls key");

            node.val_id = Some(val_id);
            node.current_stake = Stake(val_data.stake);

            if val_data.stake < active_validator_stake {
                inactive_validators.push_back(node);
            } else {
                active_validators.push_back(node);
            }
        } else {
            inactive_validators.push_back(node);
        }
    }

    (active_validators, inactive_validators)
}

async fn add_new_validator(
    client: &ReqwestClient,
    staking_contract: &StakingContract,
    node: &mut NodeData,
) -> bool {
    // add validator only if it doesn't exist
    if node.val_id.is_some() {
        return true;
    }

    let min_stake = Stake::from(U256::from(MON) * U256::from(MIN_STAKE_MON));

    info!(
        node_name = node.name,
        stake =? min_stake,
        delegator_key =? Bytes::from(node.secp_privkey.secret_bytes()),
        "Adding new node to the staking contract",
    );

    let tx_hash = staking_contract
        .send_add_validator_tx(node, min_stake.0, None)
        .await
        .expect("add validator doesn't fail");

    let receipt = wait_for_tx_receipt(&client, tx_hash, Duration::from_secs(1))
        .await
        .expect("transaction completed");

    let tx_hash_hex = hex::encode_prefixed(tx_hash);
    let trace: Result<serde_json::Value, _> = client
        .request("debug_traceTransaction", [tx_hash_hex])
        .await;

    if receipt.status() {
        let trace_hex = trace
            .unwrap()
            .get("output")
            .and_then(|val| val.as_str())
            .expect("trace has return value")
            .to_owned();
        let val_id: U256 = U256::abi_decode(&hex::decode(trace_hex).unwrap(), true).unwrap();

        node.val_id = Some(val_id.to::<u64>());
        node.current_stake = Stake::from(min_stake);
    }

    receipt.status()
}

async fn add_node_to_validator_set(
    node: &mut NodeData,
    client: &ReqwestClient,
    staking_contract: &StakingContract,
) -> bool {
    if node.val_id.is_none() {
        if !add_new_validator(client, staking_contract, node).await {
            info!(node =? node, "failed to add validator to staking contract");
            return false;
        }
    }
    let val_id = node.val_id.unwrap();
    let active_validator_stake = U256::from(ACTIVE_VALIDATOR_STAKE_MON) * U256::from(MON);
    let delegate_amount = active_validator_stake - node.current_stake.0;

    info!(
        node_name = node.name,
        current_stake =? node.current_stake,
        next_stake =? active_validator_stake,
        ?delegate_amount,
        delegator_key =? Bytes::from(node.secp_privkey.secret_bytes()),
        "Delegating to node",
    );

    if let Some(tx_hash) = staking_contract
        .send_delegate_tx(node, val_id, delegate_amount)
        .await
    {
        let receipt = wait_for_tx_receipt(&client, tx_hash, Duration::from_secs(1))
            .await
            .unwrap();
        if receipt.status() {
            node.current_stake = Stake(active_validator_stake);
            return true;
        }
    }

    false
}

async fn remove_node_from_validator_set(
    node: &mut NodeData,
    client: &ReqwestClient,
    staking_contract: &StakingContract,
) -> bool {
    let val_id = node.val_id.unwrap();
    let next_stake = U256::from(ACTIVE_VALIDATOR_STAKE_MON - 1_000_000) * U256::from(MON);
    let undelegate_amount = node.current_stake.0 - next_stake;
    let next_withdrawal_id = {
        let mut free = 1;
        let pending_withdrawals = node.pending_withdrawals.entry(val_id).or_default();
        loop {
            if pending_withdrawals.contains_key(&free) {
                free += 1;
                continue;
            }

            // add to pending list if the withdrawal id already exists
            let existing_req = staking_contract
                .get_withdrawal_request(val_id, node.eth_address, free)
                .await;
            if existing_req.epoch != U256::ZERO {
                pending_withdrawals.insert(free, Epoch(existing_req.epoch.to()));
                continue;
            }

            break;
        }

        free
    };

    info!(
        node_name = node.name,
        current_stake =? node.current_stake,
        ?next_stake,
        ?undelegate_amount,
        delegator_key =? Bytes::from(node.secp_privkey.secret_bytes()),
        "Undelegating from node",
    );

    if let Some(tx_hash) = staking_contract
        .send_undelegate_tx(node, val_id, next_withdrawal_id, undelegate_amount)
        .await
    {
        let receipt = wait_for_tx_receipt(&client, tx_hash, Duration::from_secs(1))
            .await
            .unwrap();
        if receipt.status() {
            let withdraw_request = staking_contract
                .get_withdrawal_request(val_id, node.eth_address, next_withdrawal_id)
                .await;

            info!(delegator = node.name, val_id, withdrawal_id = next_withdrawal_id, ?withdraw_request, "Fetched withdraw request after undelegating");

            node.pending_withdrawals
                .entry(val_id)
                .or_default()
                .insert(next_withdrawal_id, Epoch(withdraw_request.epoch.to()));

            node.current_stake = Stake(next_stake);
            return true;
        }
    }

    false
}

async fn complete_pending_withdrawals<'a, I>(
    client: &ReqwestClient,
    staking_contract: &StakingContract,
    current_epoch: Epoch,
    delegators: I,
) where
    I: Iterator<Item = &'a mut dyn Delegator>,
{
    for delegator in delegators {
        let mut withdrawals_ready = Vec::new();
        for (val_id, pending_withdrawals) in delegator.get_pending_withdrawals() {
            for (withdrawal_id, epoch) in pending_withdrawals {
                if current_epoch >= *epoch {
                    withdrawals_ready.push((*val_id, *withdrawal_id, *epoch));
                }
            }
        }

        for (val_id, withdrawal_id, epoch) in withdrawals_ready {
            info!(val_id, withdrawal_id, ?epoch, "Withdrawing stake after undelegation");

            if let Some(tx_hash) = staking_contract
                .send_withdraw_tx(delegator, val_id, withdrawal_id)
                .await
            {
                let receipt = wait_for_tx_receipt(&client, tx_hash, Duration::from_secs(1))
                    .await
                    .unwrap();
                if receipt.status() {
                    info!(val_id, withdrawal_id, ?epoch, "Withdrawal succeeded");
                    delegator
                        .get_pending_withdrawals()
                        .get_mut(&val_id)
                        .unwrap()
                        .remove(&withdrawal_id)
                        .unwrap();
                } else {
                    let tx_hash_hex = hex::encode_prefixed(tx_hash);
                    let trace_ret: Result<serde_json::Value, _> = client
                        .request("debug_traceTransaction", [tx_hash_hex])
                        .await;
                
                    let trace = trace_ret.unwrap();

                    error!(val_id, withdrawal_id, ?trace, "Failed to withdraw");
                }
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(std::io::stdout)
        .with_span_list(false)
        .with_current_span(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("failed to set logger");

    info!("Starting stake gen");

    let cli_config = CliConfig::parse();
    let config = Config::from_file(cli_config.config_file).unwrap();

    let mut faucet = Faucet::new(
        config
            .faucets
            .get(0)
            .expect("at least 1 provided faucet key"),
        cli_config.rpc_url.clone(),
        cli_config.chain_id,
    )
    .await;
    let client = ClientBuilder::default().http(cli_config.rpc_url.clone());
    let staking_contract = StakingContract::new(cli_config.rpc_url, cli_config.chain_id);

    // fund all validators
    let mut fund_tx_hashes = Vec::new();
    let mut known_nodes = config.known_nodes;
    for node in known_nodes.iter_mut() {
        let amount = U256::from(MON) * U256::from(2 * ACTIVE_VALIDATOR_STAKE_MON);
        fund_tx_hashes.push(
            faucet
                .fund_address(node.eth_address, amount)
                .await
                .expect("faucet doesn't fail"),
        );

        node.nonce = get_transaction_count(&client, &node.eth_address).await;
    }

    // sleep until all validators are funded
    sleep(Duration::from_secs(5)).await;

    let (mut active_validators, mut inactive_validators) =
        split_known_nodes(&client, &staking_contract, known_nodes).await;

    // add all inactive validators first
    while let Some(mut node) = inactive_validators.pop_front() {
        assert!(add_node_to_validator_set(&mut node, &client, &staking_contract).await);
        active_validators.push_back(node);
    }

    // wake up every epoch to add one validator and remove one validator
    let (mut current_epoch, _in_epoch_delay) = staking_contract.get_epoch().await;
    loop {
        let (mut epoch, mut in_epoch_delay) = staking_contract.get_epoch().await;
        while epoch <= current_epoch || in_epoch_delay {
            info!(
                epoch =? current_epoch,
                "Sleeping for {} seconds", config.switch_interval_secs
            );
            sleep(Duration::from_secs(config.switch_interval_secs)).await;
            (epoch, in_epoch_delay) = staking_contract.get_epoch().await;
        }
        current_epoch = epoch;
        info!(?current_epoch, "Current Epoch");

        let val_set = staking_contract.get_execution_validator_set().await;
        for val in &active_validators {
            assert!(val_set.contains(&U256::from(val.val_id.unwrap())));
        }

        let undelegated_node = if let Some(mut node_to_undelegate) = active_validators.pop_front() {
            assert!(
                remove_node_from_validator_set(&mut node_to_undelegate, &client, &staking_contract)
                    .await
            );
            Some(node_to_undelegate)
        } else {
            None
        };

        let delegated_node = if let Some(mut node_to_delegate) = inactive_validators.pop_front() {
            assert!(
                add_node_to_validator_set(&mut node_to_delegate, &client, &staking_contract).await
            );
            Some(node_to_delegate)
        } else {
            None
        };

        if let Some(undelegated_node) = undelegated_node {
            inactive_validators.push_back(undelegated_node);
        }

        if let Some(delegated_node) = delegated_node {
            active_validators.push_back(delegated_node);
        }

        complete_pending_withdrawals(
            &client,
            &staking_contract,
            current_epoch,
            active_validators
                .iter_mut()
                .chain(inactive_validators.iter_mut())
                .map(|node| node as &mut dyn Delegator),
        )
        .await;
    }
}
