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
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Duration,
};

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{
    hex::{self, FromHex},
    Address, Bytes, FixedBytes, TxHash, TxKind, U256, U64,
};
use alloy_rlp::{BytesMut, Encodable};
use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use alloy_rpc_types::TransactionReceipt;
use alloy_signer::{k256::ecdsa::SigningKey, SignerSync};
use alloy_signer_local::LocalSigner;
use alloy_sol_types::SolValue;
use blst::min_pk::{PublicKey as BlsPublicKey, SecretKey as BlsPrivateKey};
use clap::Parser;
use eyre::{Context, Result};
use monad_types::{Epoch, Stake};
use secp256k1::{PublicKey as SecpPublicKey, SecretKey as SecpPrivateKey};
use serde::{de::Error, Deserialize};
use staking_contract::{StakingContract, ACTIVE_VALIDATOR_STAKE_MON, MIN_STAKE_MON, MON};
use tokio::time::sleep;
use url::Url;

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

pub struct NodeData {
    pub name: Option<String>,

    pub secp_privkey: SecpPrivateKey,
    pub secp_pubkey: SecpPublicKey,

    pub bls_privkey: BlsPrivateKey,
    pub bls_pubkey: BlsPublicKey,

    pub eth_address: Address,
    pub nonce: Rc<RefCell<u64>>,

    pub val_id: Option<u64>,
    pub current_stake: Stake,
}

impl std::cmp::PartialEq for NodeData {
    fn eq(&self, other: &Self) -> bool {
        self.secp_pubkey == other.secp_pubkey
    }
}

impl std::cmp::Eq for NodeData {}

impl std::cmp::PartialOrd for NodeData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.secp_pubkey.partial_cmp(&other.secp_pubkey)
    }
}

impl std::cmp::Ord for NodeData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::fmt::Debug for NodeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeData")
            .field("name", &self.name)
            .field(
                "secp_privkey",
                &hex::encode(self.secp_privkey.secret_bytes()),
            )
            .field("secp_pubkey", &hex::encode(self.secp_pubkey.serialize()))
            .field("bls_privkey", &hex::encode(self.bls_privkey.serialize()))
            .field("bls_pubkey", &hex::encode(self.bls_pubkey.compress()))
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawNodeData {
            name: Option<String>,
            secp_privkey: String,
            bls_privkey: String,
        }
        let raw_node_data = RawNodeData::deserialize(deserializer)?;

        let secp_key_hex = raw_node_data.secp_privkey;
        let secp_key_bytes = FixedBytes::<32>::from_hex(secp_key_hex).map_err(D::Error::custom)?;

        let bls_key_hex = raw_node_data.bls_privkey;
        let bls_key_bytes = FixedBytes::<32>::from_hex(bls_key_hex).map_err(D::Error::custom)?;

        Ok(NodeData::new(
            raw_node_data.name,
            secp_key_bytes.into(),
            bls_key_bytes.into(),
        ))
    }
}

impl NodeData {
    pub fn new(
        name: Option<String>,
        secp_privkey_bytes: [u8; 32],
        bls_privkey_bytes: [u8; 32],
    ) -> Self {
        let secp_privkey = SecpPrivateKey::from_slice(&secp_privkey_bytes).expect(&format!(
            "Invalid secp private key: {:?}",
            secp_privkey_bytes
        ));
        let secp_pubkey = secp_privkey.public_key(secp256k1::SECP256K1);
        let bls_privkey = BlsPrivateKey::from_bytes(&bls_privkey_bytes)
            .expect(&format!("Invalid bls private key: {:?}", bls_privkey_bytes));
        let bls_pubkey = bls_privkey.sk_to_pk();

        Self {
            name,

            secp_privkey,
            secp_pubkey,

            bls_privkey,
            bls_pubkey,

            eth_address: Address::from_private_key(
                &SigningKey::from_slice(&secp_privkey_bytes).unwrap(),
            ),
            nonce: Rc::new(RefCell::new(0)),

            val_id: None,
            current_stake: Stake::from(0),
        }
    }

    pub async fn update_nonce(&mut self, client: &ReqwestClient) {
        *self.nonce.borrow_mut() = get_transaction_count(client, &self.eth_address).await;
    }
}

pub struct Delegator {
    pub signer: LocalSigner<SigningKey>,
    pub secp_privkey: SecpPrivateKey,

    pub eth_address: Address,
    pub nonce: Rc<RefCell<u64>>,

    pub withdrawal_id: u8,
    // (val_id, withdrawal_id, epoch)
    pub pending_withdrawals: Vec<(u64, u8, Epoch)>,
}

impl std::fmt::Debug for Delegator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delegator")
            .field("SecpPrivateKey", &self.secp_privkey)
            .field("Address", &self.eth_address)
            .finish_non_exhaustive()
    }
}

impl Delegator {
    pub fn random() -> Self {
        let signer = LocalSigner::random();
        let secp_privkey = SecpPrivateKey::from_slice(&signer.to_bytes().0).unwrap();
        let eth_address = signer.address();

        Self {
            signer,
            secp_privkey,

            eth_address,
            nonce: Rc::new(RefCell::new(0)),

            withdrawal_id: 0,
            pending_withdrawals: Vec::new(),
        }
    }

    pub fn from_node_data(node_data: &NodeData) -> Self {
        let signer = LocalSigner::from_bytes(&node_data.secp_privkey.secret_bytes().into())
            .expect("valid secp private key");
        Self {
            signer,
            secp_privkey: node_data.secp_privkey,

            eth_address: node_data.eth_address,
            nonce: node_data.nonce.clone(),

            withdrawal_id: 0,
            pending_withdrawals: Vec::new(),
        }
    }
}

pub struct Faucet {
    pub secp_privkey: SecpPrivateKey,
    pub nonce: u64,

    pub client: ReqwestClient,
    pub chain_id: u64,
}

impl Faucet {
    pub async fn new(faucet_key_hex: &str, rpc_url: Url, chain_id: u64) -> Self {
        let client = ClientBuilder::default().http(rpc_url);
        let faucet_key =
            SecpPrivateKey::from_slice(&hex::decode(faucet_key_hex).expect("invalid faucet key"))
                .expect("invalid faucet key");
        let public_key = &faucet_key
            .public_key(secp256k1::SECP256K1)
            .serialize_uncompressed()[1..];
        let nonce = get_transaction_count(&client, &Address::from_raw_public_key(public_key)).await;

        Faucet {
            secp_privkey: faucet_key,
            nonce,

            client,
            chain_id,
        }
    }

    pub async fn fund_address(&mut self, eth_address: Address, amount: U256) -> Option<TxHash> {
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce: self.nonce,
            gas_limit: 30_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(eth_address),
            value: amount,
            access_list: Default::default(),
            input: Default::default(),
        };

        let signer = LocalSigner::from_bytes(&FixedBytes::<32>::from_slice(
            &self.secp_privkey.secret_bytes(),
        ))
        .unwrap();

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

        let mut rlp_encoded_tx = BytesMut::new();
        signed_tx.encode(&mut rlp_encoded_tx);

        match self
            .client
            .request::<_, TxHash>(
                "eth_sendRawTransaction",
                &[format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await
        {
            Ok(tx_hash) => {
                self.nonce += 1;
                Some(tx_hash)
            }
            Err(rpc_error) => {
                println!("Faucet RPC error: {}", rpc_error);
                None
            }
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

async fn wait_for_tx_receipt(
    client: &ReqwestClient,
    tx_hash: TxHash,
    interval: Duration,
) -> Result<TransactionReceipt> {
    loop {
        match client
            .request::<_, Option<TransactionReceipt>>("eth_getTransactionReceipt", [tx_hash])
            .await?
        {
            Some(receipt) => return Ok(receipt),
            None => {
                sleep(interval).await;
            }
        }
    }
}

async fn delegate_random_inactive_node(
    inactive_validators: &mut BTreeSet<NodeData>,
    staking_contract: &StakingContract,
) -> Option<NodeData> {
    if let Some(mut node_to_delegate) = inactive_validators.pop_first() {
        let next_stake_mon = U256::from(ACTIVE_VALIDATOR_STAKE_MON);
        let next_stake = Stake::from(next_stake_mon * U256::from(MON));
        let delegate_amount = next_stake - node_to_delegate.current_stake;

        let mut delegator = Delegator::from_node_data(&node_to_delegate);
        println!(
            "Delegating {:?} to {:?} using {:?}",
            delegate_amount,
            node_to_delegate.name,
            Bytes::from(delegator.secp_privkey.secret_bytes())
        );

        if let Some(tx_hash) = staking_contract
            .delegate(
                &mut delegator,
                node_to_delegate.val_id.unwrap(),
                delegate_amount.0,
            )
            .await
        {
            node_to_delegate.current_stake = next_stake;
            return Some(node_to_delegate);
        }
    }

    None
}

async fn undelegate_random_active_node(
    active_validators: &mut BTreeSet<NodeData>,
    staking_contract: &StakingContract,
) -> Option<NodeData> {
    let mut node_to_undelegate = active_validators.pop_first().unwrap();
    let next_stake_mon = U256::from(ACTIVE_VALIDATOR_STAKE_MON - 1_000_000);
    let next_stake = Stake::from(next_stake_mon * U256::from(MON));
    let undelegate_amount = node_to_undelegate.current_stake - next_stake;

    let mut undelegator = Delegator::from_node_data(&node_to_undelegate);
    println!(
        "Undelegating {:?} from {:?} using {:?}",
        undelegate_amount,
        node_to_undelegate.name,
        Bytes::from(undelegator.secp_privkey.secret_bytes()),
    );

    if let Some(tx_hash) = staking_contract
        .undelegate(
            &mut undelegator,
            node_to_undelegate.val_id.unwrap(),
            undelegate_amount.0,
        )
        .await
    {
        node_to_undelegate.current_stake = next_stake;
        return Some(node_to_undelegate);
    }

    None
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("Starting stake gen");

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
    let staking_contract = StakingContract::new(cli_config.rpc_url.clone(), cli_config.chain_id);

    let client = ClientBuilder::default().http(cli_config.rpc_url);

    // fund all validators
    let mut known_nodes = config.known_nodes;
    let mut fund_tx_hashes = Vec::new();
    for node in &known_nodes {
        let amount = U256::from(MON) * U256::from(2 * ACTIVE_VALIDATOR_STAKE_MON);
        fund_tx_hashes.push(
            faucet
                .fund_address(node.eth_address, amount)
                .await
                .expect("faucet doesn't fail"),
        );

        *node.nonce.borrow_mut() = get_transaction_count(&client, &node.eth_address).await;
    }

    // sleep until all validators are funded
    sleep(Duration::from_secs(5)).await;

    // add all validators
    for node in &mut known_nodes {
        let min_stake = Stake::from(U256::from(MON) * U256::from(MIN_STAKE_MON));
        let tx_hash = staking_contract
            .add_validator(node, min_stake.0, None, None)
            .await
            .expect("add validator doesn't fail");

        let receipt = wait_for_tx_receipt(&client, tx_hash, Duration::from_secs(1))
            .await
            .expect("transaction completed");

        let trace: serde_json::Value = client
            .request("debug_traceTransaction", [tx_hash])
            .await
            .unwrap();

        if receipt.status() {
            let trace_hex = trace
                .get("output")
                .and_then(|val| val.as_str())
                .expect("trace has return value");
            let val_id: U256 = U256::abi_decode(&hex::decode(trace_hex).unwrap(), true).unwrap();

            node.val_id = Some(val_id.to::<u64>());
            node.current_stake = Stake::from(min_stake);
        }
    }

    // populate the validator ids for known_nodes
    // TODO remove this
    {
        let mut unknown_val_ids = known_nodes
            .iter_mut()
            .filter_map(|node| {
                node.val_id
                    .is_none()
                    .then_some((Bytes::from(node.secp_pubkey.serialize()), node))
            })
            .collect::<BTreeMap<_, _>>();
        if !unknown_val_ids.is_empty() {
            let mut val_id = 1;
            loop {
                let val = staking_contract.get_validator(val_id).await;

                if val.authAddress == Address::ZERO {
                    break;
                }

                if let Some(node) = unknown_val_ids.get_mut(&val.secpPubkey) {
                    (**node).val_id = Some(val_id);
                    (**node).current_stake = Stake(val.stake);
                }

                val_id += 1;
            }
        }
    }

    let mut active_validators: BTreeSet<NodeData> = BTreeSet::new();
    let mut inactive_validators: BTreeSet<NodeData> = BTreeSet::new();

    // delegate to initial validators
    for mut node in known_nodes {
        let active_stake = Stake::from(U256::from(MON) * U256::from(ACTIVE_VALIDATOR_STAKE_MON));
        if let Some(amount_for_active_stake) = active_stake.0.checked_sub(node.current_stake.0) {
            if amount_for_active_stake == U256::ZERO {
                continue;
            }

            let mut delegator = Delegator::from_node_data(&node);
            println!(
                "Delegating {:?} to {:?} using {:?}",
                amount_for_active_stake,
                node.name,
                Bytes::from(delegator.secp_privkey.secret_bytes()),
            );
            staking_contract
                .delegate(
                    &mut delegator,
                    node.val_id.unwrap(),
                    amount_for_active_stake,
                )
                .await;

            node.current_stake = active_stake;
        }

        active_validators.insert(node);
    }

    let (mut current_epoch, _in_epoch_delay) = staking_contract.get_epoch().await;
    loop {
        let (mut epoch, _) = staking_contract.get_epoch().await;
        while epoch <= current_epoch {
            println!(
                "Current Epoch: {:?}. Sleeping for {} seconds",
                current_epoch, config.switch_interval_secs
            );
            sleep(Duration::from_secs(config.switch_interval_secs)).await;
            (epoch, _) = staking_contract.get_epoch().await;
        }
        current_epoch = epoch;
        println!("Current Epoch: {:?}", current_epoch);

        let val_set = staking_contract.get_consensus_validator_set().await;
        for val in &active_validators {
            assert!(val_set.contains(&U256::from(val.val_id.unwrap())));
        }

        let undelegated_node =
            undelegate_random_active_node(&mut active_validators, &staking_contract).await;

        let delegated_node =
            delegate_random_inactive_node(&mut inactive_validators, &staking_contract).await;

        if let Some(undelegated_node) = undelegated_node {
            inactive_validators.insert(undelegated_node);
        }
        
        if let Some(delegated_node) = delegated_node {
            active_validators.insert(delegated_node);
        }
    }
}
