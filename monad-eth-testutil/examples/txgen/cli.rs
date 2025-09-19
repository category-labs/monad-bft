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

use clap::{Parser, Subcommand};
use url::Url;

use crate::prelude::*;

#[derive(Debug, Parser, Clone)]
#[command(name = "monad-node", about, long_about = None)]
pub struct CliConfig {
    /// Path to the config file to use instead of the cli args
    #[arg(long, global = true)]
    pub config_file: Option<String>,

    #[arg(long, global = true)]
    pub rpc_url: Option<Url>,

    /// Target tps of the generator
    #[arg(long, global = true)]
    pub tps: Option<u64>,

    /// Funded private keys used to seed native tokens to sender accounts
    #[arg(long, global = true)]
    pub root_private_keys: Option<Vec<String>>,

    /// Seed used to generate private keys for recipients
    #[arg(long, global = true)]
    pub recipient_seed: Option<u64>,

    /// Seed used to generate private keys for senders.
    /// If set the same as recipient seed, the accounts will be the same
    #[arg(long, global = true)]
    pub sender_seed: Option<u64>,

    /// Number of recipient accounts to generate and cycle between
    #[arg(long, global = true)]
    pub recipients: Option<usize>,

    /// Number of sender accounts to generate and cycle sending from
    #[arg(long, global = true)]
    pub senders: Option<usize>,

    /// How long to wait before refreshing balances. A function of the execution delay and block speed
    #[arg(long, global = true)]
    pub refresh_delay_secs: Option<f64>,

    /// Should the txgen query for erc20 balances
    /// This introduces many eth_calls which can affect performance and are not strictly needed for the gen to function
    #[arg(long, global = true)]
    pub erc20_balance_of: Option<bool>,

    /// Which generation mode to use. Corresponds to Generator impls
    #[command(subcommand)]
    pub gen_mode: CliGenMode,

    /// How many senders should be batched together when cycling between gen -> rpc sender -> refresher -> gen...
    #[arg(long, global = true)]
    pub sender_group_size: Option<usize>,

    /// How many txs should be generated per sender per cycle.
    /// Or put another way, how many txs should be generated before refreshing the nonce from chain state
    #[clap(long, global = true)]
    pub tx_per_sender: Option<usize>,

    /// Override for erc20 contract address
    #[clap(long, global = true)]
    pub erc20_contract: Option<String>,

    /// Override for ecmul contract address.
    #[clap(long, global = true)]
    pub ecmul_contract: Option<String>,

    /// Override for uniswap contract address
    #[clap(long, global = true)]
    pub uniswap_contract: Option<String>,

    /// Queries rpc for receipts of each sent tx when set. Queries per txhash, prefer `use_receipts_by_block` for efficiency
    #[clap(long, global = true)]
    pub use_receipts: Option<bool>,

    /// Queries rpc for receipts for each committed block and filters against txs sent by this txgen.
    /// More efficient
    #[clap(long, global = true)]
    pub use_receipts_by_block: Option<bool>,

    /// Fetches logs for each tx sent
    #[clap(long, global = true)]
    pub use_get_logs: Option<bool>,

    /// Chain id
    #[arg(long, global = true)]
    pub chain_id: Option<u64>,

    /// Minimum native amount in wei for each sender.
    /// When a sender has less than this amount, it's native balance is topped off from a root private key
    #[arg(long, global = true)]
    pub min_native_amount: Option<u128>,

    /// Native amount in wei transfered to each sender from an available root private key when the sender's
    /// native balance passes below `min_native_amount`
    #[arg(long, global = true)]
    pub seed_native_amount: Option<u128>,

    /// Writes `DEBUG` logs to ./debug.log
    #[arg(long, global = true)]
    pub debug_log_file: Option<bool>,

    /// Writes `TRACE` logs to ./trace.log
    #[arg(long, global = true)]
    pub trace_log_file: Option<bool>,

    #[arg(long, global = true)]
    pub use_static_tps_interval: Option<bool>,

    /// Otel endpoint
    #[arg(long, global = true)]
    pub otel_endpoint: Option<String>,

    /// Otel replica name
    #[arg(long, global = true)]
    pub otel_replica_name: Option<String>,
}

pub enum RequiredContract {
    None,
    ERC20,
    ECMUL,
    Uniswap,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CliGenMode {
    FewToMany {
        #[clap(long, default_value = "erc20")]
        tx_type: TxType,
    },
    ManyToMany {
        #[clap(long, default_value = "erc20")]
        tx_type: TxType,
    },
    Duplicates,
    RandomPriorityFee,
    HighCallData,
    HighCallDataLowGasLimit,
    SelfDestructs,
    NonDeterministicStorage,
    StorageDeletes,
    NullGen,
    ECMul,
    Uniswap,
}

impl From<CliGenMode> for GenMode {
    fn from(value: CliGenMode) -> Self {
        match value {
            CliGenMode::FewToMany { tx_type } => GenMode::FewToMany(FewToManyConfig { tx_type }),
            CliGenMode::ManyToMany { tx_type } => GenMode::ManyToMany(ManyToManyConfig { tx_type }),
            CliGenMode::Duplicates => GenMode::Duplicates,
            CliGenMode::RandomPriorityFee => GenMode::RandomPriorityFee,
            CliGenMode::HighCallData => GenMode::HighCallData,
            CliGenMode::HighCallDataLowGasLimit => GenMode::HighCallDataLowGasLimit,
            CliGenMode::SelfDestructs => GenMode::SelfDestructs,
            CliGenMode::NonDeterministicStorage => GenMode::NonDeterministicStorage,
            CliGenMode::StorageDeletes => GenMode::StorageDeletes,
            CliGenMode::NullGen => GenMode::NullGen,
            CliGenMode::ECMul => GenMode::ECMul,
            CliGenMode::Uniswap => GenMode::Uniswap,
        }
    }
}

impl From<CliConfig> for Config {
    fn from(value: CliConfig) -> Self {
        let mut config = Config {
            workload_groups: vec![value.clone().into()],
            ..Default::default()
        };

        if let Some(rpc_url) = value.rpc_url {
            config.rpc_urls = vec![rpc_url.to_string()];
        }

        if let Some(root_private_keys) = value.root_private_keys {
            config.root_private_keys = root_private_keys;
        }
        if let Some(refresh_delay_secs) = value.refresh_delay_secs {
            config.refresh_delay_secs = refresh_delay_secs;
        }

        if let Some(use_receipts) = value.use_receipts {
            config.use_receipts = use_receipts;
        }
        if let Some(use_receipts_by_block) = value.use_receipts_by_block {
            config.use_receipts_by_block = use_receipts_by_block;
        }
        if let Some(use_get_logs) = value.use_get_logs {
            config.use_get_logs = use_get_logs;
        }
        if let Some(chain_id) = value.chain_id {
            config.chain_id = chain_id;
        }
        if let Some(min_native_amount) = value.min_native_amount {
            config.min_native_amount = min_native_amount.to_string();
        }
        if let Some(seed_native_amount) = value.seed_native_amount {
            config.seed_native_amount = seed_native_amount.to_string();
        }
        if let Some(debug_log_file) = value.debug_log_file {
            config.debug_log_file = debug_log_file;
        }
        if let Some(trace_log_file) = value.trace_log_file {
            config.trace_log_file = trace_log_file;
        }
        if let Some(use_static_tps_interval) = value.use_static_tps_interval {
            config.use_static_tps_interval = use_static_tps_interval;
        }
        if let Some(otel_endpoint) = value.otel_endpoint {
            config.otel_endpoint = Some(otel_endpoint);
        }
        if let Some(otel_replica_name) = value.otel_replica_name {
            config.otel_replica_name = otel_replica_name;
        }
        config
    }
}

impl From<CliConfig> for WorkloadGroup {
    fn from(value: CliConfig) -> Self {
        WorkloadGroup {
            // Effectively infinite runtime
            runtime_minutes: 100_000_000_000.0,
            traffic_gens: vec![value.into()],
            ..Default::default()
        }
    }
}

impl From<CliConfig> for TrafficGen {
    fn from(value: CliConfig) -> Self {
        let mut traffic_gen = TrafficGen {
            gen_mode: value.gen_mode.into(),
            ..Default::default()
        };

        if let Some(tps) = value.tps {
            traffic_gen.tps = tps;
        }
        if let Some(recipient_seed) = value.recipient_seed {
            traffic_gen.recipient_seed = recipient_seed;
        }
        if let Some(sender_seed) = value.sender_seed {
            traffic_gen.sender_seed = sender_seed;
        }
        if let Some(recipients) = value.recipients {
            traffic_gen.recipients = recipients;
        }
        if let Some(senders) = value.senders {
            traffic_gen.senders = Some(senders);
        }
        if let Some(erc20_balance_of) = value.erc20_balance_of {
            traffic_gen.erc20_balance_of = erc20_balance_of;
        }
        if let Some(sender_group_size) = value.sender_group_size {
            traffic_gen.sender_group_size = Some(sender_group_size);
        }
        if let Some(tx_per_sender) = value.tx_per_sender {
            traffic_gen.tx_per_sender = Some(tx_per_sender);
        }
        traffic_gen
    }
}
