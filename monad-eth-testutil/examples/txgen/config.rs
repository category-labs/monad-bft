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

use std::str::FromStr;

use eyre::bail;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    prelude::*,
    shared::{ecmul::ECMul, erc20::ERC20, uniswap::Uniswap},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,

    /// Funded private keys used to seed native tokens to sender accounts
    #[serde(default = "default_root_private_keys")]
    pub root_private_keys: Vec<String>,

    /// Traffic generation configurations to run sequentially
    pub traffic_gen: Vec<TrafficGen>,

    /// How long to wait before refreshing balances. A function of the execution delay and block speed
    #[serde(default = "default_refresh_delay_secs")]
    pub refresh_delay_secs: f64,

    /// Queries rpc for receipts of each sent tx when set. Queries per txhash, prefer `use_receipts_by_block` for efficiency
    #[serde(default = "default_use_receipts")]
    pub use_receipts: bool,

    /// Queries rpc for receipts for each committed block and filters against txs sent by this txgen.
    /// More efficient
    #[serde(default = "default_use_receipts_by_block")]
    pub use_receipts_by_block: bool,

    /// Fetches logs for each tx sent
    #[serde(default = "default_use_get_logs")]
    pub use_get_logs: bool,

    /// Base fee used when calculating gas costs and value
    #[serde(default = "default_base_fee_gwei")]
    pub base_fee_gwei: u128,

    /// Chain id
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,

    /// Minimum native amount in wei for each sender.
    /// When a sender has less than this amount, it's native balance is topped off from a root private key
    #[serde(default = "default_min_native_amount")]
    pub min_native_amount: u128,

    /// Native amount in wei transfered to each sender from an available root private key when the sender's
    /// native balance passes below `min_native_amount`
    #[serde(default = "default_seed_native_amount")]
    pub seed_native_amount: u128,

    /// Writes `DEBUG` logs to ./debug.log
    #[serde(default = "default_debug_log_file")]
    pub debug_log_file: bool,

    /// Writes `TRACE` logs to ./trace.log
    #[serde(default = "default_trace_log_file")]
    pub trace_log_file: bool,

    #[serde(default = "default_use_static_tps_interval")]
    pub use_static_tps_interval: bool,

    /// Otel endpoint
    pub otel_endpoint: Option<String>,

    /// Otel replica name
    #[serde(default = "default_otel_replica_name")]
    pub otel_replica_name: String,
}

// Default value functions
fn default_rpc_url() -> String {
    "http://localhost:8545".to_string()
}

fn default_tps() -> u64 {
    1000
}

fn default_root_private_keys() -> Vec<String> {
    vec![
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d".to_string(),
        "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a".to_string(),
        "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6".to_string(),
        "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a".to_string(),
        "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba".to_string(),
    ]
}

fn default_recipient_seed() -> u64 {
    10101
}

fn default_sender_seed() -> u64 {
    10101
}

fn default_recipients() -> usize {
    100000
}

fn default_refresh_delay_secs() -> f64 {
    5.0
}

fn default_erc20_balance_of() -> bool {
    false
}

fn default_use_receipts() -> bool {
    false
}

fn default_use_receipts_by_block() -> bool {
    false
}

fn default_use_get_logs() -> bool {
    false
}

fn default_base_fee_gwei() -> u128 {
    50
}

fn default_chain_id() -> u64 {
    20143
}

fn default_min_native_amount() -> u128 {
    100_000_000_000_000_000_000
}

fn default_seed_native_amount() -> u128 {
    1_000_000_000_000_000_000_000
}

fn default_debug_log_file() -> bool {
    false
}

fn default_trace_log_file() -> bool {
    false
}

fn default_use_static_tps_interval() -> bool {
    false
}

fn default_otel_replica_name() -> String {
    "default".to_string()
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&content)?;
        Ok(config)
    }

    pub fn to_file(&self, path: &str) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn tx_per_sender(&self, traffic_gen: &TrafficGen) -> usize {
        if let Some(x) = traffic_gen.tx_per_sender {
            return x;
        }
        match &traffic_gen.gen_mode {
            GenMode::FewToMany(..) => 500,
            GenMode::ManyToMany(..) => 10,
            GenMode::Duplicates(..) => 10,
            GenMode::RandomPriorityFee(..) => 10,
            GenMode::HighCallData(..) => 10,
            GenMode::SelfDestructs(..) => 10,
            GenMode::NonDeterministicStorage(..) => 10,
            GenMode::StorageDeletes(..) => 10,
            GenMode::NullGen(..) => 0,
            GenMode::ECMul(..) => 10,
            GenMode::Uniswap(..) => 10,
            GenMode::HighCallDataLowGasLimit(..) => 30,
            GenMode::ReserveBalance(..) => 1,
            GenMode::SystemSpam(..) => 500,
            GenMode::SystemKeyNormal(..) => 500,
            GenMode::SystemKeyNormalRandomPriorityFee(..) => 500,
        }
    }

    pub fn sender_group_size(&self, traffic_gen: &TrafficGen) -> usize {
        if let Some(x) = traffic_gen.sender_group_size {
            return x;
        }
        match &traffic_gen.gen_mode {
            GenMode::FewToMany(..) => 100,
            GenMode::ManyToMany(..) => 100,
            GenMode::Duplicates(..) => 100,
            GenMode::RandomPriorityFee(..) => 100,
            GenMode::NonDeterministicStorage(..) => 100,
            GenMode::StorageDeletes(..) => 100,
            GenMode::NullGen(..) => 10,
            GenMode::SelfDestructs(..) => 10,
            GenMode::HighCallData(..) => 10,
            GenMode::ECMul(..) => 10,
            GenMode::HighCallDataLowGasLimit(..) => 3,
            GenMode::Uniswap(..) => 20,
            GenMode::ReserveBalance(..) => 100,
            GenMode::SystemSpam(..) => 1,
            GenMode::SystemKeyNormal(..) => 1,
            GenMode::SystemKeyNormalRandomPriorityFee(..) => 1,
        }
    }

    pub fn senders(&self, traffic_gen: &TrafficGen) -> usize {
        if let Some(x) = traffic_gen.senders {
            return x;
        }
        match &traffic_gen.gen_mode {
            GenMode::FewToMany(..) => 1000,
            GenMode::ManyToMany(..) => 2500,
            GenMode::Duplicates(..) => 2500,
            GenMode::RandomPriorityFee(..) => 2500,
            GenMode::NonDeterministicStorage(..) => 2500,
            GenMode::StorageDeletes(..) => 2500,
            GenMode::NullGen(..) => 100,
            GenMode::SelfDestructs(..) => 100,
            GenMode::HighCallData(..) => 100,
            GenMode::HighCallDataLowGasLimit(..) => 100,
            GenMode::ECMul(..) => 100,
            GenMode::Uniswap(..) => 200,
            GenMode::ReserveBalance(..) => 2500,
            GenMode::SystemSpam(..) => 1,
            GenMode::SystemKeyNormal(..) => 1,
            GenMode::SystemKeyNormalRandomPriorityFee(..) => 1,
        }
    }

    pub fn required_contract(&self, traffic_gen: &TrafficGen) -> RequiredContract {
        use RequiredContract::*;
        match &traffic_gen.gen_mode {
            GenMode::FewToMany(config) => match config.tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::ManyToMany(config) => match config.tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::Duplicates(..) => ERC20,
            GenMode::RandomPriorityFee(..) => ERC20,
            GenMode::HighCallData(..) => None,
            GenMode::HighCallDataLowGasLimit(..) => None,
            GenMode::SelfDestructs(..) => None,
            GenMode::NonDeterministicStorage(..) => ERC20,
            GenMode::StorageDeletes(..) => ERC20,
            GenMode::NullGen(..) => None,
            GenMode::ECMul(..) => ECMUL,
            GenMode::Uniswap(..) => Uniswap,
            GenMode::ReserveBalance(..) => None,
            GenMode::SystemSpam(..) => None,
            GenMode::SystemKeyNormal(..) => None,
            GenMode::SystemKeyNormalRandomPriorityFee(..) => None,
        }
    }

    pub fn base_fee(&self) -> u128 {
        self.base_fee_gwei
            .checked_mul(10u128.pow(9))
            .expect("Gwei must be convertable to wei using u128")
    }

    pub fn rpc_url(&self) -> Result<Url> {
        self.rpc_url.parse().map_err(Into::into)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficGen {
    /// How long to run this traffic pattern in seconds
    pub runtime: u64,

    /// Target tps of the generator for this traffic phase
    #[serde(default = "default_tps")]
    pub tps: u64,

    /// Seed used to generate private keys for recipients
    #[serde(default = "default_recipient_seed")]
    pub recipient_seed: u64,

    /// Seed used to generate private keys for senders.
    /// If set the same as recipient seed, the accounts will be the same
    #[serde(default = "default_sender_seed")]
    pub sender_seed: u64,

    /// Number of recipient accounts to generate and cycle between
    #[serde(default = "default_recipients")]
    pub recipients: usize,

    /// Number of sender accounts to generate and cycle sending from
    pub senders: Option<usize>,

    /// Should the txgen query for erc20 balances
    /// This introduces many eth_calls which can affect performance and are not strictly needed for the gen to function
    #[serde(default = "default_erc20_balance_of")]
    pub erc20_balance_of: bool,

    /// Which generation mode to use. Corresponds to Generator impls
    pub gen_mode: GenMode,

    /// How many senders should be batched together when cycling between gen -> rpc sender -> refresher -> gen...
    pub sender_group_size: Option<usize>,

    /// How many txs should be generated per sender per cycle.
    /// Or put another way, how many txs should be generated before refreshing the nonce from chain state
    pub tx_per_sender: Option<usize>,
}

pub enum RequiredContract {
    None,
    ERC20,
    ECMUL,
    Uniswap,
}

#[derive(Debug, Clone)]
pub enum DeployedContract {
    None,
    ERC20(ERC20),
    ECMUL(ECMul),
    Uniswap(Uniswap),
}

impl DeployedContract {
    pub fn erc20(self) -> Result<ERC20> {
        match self {
            Self::ERC20(erc20) => Ok(erc20),
            _ => bail!("Expected erc20, found {:?}", &self),
        }
    }

    pub fn ecmul(self) -> Result<ECMul> {
        match self {
            Self::ECMUL(x) => Ok(x),
            _ => bail!("Expected ecmul, found {:?}", &self),
        }
    }

    pub fn uniswap(self) -> Result<Uniswap> {
        match self {
            Self::Uniswap(uniswap) => Ok(uniswap),
            _ => bail!("Expected uniswap, found {:?}", &self),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenMode {
    FewToMany(FewToManyConfig),
    ManyToMany(ManyToManyConfig),
    Duplicates(DuplicatesConfig),
    RandomPriorityFee(RandomPriorityFeeConfig),
    HighCallData(HighCallDataConfig),
    HighCallDataLowGasLimit(HighCallDataLowGasLimitConfig),
    SelfDestructs(SelfDestructsConfig),
    NonDeterministicStorage(NonDeterministicStorageConfig),
    StorageDeletes(StorageDeletesConfig),
    NullGen(NullGenConfig),
    ECMul(ECMulConfig),
    Uniswap(UniswapConfig),
    ReserveBalance(ReserveBalanceConfig),
    SystemSpam(SystemSpamConfig),
    SystemKeyNormal(SystemKeyNormalConfig),
    SystemKeyNormalRandomPriorityFee(SystemKeyNormalRandomPriorityFeeConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FewToManyConfig {
    #[serde(default = "default_tx_type")]
    pub tx_type: TxType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManyToManyConfig {
    #[serde(default = "default_tx_type")]
    pub tx_type: TxType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicatesConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RandomPriorityFeeConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighCallDataConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighCallDataLowGasLimitConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfDestructsConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonDeterministicStorageConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDeletesConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullGenConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ECMulConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniswapConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReserveBalanceConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemSpamConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemKeyNormalConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemKeyNormalRandomPriorityFeeConfig {}

fn default_tx_type() -> TxType {
    TxType::ERC20
}

#[derive(Deserialize, Clone, Copy, Debug, Serialize)]
pub enum TxType {
    #[serde(rename = "erc20")]
    ERC20,
    #[serde(rename = "native")]
    Native,
}

impl FromStr for TxType {
    type Err = eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "erc20" => Ok(TxType::ERC20),
            "native" => Ok(TxType::Native),
            _ => Err(eyre::eyre!("Invalid TxType: {}", s)),
        }
    }
}
