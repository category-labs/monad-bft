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
    shared::{ecmul::ECMul, eip7702::EIP7702, erc20::ERC20, uniswap::Uniswap},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Config {
    #[serde(default)]
    pub rpc_urls: Vec<String>,

    #[serde(default)]
    pub ws_url: String,

    /// Funded private keys used to seed native tokens to sender accounts
    pub root_private_keys: Vec<String>,

    /// Workload group configurations to run sequentially
    /// One or more TrafficGens are allowed per workload group
    pub workload_groups: Vec<WorkloadGroup>,

    /// How long to wait before refreshing balances. A function of the execution delay and block speed
    pub refresh_delay_secs: f64,

    /// Queries rpc for receipts of each sent tx when set. Queries per txhash, prefer `use_receipts_by_block` for efficiency
    pub use_receipts: bool,

    /// Queries rpc for receipts for each committed block and filters against txs sent by this txgen.
    /// More efficient
    pub use_receipts_by_block: bool,

    /// Fetches logs for each tx sent
    pub use_get_logs: bool,

    /// Chain id
    pub chain_id: u64,

    /// Minimum native amount in wei for each sender.
    /// When a sender has less than this amount, it's native balance is topped off from a root private key
    pub min_native_amount: String,

    /// Native amount in wei transfered to each sender from an available root private key when the sender's
    /// native balance passes below `min_native_amount`
    pub seed_native_amount: String,

    /// Writes `DEBUG` logs to ./debug.log
    pub debug_log_file: bool,

    /// Writes `TRACE` logs to ./trace.log
    pub trace_log_file: bool,

    pub use_static_tps_interval: bool,

    /// Otel endpoint
    pub otel_endpoint: Option<String>,

    /// Otel replica name
    pub otel_replica_name: String,

    /// Gas limit for contract deployment transactions
    pub gas_limit_contract_deployment: Option<u64>,

    /// Gas limit for contract call transactions (native and ERC20)
    pub set_tx_gas_limit: Option<u64>,

    /// Static priority fee for transactions (native and ERC20)
    pub priority_fee: Option<u128>,

    /// Range for random priority fee (min,max in wei)
    pub random_priority_fee_range: Option<(u128, u128)>,

    /// Override for ERC20 contract address
    pub erc20_contract: Option<String>,

    /// Override for native contract address
    pub native_contract: Option<String>,

    /// Report directory
    pub report_dir: Option<String>,

    /// Prometheus URL
    pub prom_url: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc_urls: vec!["http://localhost:8545".to_string()],
            ws_url: "ws://localhost:8546".to_string(),
            root_private_keys: vec![
                "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
                "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d".to_string(),
                "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a".to_string(),
                "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6".to_string(),
                "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a".to_string(),
                "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba".to_string(),
            ],
            workload_groups: vec![],
            refresh_delay_secs: 5.0,
            use_receipts: false,
            use_receipts_by_block: false,
            use_get_logs: false,
            chain_id: 20143,
            min_native_amount: "100_000_000_000_000_000_000".to_string(),
            seed_native_amount: "1_000_000_000_000_000_000_000".to_string(),
            debug_log_file: false,
            trace_log_file: false,
            use_static_tps_interval: false,
            otel_endpoint: None,
            otel_replica_name: "default".to_string(),
            gas_limit_contract_deployment: None,
            set_tx_gas_limit: None,
            priority_fee: None,
            random_priority_fee_range: None,
            erc20_contract: None,
            native_contract: None,
            report_dir: Some("reports".to_string()),
            prom_url: None,
        }
    }
}

impl TrafficGen {
    pub fn tx_per_sender(&self) -> usize {
        if let Some(x) = self.tx_per_sender {
            return x;
        }
        match &self.gen_mode {
            GenMode::FewToMany(..) => 500,
            GenMode::ManyToMany(..) => 10,
            GenMode::Duplicates => 10,
            GenMode::RandomPriorityFee(..) => 10,
            GenMode::HighCallData(..) => 10,
            GenMode::SelfDestructs => 10,
            GenMode::NonDeterministicStorage(..) => 10,
            GenMode::StorageDeletes(..) => 10,
            GenMode::NullGen => 0,
            GenMode::ECMul => 10,
            GenMode::Uniswap => 10,
            GenMode::ReserveBalance => 1,
            GenMode::ReserveBalanceFail(..) => 1,
            GenMode::SystemSpam(..) => 500,
            GenMode::SystemKeyNormal => 500,
            GenMode::SystemKeyNormalRandomPriorityFee => 500,
            GenMode::EIP7702Reuse(..) => 10,
            GenMode::EIP7702Create(..) => 10,
            GenMode::ExtremeValues(..) => 10,
        }
    }

    pub fn sender_group_size(&self) -> usize {
        if let Some(x) = self.sender_group_size {
            return x;
        }
        match &self.gen_mode {
            GenMode::FewToMany(..) => 100,
            GenMode::ManyToMany(..) => 100,
            GenMode::Duplicates => 100,
            GenMode::RandomPriorityFee(..) => 100,
            GenMode::NonDeterministicStorage(..) => 100,
            GenMode::StorageDeletes(..) => 100,
            GenMode::NullGen => 10,
            GenMode::SelfDestructs => 10,
            GenMode::HighCallData(..) => 10,
            GenMode::ECMul => 10,
            GenMode::Uniswap => 20,
            GenMode::ReserveBalance => 100,
            GenMode::ReserveBalanceFail(..) => 100,
            GenMode::SystemSpam(..) => 1,
            GenMode::SystemKeyNormal => 1,
            GenMode::SystemKeyNormalRandomPriorityFee => 1,
            GenMode::EIP7702Reuse(..) => 10,
            GenMode::EIP7702Create(..) => 10,
            GenMode::ExtremeValues(..) => 10,
        }
    }

    pub fn senders(&self) -> usize {
        if let Some(x) = self.senders {
            return x;
        }
        match &self.gen_mode {
            GenMode::FewToMany(..) => 1000,
            GenMode::ManyToMany(..) => 2500,
            GenMode::Duplicates => 2500,
            GenMode::RandomPriorityFee(..) => 2500,
            GenMode::NonDeterministicStorage(..) => 2500,
            GenMode::StorageDeletes(..) => 2500,
            GenMode::NullGen => 100,
            GenMode::SelfDestructs => 100,
            GenMode::HighCallData(..) => 100,
            GenMode::ECMul => 100,
            GenMode::Uniswap => 200,
            GenMode::ReserveBalance => 2500,
            GenMode::ReserveBalanceFail(..) => 2500,
            GenMode::SystemSpam(..) => 1,
            GenMode::SystemKeyNormal => 1,
            GenMode::SystemKeyNormalRandomPriorityFee => 1,
            GenMode::EIP7702Reuse(..) => 100,
            GenMode::EIP7702Create(..) => 100,
            GenMode::ExtremeValues(..) => 100,
        }
    }

    pub fn required_contract(&self) -> RequiredContract {
        use RequiredContract::*;
        match &self.gen_mode {
            GenMode::FewToMany(config) => match config.tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::ManyToMany(config) => match config.tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::Duplicates => None,
            GenMode::RandomPriorityFee(config) => match config.tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::HighCallData(..) => ERC20,
            GenMode::SelfDestructs => None,
            GenMode::NonDeterministicStorage(..) => ERC20,
            GenMode::StorageDeletes(..) => ERC20,
            GenMode::NullGen => None,
            GenMode::ECMul => ECMUL,
            GenMode::Uniswap => Uniswap,
            GenMode::ReserveBalance => None,
            GenMode::ReserveBalanceFail(..) => None,
            GenMode::SystemSpam(..) => None,
            GenMode::SystemKeyNormal => None,
            GenMode::SystemKeyNormalRandomPriorityFee => None,
            GenMode::EIP7702Reuse(..) => EIP7702,
            GenMode::EIP7702Create(..) => EIP7702,
            GenMode::ExtremeValues(..) => ERC20,
        }
    }

    pub fn erc20_contract_count(&self) -> usize {
        match &self.gen_mode {
            GenMode::FewToMany(config) => {
                if matches!(config.tx_type, TxType::ERC20) {
                    config.contract_count
                } else {
                    0
                }
            }
            GenMode::ManyToMany(config) => {
                if matches!(config.tx_type, TxType::ERC20) {
                    config.contract_count
                } else {
                    0
                }
            }
            GenMode::RandomPriorityFee(config) => {
                if matches!(config.tx_type, TxType::ERC20) {
                    config.contract_count
                } else {
                    0
                }
            }
            GenMode::HighCallData(config) => config.contract_count,
            GenMode::NonDeterministicStorage(config) => config.contract_count,
            GenMode::StorageDeletes(config) => config.contract_count,
            GenMode::ExtremeValues(config) => config.contract_count,
            _ => 0,
        }
    }
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

    pub fn to_file(&self, path: &str) -> Result<()> {
        let content =
            toml::to_string_pretty(self).wrap_err("Failed to serialize config to TOML")?;
        std::fs::write(path, content)
            .wrap_err_with(|| format!("Failed to write config to {:?}", path))
    }

    pub fn rpc_urls(&self) -> Result<Vec<Url>> {
        if self.rpc_urls.is_empty() {
            bail!("No RPC URLs provided");
        }

        self.rpc_urls
            .iter()
            .map(|url| {
                url.parse()
                    .wrap_err_with(|| format!("Failed to parse RPC URL: {}", url))
            })
            .collect()
    }

    pub fn ws_url(&self) -> Result<Url> {
        self.ws_url
            .parse()
            .wrap_err_with(|| format!("Failed to parse WS URL: {}", self.ws_url))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WorkloadGroup {
    /// How long to run this traffic pattern in seconds
    pub runtime_minutes: f64,
    pub name: String,
    pub traffic_gens: Vec<TrafficGen>,

    /// Approximately what percentage of transactions should be mutated.
    /// Txns are selected randomly for mutation, so actual percentage may vary,
    /// but should be close to the configured percentage. On average, each
    /// mutated txn will have one of its fields modified, but there may be more.
    pub mutation_percentage: f64,

    /// Spam rpc and websocket with wallet workflow requests and compare the responses
    pub spam_rpc_ws: bool,

    /// Compare block headers returned from rpc and websocket
    pub compare_rpc_ws: bool,

    /// Number of concurrent websocket connections to use for spamming rpc and websocket
    pub num_ws_connections: usize,
}

impl Default for WorkloadGroup {
    fn default() -> Self {
        Self {
            runtime_minutes: 10.0,
            name: "default".to_string(),
            traffic_gens: vec![],
            mutation_percentage: 0.0,
            spam_rpc_ws: false,
            compare_rpc_ws: false,
            num_ws_connections: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct TrafficGen {
    /// Target tps of the generator for this traffic phase
    pub tps: u64,

    /// Seed used to generate private keys for recipients
    pub recipient_seed: u64,

    /// Seed used to generate private keys for senders.
    /// If set the same as recipient seed, the accounts will be the same
    pub sender_seed: u64,

    /// Number of recipient accounts to generate and cycle between
    pub recipients: usize,

    /// Number of sender accounts to generate and cycle sending from
    pub senders: Option<usize>,

    /// Should the txgen query for erc20 balances
    /// This introduces many eth_calls which can affect performance and are not strictly needed for the gen to function
    pub erc20_balance_of: bool,

    /// Which generation mode to use. Corresponds to Generator impls
    pub gen_mode: GenMode,

    /// How many senders should be batched together when cycling between gen -> rpc sender -> refresher -> gen...
    pub sender_group_size: Option<usize>,

    /// How many txs should be generated per sender per cycle.
    /// Or put another way, how many txs should be generated before refreshing the nonce from chain state
    pub tx_per_sender: Option<usize>,
}

impl Default for TrafficGen {
    fn default() -> Self {
        Self {
            tps: 1000,
            recipient_seed: 10101,
            sender_seed: 10101,
            recipients: 100000,
            senders: None,
            erc20_balance_of: false,
            gen_mode: GenMode::FewToMany(FewToManyConfig {
                tx_type: TxType::ERC20,
                contract_count: 1,
            }),
            sender_group_size: None,
            tx_per_sender: None,
        }
    }
}

pub enum RequiredContract {
    None,
    ERC20,
    ECMUL,
    Uniswap,
    EIP7702,
}

#[derive(Debug, Clone)]
pub enum DeployedContract {
    None,
    ERC20(Vec<ERC20>),
    ECMUL(ECMul),
    Uniswap(Uniswap),
    EIP7702(EIP7702),
}

impl DeployedContract {
    pub fn erc20(self) -> Result<Vec<ERC20>> {
        match self {
            Self::ERC20(erc20s) => Ok(erc20s),
            _ => bail!("Expected erc20, found {:?}", &self),
        }
    }

    pub fn erc20_first(self) -> Result<ERC20> {
        match self {
            Self::ERC20(erc20s) => erc20s
                .first()
                .copied()
                .ok_or_else(|| eyre::eyre!("No ERC20 contracts available")),
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

    pub fn eip7702(self) -> Result<EIP7702> {
        match self {
            Self::EIP7702(batch_call) => Ok(batch_call),
            _ => bail!("Expected eip7702, found {:?}", &self),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GenMode {
    FewToMany(FewToManyConfig),
    ManyToMany(ManyToManyConfig),
    Duplicates,
    #[serde(rename = "eip7702_reuse")]
    EIP7702Reuse(EIP7702Config),
    #[serde(rename = "eip7702_create")]
    EIP7702Create(EIP7702CreateConfig),
    RandomPriorityFee(RandomPriorityFeeConfig),
    HighCallData(HighCallDataConfig),
    SelfDestructs,
    NonDeterministicStorage(NonDeterministicStorageConfig),
    StorageDeletes(StorageDeletesConfig),
    NullGen,
    #[serde(rename = "ecmul")]
    ECMul,
    #[serde(rename = "uniswap")]
    Uniswap,
    ReserveBalance,
    ReserveBalanceFail(ReserveBalanceFailConfig),
    SystemSpam(SystemSpamConfig),
    SystemKeyNormal,
    SystemKeyNormalRandomPriorityFee,
    ExtremeValues(ExtremeValuesConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FewToManyConfig {
    #[serde(default = "default_tx_type")]
    pub tx_type: TxType,
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for FewToManyConfig {
    fn default() -> Self {
        Self {
            tx_type: default_tx_type(),
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManyToManyConfig {
    #[serde(default = "default_tx_type")]
    pub tx_type: TxType,
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for ManyToManyConfig {
    fn default() -> Self {
        Self {
            tx_type: default_tx_type(),
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RandomPriorityFeeConfig {
    #[serde(default = "default_tx_type_native")]
    pub tx_type: TxType,
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for RandomPriorityFeeConfig {
    fn default() -> Self {
        Self {
            tx_type: default_tx_type_native(),
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HighCallDataConfig {
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for HighCallDataConfig {
    fn default() -> Self {
        Self {
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NonDeterministicStorageConfig {
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for NonDeterministicStorageConfig {
    fn default() -> Self {
        Self {
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageDeletesConfig {
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for StorageDeletesConfig {
    fn default() -> Self {
        Self {
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtremeValuesConfig {
    #[serde(default = "default_contract_count")]
    pub contract_count: usize,
}

impl Default for ExtremeValuesConfig {
    fn default() -> Self {
        Self {
            contract_count: default_contract_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemSpamConfig {
    pub call_type: SystemCallType,
}

const DEFAULT_TOTAL_AUTHORIZATIONS: usize = 5;
const DEFAULT_AUTHORIZATIONS_PER_TX: usize = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EIP7702Config {
    /// Number of authorizations to create upfront and reuse across transactions
    /// These authorizations will be used to execute code on behalf of authorized accounts
    pub total_authorizations: usize,

    /// Number of authorizations to include in each transaction's authorization list
    /// Each transaction will call the execute function to actually use these authorizations
    pub authorizations_per_tx: usize,
}

impl Default for EIP7702Config {
    fn default() -> Self {
        Self {
            total_authorizations: DEFAULT_TOTAL_AUTHORIZATIONS,
            authorizations_per_tx: DEFAULT_AUTHORIZATIONS_PER_TX,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EIP7702CreateConfig {
    /// Number of authorizations to create in each transaction
    /// These authorizations are created but not used to execute code
    pub authorizations_per_tx: usize,
}

impl Default for EIP7702CreateConfig {
    fn default() -> Self {
        Self {
            authorizations_per_tx: DEFAULT_AUTHORIZATIONS_PER_TX,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ReserveBalanceFailConfig {
    /// Number of failing transactions to generate per account
    pub num_fail_txs: usize,
}

impl Default for ReserveBalanceFailConfig {
    fn default() -> Self {
        Self { num_fail_txs: 5 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SystemCallType {
    Reward,
    Snapshot,
    EpochChange,
}

fn default_tx_type() -> TxType {
    TxType::ERC20
}

fn default_tx_type_native() -> TxType {
    TxType::Native
}

fn default_contract_count() -> usize {
    1
}

#[derive(Deserialize, Clone, Copy, Debug, Serialize, PartialEq, Eq, clap::ValueEnum)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_type_from_str() {
        assert_eq!(TxType::from_str("erc20").unwrap(), TxType::ERC20);
        assert_eq!(TxType::from_str("native").unwrap(), TxType::Native);
    }

    #[test]
    fn load_sample_configs() {
        let config =
            Config::from_file("examples/txgen/sample_configs/sequential_phases.json").unwrap();
        assert_eq!(config.rpc_urls.len(), 2);
        assert_eq!(config.rpc_urls[0], "http://localhost:33332");
        assert_eq!(config.rpc_urls[1], "http://localhost:8080");

        assert_eq!(config.workload_groups.len(), 3);
        assert_eq!(
            config.workload_groups[0].traffic_gens[0].gen_mode,
            GenMode::FewToMany(FewToManyConfig {
                tx_type: TxType::ERC20,
                contract_count: 1,
            })
        );
        assert_eq!(
            config.workload_groups[1].traffic_gens[0].gen_mode,
            GenMode::NonDeterministicStorage(NonDeterministicStorageConfig::default())
        );
        assert_eq!(
            config.workload_groups[2].traffic_gens[0].gen_mode,
            GenMode::Duplicates
        );

        // Check that the toml config parses
        let content =
            std::fs::read_to_string("examples/txgen/sample_configs/sequential_phases.toml")
                .unwrap();
        let toml_config: Config = toml::from_str(&content).unwrap();

        // Check that the toml config matches the json config
        // We do this per workload group since one large assert is hard to debug if it fails
        for idx in 0..3 {
            assert_eq!(
                toml_config.workload_groups[idx].traffic_gens[0].gen_mode,
                config.workload_groups[idx].traffic_gens[0].gen_mode
            );

            assert_eq!(
                toml_config.workload_groups[idx],
                config.workload_groups[idx]
            );
        }

        assert_eq!(toml_config, config);
    }
}
