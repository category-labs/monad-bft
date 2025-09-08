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

use clap::{Parser, Subcommand, ValueEnum};
use eyre::bail;
use serde::Deserialize;
use url::Url;

use crate::{
    prelude::*,
    shared::{ecmul::ECMul, erc20::ERC20, uniswap::Uniswap},
};

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Config {
    #[arg(long, global = true, default_value = "http://localhost:8545")]
    pub rpc_url: Url,

    /// Target tps of the generator
    #[arg(long, global = true, default_value = "1000")]
    pub tps: u64,

    /// Funded private keys used to seed native tokens to sender accounts
    #[arg(
        long,
        global = true,
        default_values_t = [
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d".to_string(),
            "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a".to_string(),
            "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6".to_string(),
            "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a".to_string(),
            "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba".to_string(),
        ]
    )]
    pub root_private_keys: Vec<String>,

    /// Seed used to generate private keys for recipients
    #[arg(long, global = true, default_value = "10101")]
    pub recipient_seed: u64,

    /// Seed used to generate private keys for senders.
    /// If set the same as recipient seed, the accounts will be the same
    #[arg(long, global = true, default_value = "10101")]
    pub sender_seed: u64,

    /// Number of recipient accounts to generate and cycle between
    #[arg(long, global = true, default_value = "100000")]
    pub recipients: usize,

    /// Number of sender accounts to generate and cycle sending from
    #[arg(long, global = true)]
    pub senders: Option<usize>,

    /// How long to wait before refreshing balances. A function of the execution delay and block speed
    #[arg(long, global = true, default_value = "5.")]
    pub refresh_delay_secs: f64,

    /// Should the txgen query for erc20 balances
    /// This introduces many eth_calls which can affect performance and are not strictly needed for the gen to function
    #[arg(long, global = true, default_value = "false")]
    pub erc20_balance_of: bool,

    /// Which generation mode to use. Corresponds to Generator impls
    #[command(subcommand)]
    pub gen_mode: GenMode,

    /// How many senders should be batched together when cycling between gen -> rpc sender -> refresher -> gen...
    #[arg(long, global = true)]
    pub sender_group_size: Option<usize>,

    /// How many txs should be generated per sender per cycle.
    /// Or put another way, how many txs should be generated before refreshing the nonce from chain state
    #[arg(long, global = true)]
    pub tx_per_sender: Option<usize>,

    /// Override for erc20 contract address
    #[arg(long, global = true)]
    pub erc20_contract: Option<String>,

    /// Override for ecmul contract address.
    #[arg(long, global = true)]
    pub ecmul_contract: Option<String>,

    /// Override for uniswap contract address
    #[arg(long, global = true)]
    pub uniswap_contract: Option<String>,

    /// Queries rpc for receipts of each sent tx when set. Queries per txhash, prefer `use_receipts_by_block` for efficiency
    #[arg(long, global = true, default_value = "false")]
    pub use_receipts: bool,

    /// Queries rpc for receipts for each committed block and filters against txs sent by this txgen.
    /// More efficient
    #[arg(long, global = true, default_value = "false")]
    pub use_receipts_by_block: bool,

    /// Fetches logs for each tx sent
    #[arg(long, global = true, default_value = "false")]
    pub use_get_logs: bool,

    /// Base fee used when calculating gas costs and value
    #[arg(long, global = true, default_value_t = 50)]
    pub base_fee_gwei: u128,

    /// Chain id
    #[arg(long, global = true, default_value_t = 20143)]
    pub chain_id: u64,

    /// Minimum native amount in wei for each sender.
    /// When a sender has less than this amount, it's native balance is topped off from a root private key
    #[arg(long, global = true, default_value_t = 100_000_000_000_000_000_000)]
    pub min_native_amount: u128,

    /// Native amount in wei transfered to each sender from an available root private key when the sender's
    /// native balance passes below `min_native_amount`
    #[arg(long, global = true, default_value_t = 1000_000_000_000_000_000_000)]
    pub seed_native_amount: u128,

    /// Writes `DEBUG` logs to ./debug.log
    #[arg(long, global = true, default_value_t = false)]
    pub debug_log_file: bool,

    /// Writes `TRACE` logs to ./trace.log
    #[arg(long, global = true, default_value_t = false)]
    pub trace_log_file: bool,

    #[arg(long, global = true)]
    pub use_static_tps_interval: bool,

    /// Otel endpoint
    #[arg(long, global = true)]
    pub otel_endpoint: Option<String>,

    /// Otel replica name
    #[arg(long, global = true, default_value = "default")]
    pub otel_replica_name: String,
}

impl Config {
    pub fn tx_per_sender(&self) -> usize {
        use GenMode::*;
        if let Some(x) = self.tx_per_sender {
            return x;
        }
        match self.gen_mode {
            FewToMany { .. } => 500,
            ManyToMany { .. }
            | Duplicates
            | RandomPriorityFee
            | HighCallData
            | SelfDestructs
            | NonDeterministicStorage
            | StorageDeletes
            | ECMul => 10,
            NullGen => 0,
            Uniswap => 10,
            HighCallDataLowGasLimit => 30,
        }
    }

    pub fn sender_group_size(&self) -> usize {
        use GenMode::*;
        if let Some(x) = self.sender_group_size {
            return x;
        }
        match self.gen_mode {
            FewToMany { .. } => 100,
            ManyToMany { .. }
            | Duplicates
            | RandomPriorityFee
            | NonDeterministicStorage
            | StorageDeletes => 100,
            NullGen | SelfDestructs | HighCallData | ECMul => 10,
            HighCallDataLowGasLimit => 3,
            Uniswap => 20,
        }
    }

    pub fn senders(&self) -> usize {
        use GenMode::*;
        if let Some(x) = self.senders {
            return x;
        }
        match self.gen_mode {
            FewToMany { .. } => 1000,
            ManyToMany { .. }
            | Duplicates
            | RandomPriorityFee
            | NonDeterministicStorage
            | StorageDeletes => 2500,
            NullGen => 100,
            SelfDestructs | HighCallData | HighCallDataLowGasLimit | ECMul => 100,
            Uniswap => 200,
        }
    }

    pub fn required_contract(&self) -> RequiredContract {
        use RequiredContract::*;
        match self.gen_mode {
            GenMode::FewToMany { tx_type } => match tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::ManyToMany { tx_type } => match tx_type {
                TxType::ERC20 => ERC20,
                TxType::Native => None,
            },
            GenMode::Duplicates => ERC20,
            GenMode::RandomPriorityFee => ERC20,
            GenMode::HighCallData => None,
            GenMode::HighCallDataLowGasLimit => None,
            GenMode::SelfDestructs => None,
            GenMode::NonDeterministicStorage => ERC20,
            GenMode::StorageDeletes => ERC20,
            GenMode::NullGen => None,
            GenMode::ECMul => ECMUL,
            GenMode::Uniswap => Uniswap,
        }
    }

    pub fn base_fee(&self) -> u128 {
        self.base_fee_gwei
            .checked_mul(10u128.pow(9))
            .expect("Gwei must be convertable to wei using u128")
    }
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

#[derive(Debug, Subcommand)]
pub enum GenMode {
    FewToMany {
        #[arg(long, default_value = "erc20")]
        tx_type: TxType,
    },
    ManyToMany {
        #[arg(long, default_value = "erc20")]
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

#[derive(Deserialize, Clone, Copy, Debug, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TxType {
    ERC20,
    Native,
}
