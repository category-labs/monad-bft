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

use std::path::PathBuf;

use alloy_primitives::Address;
use clap::{Parser, ValueEnum};
use monad_chain_config::{MONAD_DEVNET_CHAIN_ID, MONAD_MAINNET_CHAIN_ID, MONAD_TESTNET_CHAIN_ID};

#[derive(Debug, Parser)]
#[command(name = "monad-rpc-domain", about, long_about = None)]
pub struct Cli {
    /// Set the L1 chain of the private domain
    #[arg(long)]
    pub chain: L1Chain,

    /// Set the chain id of the private domain
    #[arg(long)]
    pub domain_chain_id: u64,

    /// Set the path of the triedb that `monad-domain` writes
    #[arg(long)]
    pub triedb_path: PathBuf,

    /// Set the accounts that operate the private domain, as a comma-separated list of
    /// addresses. Operators are served every method
    #[arg(long, value_delimiter = ',')]
    pub operators: Vec<Address>,

    /// Set the address for RPC to bind to
    #[arg(long, default_value_t = String::from("127.0.0.1"))]
    pub rpc_addr: String,

    /// Set the port number for RPC to listen
    #[arg(long, default_value_t = 8080)]
    pub rpc_port: u16,

    /// Set the number of worker threads for the RPC server
    #[arg(long, default_value_t = 2)]
    pub worker_threads: usize,

    /// Set the max number of requests in a batch request
    #[arg(long, default_value_t = 1000)]
    pub batch_request_limit: u16,

    /// Set the max number of requests that can execute concurrently in a batch request
    #[arg(long, default_value_t = 32)]
    pub batch_concurrent_limit: u16,

    /// Set the max request size in bytes
    #[arg(long, default_value_t = 2_000_000)]
    pub max_request_size: usize,

    /// Set the max response size in bytes
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Set the max block range for eth_getLogs
    #[arg(long, default_value_t = 1000)]
    pub eth_get_logs_max_block_range: u64,

    /// Set the max concurrent requests for eth_feeHistory
    #[arg(long, default_value_t = 100)]
    pub feehistory_max_concurrent_requests: usize,

    /// Set the max concurrent requests for eth_call and eth_estimateGas
    #[arg(long, default_value_t = 1000)]
    pub eth_call_max_concurrent_requests: u32,

    /// Set the number of threads used for executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 2)]
    pub eth_call_executor_threads: u32,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 64)]
    pub eth_call_executor_fibers: u32,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 2)]
    pub eth_call_executor_queuing_timeout: u32,

    /// Set the max concurrent requests for eth_call and eth_estimateGas with high gas cost
    #[arg(long, default_value_t = 20)]
    pub eth_call_high_max_concurrent_requests: u32,

    /// Set the number of threads used for executing eth_call and eth_estimateGas with high gas cost
    #[arg(long, default_value_t = 1)]
    pub eth_call_high_executor_threads: u32,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas with high gas cost
    #[arg(long, default_value_t = 2)]
    pub eth_call_high_executor_fibers: u32,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call with high gas cost
    #[arg(long, default_value_t = 30)]
    pub eth_call_high_executor_queuing_timeout: u32,

    /// Set the max concurrent requests for block tracing methods
    #[arg(long, default_value_t = 20)]
    pub eth_trace_block_max_concurrent_requests: u32,

    /// Set the number of threads used for trace operations
    #[arg(long, default_value_t = 1)]
    pub eth_trace_block_executor_threads: u32,

    /// Set the number of fibers used for executing block tracing methods
    #[arg(long, default_value_t = 2)]
    pub eth_trace_block_executor_fibers: u32,

    /// Set the number of fibers used for executing transactions within trace blocks
    #[arg(long, default_value_t = 100)]
    pub eth_trace_tx_executor_fibers: u32,

    /// Set the maximum timeout (in seconds) for queuing when executing block tracing methods
    #[arg(long, default_value_t = 30)]
    pub eth_trace_block_executor_queuing_timeout: u32,

    /// Set the memory limit of the node cache when executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 100 << 20)]
    pub eth_call_executor_node_lru_max_mem: u64,

    /// Set the gas limit for eth_call
    #[arg(long, default_value_t = 30_000_000)]
    pub eth_call_provider_gas_limit: u64,

    /// Set the gas limit for eth_estimateGas
    #[arg(long, default_value_t = 30_000_000)]
    pub eth_estimate_gas_provider_gas_limit: u64,

    /// Set the memory limit of the node cache for RPC requests other than eth_call and eth_estimateGas
    #[arg(long, default_value_t = 100 << 20)]
    pub triedb_node_lru_max_mem: u64,

    /// Set the max concurrent requests for triedb reads
    #[arg(long, default_value_t = 20_000)]
    pub triedb_max_buffered_read_requests: usize,

    /// Set the max number of concurrently executing async triedb read requests before we
    /// start exerting backpressure
    #[arg(long, default_value_t = 10_000)]
    pub triedb_max_async_read_concurrency: usize,

    /// Set the max concurrent requests for triedb traversals
    #[arg(long, default_value_t = 40)]
    pub triedb_max_buffered_traverse_requests: usize,

    /// Set the max number of concurrently executing async triedb traverse requests before we
    /// start exerting backpressure
    #[arg(long, default_value_t = 20)]
    pub triedb_max_async_traverse_concurrency: usize,

    /// Set the maximum number of finalized blocks in cache
    #[arg(long, default_value_t = 200)]
    pub max_finalized_block_cache_len: usize,

    /// Set the maximum number of voted blocks in cache
    #[arg(long, default_value_t = 3)]
    pub max_voted_block_cache_len: usize,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum L1Chain {
    #[value(name = "monad_devnet")]
    Devnet,
    #[value(name = "monad_testnet")]
    Testnet,
    #[value(name = "monad_mainnet")]
    Mainnet,
}

impl L1Chain {
    pub fn chain_id(self) -> u64 {
        match self {
            Self::Devnet => MONAD_DEVNET_CHAIN_ID,
            Self::Testnet => MONAD_TESTNET_CHAIN_ID,
            Self::Mainnet => MONAD_MAINNET_CHAIN_ID,
        }
    }
}

/// Whether `domain_chain_id` is a private domain of the L1 `l1_chain_id`: the L1's chain id
/// in its low 16 bits and a nonzero domain number above them.
pub fn is_domain_of(domain_chain_id: u64, l1_chain_id: u64) -> bool {
    const L1_CHAIN_ID_BITS: u32 = 16;
    (domain_chain_id >> L1_CHAIN_ID_BITS) != 0
        && (domain_chain_id & ((1 << L1_CHAIN_ID_BITS) - 1)) == l1_chain_id
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;
    use clap::Parser;
    use monad_chain_config::{
        MONAD_DEVNET_CHAIN_ID, MONAD_MAINNET_CHAIN_ID, MONAD_TESTNET_CHAIN_ID,
    };

    use super::{is_domain_of, Cli};

    #[test]
    fn operators_are_a_comma_separated_list() {
        let parse = |operators: &[&str]| {
            let required = [
                "monad-rpc-domain",
                "--chain=monad_devnet",
                "--domain-chain-id=85679",
                "--triedb-path=triedb",
            ];
            Cli::try_parse_from(required.iter().chain(operators)).map(|cli| cli.operators)
        };
        let first = Address::repeat_byte(0x11);
        let second = Address::repeat_byte(0x22);

        assert!(parse(&[]).unwrap().is_empty());
        assert_eq!(
            parse(&["--operators", &format!("{first},{second}")]).unwrap(),
            [first, second]
        );
        assert!(parse(&["--operators", &format!("{first},0x1234")]).is_err());
    }

    #[test]
    fn domain_chain_id_ends_with_its_l1() {
        assert!(is_domain_of(
            (1 << 16) | MONAD_DEVNET_CHAIN_ID,
            MONAD_DEVNET_CHAIN_ID
        ));
        assert!(is_domain_of(
            (u64::MAX << 16) | MONAD_MAINNET_CHAIN_ID,
            MONAD_MAINNET_CHAIN_ID
        ));

        assert!(!is_domain_of(0, MONAD_DEVNET_CHAIN_ID));
        assert!(!is_domain_of(MONAD_DEVNET_CHAIN_ID, MONAD_DEVNET_CHAIN_ID));
        assert!(!is_domain_of(
            (1 << 16) | MONAD_TESTNET_CHAIN_ID,
            MONAD_DEVNET_CHAIN_ID
        ));
        // 120143 ends with 20143 in decimal, not in its low 16 bits.
        assert!(!is_domain_of(120_143, MONAD_DEVNET_CHAIN_ID));
    }
}
