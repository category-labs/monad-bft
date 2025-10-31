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

use std::collections::BTreeMap;

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_rlp::Encodable;
use itertools::Itertools;
use monad_chain_config::{revision::MockChainRevision, ChainConfig, MockChainConfig};
use monad_crypto::NopSignature;
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_testutil::generate_block_with_txs;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Balance, Round, SeqNum};

pub type SignatureType = NopSignature;
pub type SignatureCollectionType = MockSignatures<NopSignature>;
pub type BlockPolicyType =
    EthBlockPolicy<SignatureType, SignatureCollectionType, MockChainConfig, MockChainRevision>;
pub type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;
pub type Pool = EthTxPool<
    SignatureType,
    SignatureCollectionType,
    StateBackendType,
    MockChainConfig,
    MockChainRevision,
>;

#[derive(Clone, PartialEq, Eq)]
pub struct BenchControllerConfig {
    pub chain_config: MockChainConfig,
    pub proposal_tx_limit: usize,
}

pub struct BenchController<'a> {
    pub chain_config: MockChainConfig,
    pub block_policy: &'a BlockPolicyType,
    pub state_backend: StateBackendType,
    pub pool: Pool,
    pub pending_blocks: Vec<EthValidatedBlock<SignatureType, SignatureCollectionType>>,
    pub metrics: EthTxPoolMetrics,
    pub proposal_tx_limit: usize,
    pub proposal_gas_limit: u64,
    pub proposal_byte_limit: u64,
}

impl<'a> BenchController<'a> {
    pub fn setup(
        block_policy: &'a BlockPolicyType,
        config: BenchControllerConfig,
        pending_block_txs: Vec<Vec<Recovered<TxEnvelope>>>,
        pool_txs: Vec<Recovered<TxEnvelope>>,
    ) -> Self {
        let BenchControllerConfig {
            chain_config,
            proposal_tx_limit,
        } = config;

        let proposal_gas_limit = pool_txs
            .iter()
            .map(|tx| tx.gas_limit())
            .sum::<u64>()
            .saturating_add(1);
        let proposal_byte_limit = pool_txs
            .iter()
            .map(|tx| tx.length() as u64)
            .sum::<u64>()
            .saturating_add(1);

        let state_backend = Self::generate_state_backend_for_txs(&pool_txs);

        let metrics = EthTxPoolMetrics::default();
        let pool = Self::create_pool(block_policy, &chain_config, pool_txs, &metrics);

        Self {
            chain_config,
            block_policy,
            state_backend,
            pool,
            pending_blocks: pending_block_txs
                .into_iter()
                .enumerate()
                .map(|(idx, txs)| {
                    generate_block_with_txs(
                        Round(idx as u64 + 1),
                        SeqNum(idx as u64 + 1),
                        MIN_BASE_FEE,
                        &MockChainConfig::DEFAULT,
                        txs,
                    )
                })
                .collect_vec(),
            metrics,
            proposal_tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
        }
    }

    pub fn create_pool(
        block_policy: &BlockPolicyType,
        chain_config: &impl ChainConfig<MockChainRevision>,
        txs: Vec<Recovered<TxEnvelope>>,
        metrics: &EthTxPoolMetrics,
    ) -> Pool {
        let mut pool = Pool::default_testing();

        pool.update_committed_block(
            &mut EthTxPoolEventTracker::new(metrics, &mut BTreeMap::default()),
            chain_config,
            generate_block_with_txs(
                Round(0),
                block_policy.get_last_commit(),
                MIN_BASE_FEE,
                &MockChainConfig::DEFAULT,
                txs,
            ),
        );

        pool
    }

    pub fn generate_state_backend_for_txs(txs: &[Recovered<TxEnvelope>]) -> StateBackendType {
        InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(
                txs.iter()
                    .map(|tx| (tx.recover_signer().expect("signer is recoverable"), 0))
                    .collect(),
            ),
        )
    }
}
