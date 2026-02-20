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

use alloy_consensus::{
    transaction::{Recovered, SignerRecoverable},
    Transaction, TxEnvelope,
};
use alloy_primitives::{Uint, B256};
use alloy_rlp::Encodable;
use itertools::Itertools;
use monad_chain_config::{revision::MockChainRevision, ChainConfig, MockChainConfig};
use monad_crypto::NopSignature;
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx, recover_tx};
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Balance, Round, SeqNum};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const BASE_FEE_PER_GAS: u64 = 100_000_000_000;
const TRANSACTION_SIZE_BYTES: usize = 400;

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
    pub accounts: usize,
    pub txs: usize,
    pub nonce_var: usize,
    pub pending_blocks: usize,
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
    pub fn setup(block_policy: &'a BlockPolicyType, config: BenchControllerConfig) -> Self {
        let BenchControllerConfig {
            chain_config,
            accounts,
            txs,
            nonce_var,
            pending_blocks,
            proposal_tx_limit,
        } = config;

        let (pending_block_txs, txs) = Self::generate_txs(accounts, txs, nonce_var, pending_blocks);

        let proposal_gas_limit = txs
            .iter()
            .map(|tx| tx.gas_limit())
            .sum::<u64>()
            .saturating_add(1);
        let proposal_byte_limit = txs
            .iter()
            .map(|tx| tx.length() as u64)
            .sum::<u64>()
            .saturating_add(1);

        let state_backend = Self::generate_state_backend_for_txs(&txs);

        let metrics = EthTxPoolMetrics::default();
        let pool = Self::create_pool(block_policy, &chain_config, txs, &metrics);

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
                        BASE_FEE_PER_GAS,
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
                BASE_FEE_PER_GAS,
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

    pub fn generate_txs(
        accounts: usize,
        txs: usize,
        nonce_var: usize,
        pending_blocks: usize,
    ) -> (Vec<Vec<Recovered<TxEnvelope>>>, Vec<Recovered<TxEnvelope>>) {
        let mut rng = ChaCha8Rng::seed_from_u64(0);

        let mut accounts = (0..accounts)
            .map(|_| (B256::from(Uint::from(rng.gen::<u64>())), 0u64))
            .collect_vec();

        let pending_block_txs = (0..pending_blocks)
            .map(|pending_block| {
                let mut txs = (txs * pending_block..txs * (pending_block + 1))
                    .map(|idx| {
                        let accounts_len = accounts.len();

                        let (account, nonce) = accounts
                            .get_mut(idx % accounts_len)
                            .expect("account idx is in range");

                        let tx = make_legacy_tx(
                            *account,
                            rng.gen_range(BASE_FEE_PER_GAS..=BASE_FEE_PER_GAS + 10000)
                                .into(),
                            30000,
                            *nonce,
                            TRANSACTION_SIZE_BYTES,
                        );

                        *nonce += 1;

                        recover_tx(tx)
                    })
                    .collect_vec();

                txs.shuffle(&mut rng);

                txs
            })
            .collect_vec();

        let mut txs = (0..txs)
            .map(|idx| {
                let (account, nonce) = accounts
                    .get(idx % accounts.len())
                    .expect("account idx is in range");

                let tx = make_legacy_tx(
                    *account,
                    rng.gen_range(BASE_FEE_PER_GAS..=BASE_FEE_PER_GAS + 10000)
                        .into(),
                    30000,
                    nonce
                        .checked_add(rng.gen_range(0..=nonce_var as u64))
                        .expect("nonce does not overflow"),
                    TRANSACTION_SIZE_BYTES,
                );

                recover_tx(tx)
            })
            .collect_vec();

        txs.shuffle(&mut rng);

        (pending_block_txs, txs)
    }
}
