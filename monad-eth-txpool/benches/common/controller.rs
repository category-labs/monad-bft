use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::{Uint, B256};
use alloy_rlp::Encodable;
use itertools::Itertools;
use monad_crypto::NopSignature;
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx};
use monad_eth_txpool::{
    EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics, EthTxPoolSnapshotManager,
};
use monad_eth_types::{Balance, BASE_FEE_PER_GAS};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Round, SeqNum};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const TRANSACTION_SIZE_BYTES: usize = 400;

pub type SignatureType = NopSignature;
pub type SignatureCollectionType = MockSignatures<NopSignature>;
pub type BlockPolicyType = EthBlockPolicy<SignatureType, SignatureCollectionType>;
pub type StateBackendType = InMemoryState;
pub type Pool = EthTxPool<SignatureType, SignatureCollectionType, StateBackendType>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BenchControllerConfig {
    pub accounts: usize,
    pub txs: usize,
    pub nonce_var: usize,
    pub pending_blocks: usize,
    pub proposal_tx_limit: usize,
}

pub struct BenchController<'a> {
    pub block_policy: &'a BlockPolicyType,
    pub state_backend: StateBackendType,
    pub pool: Pool,
    pub pending_blocks: Vec<EthValidatedBlock<SignatureType, SignatureCollectionType>>,
    pub metrics: EthTxPoolMetrics,
    pub snapshot_manager: EthTxPoolSnapshotManager,
    pub proposal_tx_limit: usize,
    pub proposal_gas_limit: u64,
    pub proposal_byte_limit: u64,
}

impl<'a> BenchController<'a> {
    pub fn setup(block_policy: &'a BlockPolicyType, config: BenchControllerConfig) -> Self {
        let BenchControllerConfig {
            accounts,
            txs,
            nonce_var,
            pending_blocks,
            proposal_tx_limit,
        } = config;

        let (pending_block_txs, txs) = Self::generate_txs(accounts, txs, nonce_var, pending_blocks);

        let state_backend = Self::generate_state_backend_for_txs(&txs);

        let mut metrics = EthTxPoolMetrics::default();
        let mut snapshot_manager = EthTxPoolSnapshotManager::default();
        let mut pool = Self::create_pool(block_policy, &state_backend, &txs, &mut metrics);

        pool.update_committed_block(
            &mut EthTxPoolEventTracker::new(
                &mut metrics,
                &mut snapshot_manager,
                &mut Vec::default(),
            ),
            generate_block_with_txs(Round(0), block_policy.get_last_commit(), Vec::default()),
        );

        Self {
            block_policy,
            state_backend,
            pool,
            pending_blocks: pending_block_txs
                .into_iter()
                .enumerate()
                .map(|(idx, txs)| {
                    generate_block_with_txs(Round(idx as u64 + 1), SeqNum(idx as u64 + 1), txs)
                })
                .collect_vec(),
            metrics,
            snapshot_manager,
            proposal_tx_limit,
            proposal_gas_limit: txs
                .iter()
                .map(|tx| tx.gas_limit())
                .sum::<u64>()
                .checked_add(1)
                .expect("proposal gas limit does not overflow"),
            proposal_byte_limit: txs
                .iter()
                .map(|tx| tx.length() as u64)
                .sum::<u64>()
                .checked_add(1)
                .expect("proposal size limit does not overflow"),
        }
    }

    pub fn create_pool(
        block_policy: &BlockPolicyType,
        state_backend: &StateBackendType,
        txs: &[Recovered<TxEnvelope>],
        metrics: &mut EthTxPoolMetrics,
    ) -> Pool {
        let mut pool = Pool::default_testing();

        let mut inserted = false;

        pool.insert_txs(
            &mut EthTxPoolEventTracker::new(
                metrics,
                &mut EthTxPoolSnapshotManager::default(),
                &mut Vec::default(),
            ),
            block_policy,
            state_backend,
            txs.to_vec(),
            true,
            |_| inserted = true,
        )
        .unwrap();

        assert!(inserted);

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

                        let signer = tx.recover_signer().unwrap();

                        *nonce += 1;

                        Recovered::new_unchecked(tx, signer)
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

                let signer = tx.recover_signer().unwrap();

                Recovered::new_unchecked(tx, signer)
            })
            .collect_vec();

        txs.shuffle(&mut rng);

        (pending_block_txs, txs)
    }
}
