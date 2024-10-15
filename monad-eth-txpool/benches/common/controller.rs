use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::txpool::TxPool;
use monad_crypto::NopSignature;
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_testutil::{generate_block_with_txs, make_tx};
use monad_eth_tx::EthSignedTransaction;
use monad_eth_txpool::EthTxPool;
use monad_eth_types::{Balance, EthAddress};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Round, SeqNum};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use reth_primitives::B256;

const TRANSACTION_SIZE_BYTES: usize = 400;

pub type SignatureCollectionType = MockSignatures<NopSignature>;
pub type BlockPolicyType = EthBlockPolicy;
pub type StateBackendType = InMemoryState;
pub type Pool = EthTxPool<SignatureCollectionType, StateBackendType>;

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
    pub pending_blocks: Vec<EthValidatedBlock<SignatureCollectionType>>,
    pub proposal_tx_limit: usize,
    pub gas_limit: u64,
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

        let mut pool = Self::create_pool(block_policy, &state_backend, &txs);

        pool.update_committed_block(&generate_block_with_txs(
            Round(0),
            block_policy.get_last_commit(),
            Vec::default(),
        ));

        Self {
            block_policy,
            state_backend,
            pool,
            pending_blocks: pending_block_txs
                .into_iter()
                .enumerate()
                .map(|(idx, tx)| {
                    generate_block_with_txs(Round(idx as u64 + 1), SeqNum(idx as u64 + 1), tx)
                })
                .collect_vec(),
            proposal_tx_limit,
            gas_limit: txs
                .iter()
                .map(|tx| tx.transaction.gas_limit())
                .sum::<u64>()
                .checked_add(1)
                .expect("proposal gas limit does not overflow"),
        }
    }

    pub fn create_pool(
        block_policy: &BlockPolicyType,
        state_backend: &StateBackendType,
        txs: &[EthSignedTransaction],
    ) -> Pool {
        let mut pool = Pool::new(true);

        assert!(!Pool::insert_tx(
            &mut pool,
            txs.iter()
                .map(|t| Bytes::from(t.envelope_encoded()))
                .collect(),
            block_policy,
            state_backend,
        )
        .is_empty());

        pool
    }

    pub fn generate_state_backend_for_txs(txs: &[EthSignedTransaction]) -> StateBackendType {
        InMemoryStateInner::new(
            Balance::MAX,
            SeqNum(4),
            InMemoryBlockState::genesis(
                txs.iter()
                    .map(|tx| {
                        (
                            EthAddress(tx.recover_signer().expect("signer is recoverable")),
                            0,
                        )
                    })
                    .collect(),
            ),
        )
    }

    pub fn generate_txs(
        accounts: usize,
        txs: usize,
        nonce_var: usize,
        pending_blocks: usize,
    ) -> (Vec<Vec<EthSignedTransaction>>, Vec<EthSignedTransaction>) {
        let mut rng = ChaCha8Rng::seed_from_u64(0);

        let mut accounts = (0..accounts)
            .map(|_| (B256::random_with(&mut rng), 0u64))
            .collect_vec();

        let pending_block_txs = (0..pending_blocks)
            .map(|pending_block| {
                let mut txs = (txs * pending_block..txs * (pending_block + 1))
                    .map(|idx| {
                        let accounts_len = accounts.len();

                        let (account, nonce) = accounts
                            .get_mut(idx % accounts_len)
                            .expect("account idx is in range");

                        let tx = make_tx(
                            *account,
                            rng.gen_range(1000..=10_000),
                            30000,
                            *nonce,
                            TRANSACTION_SIZE_BYTES,
                        );

                        *nonce += 1;

                        tx
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

                make_tx(
                    *account,
                    rng.gen_range(1000..=10_000),
                    30000,
                    nonce
                        .checked_add(rng.gen_range(0..=nonce_var as u64))
                        .expect("nonce does not overflow"),
                    TRANSACTION_SIZE_BYTES,
                )
            })
            .collect_vec();

        txs.shuffle(&mut rng);

        (pending_block_txs, txs)
    }
}
