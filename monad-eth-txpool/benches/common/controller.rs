use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::txpool::TxPool;
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::make_tx;
use monad_eth_tx::EthSignedTransaction;
use monad_eth_txpool::EthTxPool;
use monad_eth_types::{Balance, EthAddress};
use monad_multi_sig::MultiSig;
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_types::SeqNum;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use reth_primitives::B256;

const TRANSACTION_SIZE_BYTES: usize = 400;

pub type SignatureCollectionType = MultiSig<NopSignature>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BenchControllerConfig {
    pub accounts: usize,
    pub txs: usize,
    pub tx_limit: usize,
    pub max_nonce: u64,
}

pub struct BenchController<'a> {
    pub block_policy: &'a EthBlockPolicy,
    pub state_backend: InMemoryState,
    pub pool: EthTxPool,
    pub tx_limit: usize,
    pub gas_limit: u64,
}

impl<'a> BenchController<'a> {
    pub fn setup(block_policy: &'a EthBlockPolicy, config: BenchControllerConfig) -> Self {
        let mut pool = EthTxPool::default();

        let BenchControllerConfig {
            accounts,
            txs,
            tx_limit,
            max_nonce,
        } = config;

        let txs = Self::make_txs(accounts, txs, max_nonce);

        let state_backend = InMemoryStateInner::new(
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
        );

        assert!(
            !TxPool::<SignatureCollectionType, EthBlockPolicy, InMemoryState>::insert_tx(
                &mut pool,
                txs.iter()
                    .map(|t| Bytes::from(t.envelope_encoded()))
                    .collect(),
                block_policy,
                &state_backend,
            )
            .is_empty()
        );

        Self {
            state_backend,
            block_policy,
            pool,
            tx_limit,
            gas_limit: txs
                .iter()
                .map(|txn| txn.transaction.gas_limit())
                .sum::<u64>()
                .checked_add(1)
                .expect("proposal gas limit does not overflow"),
        }
    }

    fn make_txs(accounts: usize, txs: usize, max_nonce: u64) -> Vec<EthSignedTransaction> {
        let mut rng = ChaCha8Rng::seed_from_u64(0);

        let accounts = (0..accounts)
            .map(|_| B256::random_with(&mut rng))
            .collect_vec();

        let mut txs = (0..txs)
            .map(|idx| {
                let account = accounts
                    .get(idx % accounts.len())
                    .expect("account idx is in range");

                make_tx(
                    *account,
                    rng.gen_range(1000..=10_000),
                    30000,
                    rng.gen_range(0..=max_nonce),
                    TRANSACTION_SIZE_BYTES,
                )
            })
            .collect_vec();

        txs.shuffle(&mut rng);

        txs
    }
}
