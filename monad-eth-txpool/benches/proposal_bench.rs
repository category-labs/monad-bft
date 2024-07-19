use alloy_rlp::Encodable;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_consensus_types::{payload::FullTransactionList, txpool::TxPool};
use monad_crypto::NopSignature;
use monad_eth_block_policy::{nonce::InMemoryState, EthBlockPolicy};
use monad_eth_reserve_balance::{PassthruReserveBalanceCache, ReserveBalanceCacheTrait};
use monad_eth_txpool::EthTxPool;
use monad_eth_types::{Balance, EthAddress};
use monad_multi_sig::MultiSig;
use monad_perf_util::PerfController;
use monad_types::{SeqNum, GENESIS_SEQ_NUM};
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use reth_primitives::{
    sign_message, Address, Transaction, TransactionKind, TransactionSigned, TxLegacy, B256,
};

const NUM_TRANSACTIONS: usize = 10_000;
const TRANSACTION_SIZE_BYTES: usize = 400;
const EXECUTION_DELAY: u64 = 4;

fn make_tx(rng: &mut ChaCha8Rng, input_len: usize) -> TransactionSigned {
    let mut input = vec![0; input_len];
    rng.fill_bytes(&mut input);
    let transaction = Transaction::Legacy(TxLegacy {
        chain_id: Some(1337),
        nonce: rng.gen_range(10_000..50_000),
        gas_price: rng.gen_range(1..10_000),
        gas_limit: 6400,
        to: TransactionKind::Call(Address::random()),
        value: 0.into(),
        input: input.into(),
    });

    let hash = transaction.signature_hash();

    let sender_secret_key = B256::random();
    let signature = sign_message(sender_secret_key, hash).expect("signature should always succeed");

    TransactionSigned::from_transaction_and_signature(transaction, signature)
}

struct BenchController {
    pub pool: EthTxPool,
    pub transactions: FullTransactionList,
    pub gas_limit: u64,
    pub block_policy: EthBlockPolicy,
    pub reserve_balance_cache: PassthruReserveBalanceCache<InMemoryState>,
}

type SignatureCollectionType = MultiSig<NopSignature>;

fn create_pool_and_transactions() -> BenchController {
    let mut txpool = EthTxPool::default();

    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let eth_block_policy =
        EthBlockPolicy::new(GENESIS_SEQ_NUM, Balance::MAX, EXECUTION_DELAY, 0, 1337);

    let mut rng = ChaCha8Rng::seed_from_u64(420);

    let txns = (0..NUM_TRANSACTIONS)
        .map(|_| make_tx(&mut rng, TRANSACTION_SIZE_BYTES))
        .collect::<Vec<_>>();
    let acc = txns
        .iter()
        .map(|tx| (EthAddress(tx.recover_signer().unwrap()), 0));
    let mut reserve_balance_cache =
        PassthruReserveBalanceCache::new(InMemoryState::new(acc, Balance::MAX, 0), EXECUTION_DELAY);

    let proposal_gas_limit: u64 = txns
        .iter()
        .map(|txn| txn.transaction.gas_limit())
        .sum::<u64>()
        + 1;

    let mut txns_encoded: Vec<u8> = vec![];
    txns.encode(&mut txns_encoded);

    let bytes = Bytes::copy_from_slice(&txns_encoded);
    let txns: Vec<Bytes> = txns
        .iter()
        .map(|t| Bytes::from(t.envelope_encoded()))
        .collect();

    assert!(!TxPool::<
        SignatureCollectionType,
        EthBlockPolicy,
        InMemoryState,
        PassthruReserveBalanceCache<InMemoryState>,
    >::insert_tx(
        &mut txpool,
        txns,
        &eth_block_policy,
        &mut reserve_balance_cache,
    )
    .is_empty());
    let txns_list = FullTransactionList::new(bytes);

    BenchController {
        pool: txpool,
        transactions: txns_list,
        gas_limit: proposal_gas_limit,
        block_policy: eth_block_policy,
        reserve_balance_cache,
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let proposal_txn_limit: usize = NUM_TRANSACTIONS;
    let mut group = c.benchmark_group("proposal");

    match PerfController::from_env() {
        Ok(mut perf) => {
            group.bench_function("create_proposal", |b| {
                b.iter_batched_ref(
                    create_pool_and_transactions,
                    |controller| {
                        perf.enable();
                        TxPool::<
                            SignatureCollectionType,
                            EthBlockPolicy,
                            InMemoryState,
                            PassthruReserveBalanceCache<InMemoryState>,
                        >::create_proposal(
                            &mut controller.pool,
                            controller.block_policy.get_last_commit() + SeqNum(1),
                            proposal_txn_limit,
                            controller.gas_limit,
                            &controller.block_policy,
                            Default::default(),
                            &mut controller.reserve_balance_cache,
                        )
                        .unwrap();
                        perf.disable();
                    },
                    BatchSize::SmallInput,
                )
            });
        }
        Err(e) => {
            println!(
                "failed to initialize perf controller, continuing without sampling. did you define the `PERF_CTL_FD` and `PERF_CTL_FD_ACK` environment variables? error: {:?}",
                e
            );
            group.bench_function("create_proposal", |b| {
                b.iter_batched_ref(
                    create_pool_and_transactions,
                    |controller| {
                        TxPool::<
                            SignatureCollectionType,
                            EthBlockPolicy,
                            InMemoryState,
                            PassthruReserveBalanceCache<InMemoryState>,
                        >::create_proposal(
                            &mut controller.pool,
                            controller.block_policy.get_last_commit() + SeqNum(1),
                            proposal_txn_limit,
                            controller.gas_limit,
                            &controller.block_policy,
                            Default::default(),
                            &mut controller.reserve_balance_cache,
                        )
                        .unwrap();
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
