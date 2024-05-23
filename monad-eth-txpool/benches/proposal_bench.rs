use alloy_rlp::Encodable;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_consensus_types::{payload::FullTransactionList, txpool::TxPool};
use monad_crypto::NopSignature;
use monad_eth_txpool::{EthBlockPolicy, EthTxPool};
use monad_multi_sig::MultiSig;
use rand::{Rng, RngCore};
use reth_primitives::{
    sign_message, Address, Transaction, TransactionKind, TransactionSigned, TxLegacy, B256,
};

const NUM_TRANSACTIONS: usize = 10_000;
const TRANSACTION_SIZE_BYTES: usize = 400;

fn make_tx(input_len: usize) -> TransactionSigned {
    let mut input = vec![0; input_len];
    rand::thread_rng().fill_bytes(&mut input);
    let transaction = Transaction::Legacy(TxLegacy {
        chain_id: Some(1337),
        nonce: rand::thread_rng().gen_range(10_000..50_000),
        gas_price: 1,
        gas_limit: 6400,
        to: TransactionKind::Call(Address::random()),
        value: 0.into(),
        input: input.into(),
    });

    let hash = transaction.signature_hash();

    let sender_secret_key = B256::random();
    let signature = sign_message(sender_secret_key, hash).expect("signature should always succeed");

    TransactionSigned {
        transaction,
        hash,
        signature,
    }
}

struct BenchController {
    pub pool: EthTxPool,
    pub transactions: FullTransactionList,
    pub gas_limit: u64,
}

type SignatureCollectionType = MultiSig<NopSignature>;

fn create_pool_and_transactions() -> BenchController {
    let mut txpool = EthTxPool::default();
    let txns = (0..NUM_TRANSACTIONS)
        .map(|_| make_tx(TRANSACTION_SIZE_BYTES))
        .collect::<Vec<_>>();

    let proposal_gas_limit: u64 = txns
        .iter()
        .map(|txn| txn.transaction.gas_limit())
        .sum::<u64>()
        + 1;

    let mut txns_encoded: Vec<u8> = vec![];
    txns.encode(&mut txns_encoded);

    let bytes = Bytes::copy_from_slice(&txns_encoded);

    for txn in txns.iter() {
        TxPool::<SignatureCollectionType, EthBlockPolicy>::insert_tx(
            &mut txpool,
            Bytes::from(txn.envelope_encoded()),
        );
    }
    let txns_list = FullTransactionList::new(bytes);

    BenchController {
        pool: txpool,
        transactions: txns_list,
        gas_limit: proposal_gas_limit,
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let proposal_txn_limit: usize = NUM_TRANSACTIONS;
    let mut group = c.benchmark_group("proposal");
    group.bench_function("create_proposal", |b| {
        b.iter_batched_ref(
            create_pool_and_transactions,
            |controller| {
                TxPool::<SignatureCollectionType, EthBlockPolicy>::create_proposal(
                    &mut controller.pool,
                    proposal_txn_limit,
                    controller.gas_limit,
                    Default::default(),
                );
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
