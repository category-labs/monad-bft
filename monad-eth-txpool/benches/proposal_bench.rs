use std::{
    fs::File,
    io::{Read, Write},
    os::fd::FromRawFd,
    str::FromStr,
    sync::Arc,
};

use alloy_rlp::Encodable;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_consensus_types::{payload::FullTransactionList, txpool::TxPool};
use monad_eth_txpool::EthTxPool;
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
        txpool.insert_tx(Bytes::from(txn.envelope_encoded()));
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

    let ctl_fd = std::env::var("PERF_CTL_FD").unwrap();
    let mut control =
        Arc::new(unsafe { File::from_raw_fd(std::ffi::c_int::from_str(&ctl_fd).unwrap()) });
    let ctl_fd_ack = std::env::var("PERF_CTL_FD_ACK").unwrap();
    let mut control_ack =
        Arc::new(unsafe { File::from_raw_fd(std::ffi::c_int::from_str(&ctl_fd_ack).unwrap()) });
    group.bench_function("create_proposal", |b| {
        b.iter_batched_ref(
            create_pool_and_transactions,
            |controller| {
                control.write_all(b"enable\n").unwrap();
                let mut buf = [0; 5];
                control_ack.read_exact(&mut buf).unwrap();
                controller.pool.create_proposal(
                    proposal_txn_limit,
                    controller.gas_limit,
                    vec![controller.transactions.clone()],
                );
                control.write_all(b"disable\n").unwrap();
                control_ack.read_exact(&mut buf).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
