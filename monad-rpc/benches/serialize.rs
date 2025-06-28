use alloy_primitives::B256;
use alloy_rpc_types::{Block, BlockTransactions, Log, Transaction, TransactionReceipt};
use arbitrary::{Arbitrary, Unstructured};
use criterion::{
    black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup,
    Criterion, Throughput,
};
use itertools::Itertools;
use monad_rpc::{
    eth_json_types::{serialize_result, MonadBlock, MonadLog, MonadTransactionReceipt},
    jsonrpc::{Response, ResponseWrapper},
};
use serde::Serialize;

fn serialize<T>(value: &T) -> String
where
    T: Serialize,
{
    let result = serialize_result(value);

    let response = ResponseWrapper::Single(Response::from_result(
        serde_json::Value::Number(serde_json::Number::from(0u64)),
        result,
    ));

    // HttpResponse::Ok().json(&response) in monad-rpc/src/handlers/mod.rs
    serde_json::to_string(&response).unwrap()
}

fn bench_serialize<T, M>(g: &mut BenchmarkGroup<'_, M>, name: &'static str, value: &T)
where
    T: Serialize,
    M: Measurement,
{
    g.throughput(Throughput::Bytes(serialize(value).as_bytes().len() as u64));
    g.bench_function(name, |b| {
        b.iter(|| serialize(black_box(value)));
    });
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("serialize");

    g.sample_size(1_000);
    g.nresamples(1_000_000);

    bench_serialize(
        &mut g,
        "block_hashes_1k",
        &MonadBlock(Block {
            header: Unstructured::new(&[0]).arbitrary().unwrap(),
            uncles: vec![],
            transactions: BlockTransactions::Hashes(
                (0..1_000u64)
                    .map(|idx| B256::arbitrary(&mut Unstructured::new(&idx.to_le_bytes())).unwrap())
                    .collect(),
            ),
            withdrawals: None,
        }),
    );

    bench_serialize(
        &mut g,
        "block_full_1k",
        &MonadBlock(Block {
            header: Unstructured::new(&[0]).arbitrary().unwrap(),
            uncles: vec![],
            transactions: BlockTransactions::Full(
                (0..1_000u64)
                    .map(|idx| {
                        Transaction::arbitrary(&mut Unstructured::new(&idx.to_le_bytes())).unwrap()
                    })
                    .collect(),
            ),
            withdrawals: None,
        }),
    );

    bench_serialize(
        &mut g,
        "block_receipts_1k",
        &(0..1_000u64)
            .map(|idx| {
                MonadTransactionReceipt(
                    TransactionReceipt::arbitrary(&mut Unstructured::new(&idx.to_le_bytes()))
                        .unwrap(),
                )
            })
            .collect_vec(),
    );

    bench_serialize(
        &mut g,
        "logs_1k",
        &(0..1_000u64)
            .map(|idx| {
                MonadLog(Log::arbitrary(&mut Unstructured::new(&idx.to_le_bytes())).unwrap())
            })
            .collect_vec(),
    );
}

criterion_group!(benches, bench);
criterion_main!(benches);
