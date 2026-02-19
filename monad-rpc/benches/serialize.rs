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

use std::hint::black_box;

use alloy_primitives::B256;
use alloy_rpc_types::{Block, BlockTransactions, Log, Transaction, TransactionReceipt};
use arbitrary::{Arbitrary, Unstructured};
use criterion::{
    criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup, Criterion,
    Throughput,
};
use itertools::Itertools;
use monad_rpc::types::{
    eth_json::{serialize_result, MonadBlock, MonadLog, MonadTransactionReceipt},
    jsonrpc::{RequestId, Response, ResponseWrapper},
};
use serde::Serialize;

fn serialize<T>(value: &T) -> String
where
    T: Serialize,
{
    let result = serialize_result(value);

    let response = ResponseWrapper::Single(Response::from_result(RequestId::Number(0), result));

    let response_raw_value = serde_json::value::to_raw_value(&response).unwrap();

    // HttpResponse::Ok().json(response_raw_value) in monad-rpc/src/handlers/mod.rs
    let ret = serde_json::to_string(&response_raw_value).unwrap();
    assert_eq!(ret.as_str(), response_raw_value.get());

    ret
}

fn bench_serialize<T, M>(g: &mut BenchmarkGroup<'_, M>, name: &'static str, value: &T)
where
    T: Serialize,
    M: Measurement,
{
    g.throughput(Throughput::Bytes(serialize(value).len() as u64));
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
