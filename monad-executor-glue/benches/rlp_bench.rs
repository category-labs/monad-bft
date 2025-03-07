use alloy_rlp::{RlpDecodable, RlpEncodable};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use monad_executor_glue::{
    StateSyncNetworkMessage, StateSyncRequest, StateSyncResponse, StateSyncUpsertType,
    StateSyncUpsertV1, SELF_STATESYNC_VERSION,
};

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Wrapped<T>(T);

pub fn criterion_benchmark(c: &mut Criterion) {
    let num_upserts = 1_000_000;
    let upsert_size = 100_usize;

    let statesync_response: Wrapped<_> = Wrapped(Wrapped(Wrapped(Wrapped(Wrapped(Wrapped(
        StateSyncNetworkMessage::Response(StateSyncResponse {
            version: SELF_STATESYNC_VERSION,
            nonce: 0,
            response_index: 0,
            request: StateSyncRequest {
                version: SELF_STATESYNC_VERSION,
                prefix: 0,
                prefix_bytes: 0,
                target: 0,
                from: 0,
                until: 0,
                old_target: 0,
            },
            response: (0..num_upserts)
                .map(|_| StateSyncUpsertV1 {
                    upsert_type: StateSyncUpsertType::Account,
                    data: vec![0_u8; upsert_size].into(),
                })
                .collect::<Vec<_>>(),
            response_n: 0,
        }),
    ))))));
    let serialized_statesync_response = alloy_rlp::encode(&statesync_response);

    let mut group = c.benchmark_group("statesync_response");
    group.throughput(Throughput::Bytes(serialized_statesync_response.len() as u64));
    group.bench_function("encoder", |b| {
        b.iter(|| {
            let serialized = alloy_rlp::encode(&statesync_response);
            if serialized_statesync_response != serialized {
                panic!("failed to roundtrip")
            }
        });
    });
    group.bench_function("decoder", |b| {
        b.iter(|| {
            let deserialized: Wrapped<_> = alloy_rlp::decode_exact(&serialized_statesync_response)
                .expect("failed to roundtrip");
            if statesync_response != deserialized {
                panic!("failed to roundtrip")
            }
        });
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
