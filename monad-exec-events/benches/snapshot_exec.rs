use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use monad_event_ring::{DecodedEventRing, EventNextResult, SnapshotEventRing};
use monad_exec_events::ExecEventDecoder;

fn bench_snapshot(c: &mut Criterion) {
    // TODO(andr-dev): Re-enable benchmark once test data updated.
    return;

    const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
    const SNAPSHOT_ZSTD_BYTES: &[u8] =
        include_bytes!("../../monad-exec-events/test/data/exec-events-emn-30b-15m.zst");

    let mut g = c.benchmark_group("snapshot_exec");

    let snapshot = SnapshotEventRing::<ExecEventDecoder>::new_from_zstd_bytes(
        SNAPSHOT_ZSTD_BYTES,
        SNAPSHOT_NAME,
    )
    .unwrap();

    let items = {
        let mut event_reader = snapshot.create_reader();

        let mut items = 0;

        loop {
            match event_reader.next_descriptor() {
                EventNextResult::Ready(_) => {
                    items += 1;
                }
                EventNextResult::NotReady => break,
                EventNextResult::Gap => panic!("snapshot cannot gap"),
            }
        }

        items
    };

    g.throughput(criterion::Throughput::Elements(items));
    g.bench_function("iter_read", |b| {
        let snapshot = SnapshotEventRing::<ExecEventDecoder>::new_from_zstd_bytes(
            SNAPSHOT_ZSTD_BYTES,
            SNAPSHOT_NAME,
        )
        .unwrap();

        b.iter_batched_ref(
            || snapshot.create_reader(),
            |event_reader| loop {
                match event_reader.next_descriptor() {
                    EventNextResult::Ready(event_descriptor) => {
                        let actual_payload = event_descriptor.try_read();

                        black_box(actual_payload);
                    }
                    EventNextResult::NotReady => break,
                    EventNextResult::Gap => panic!("snapshot cannot gap"),
                };
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_snapshot);
criterion_main!(benches);
