use std::{fs::File, io::Read, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use monad_compress::{brotli::BrotliCompression, CompressionAlgo};

fn bench_txns(c: &mut Criterion) {
    let mut file = File::open("examples/txbatch.rlp").expect("file exists");
    let mut txns = Vec::new();
    file.read_to_end(&mut txns).expect("file read success");

    for compression_level in 0..=11 {
        c.bench_function(
            &format!("bench_compress_txns_level_{compression_level}"),
            |b| {
                b.iter_batched(
                    || txns.clone(),
                    |txns| {
                        let algo = BrotliCompression::new(compression_level, 22);
                        let mut compressed = Vec::new();
                        algo.compress(&txns, &mut compressed)
                            .expect("compression success");
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(10));
    targets = bench_txns
}

criterion_main!(benches);
