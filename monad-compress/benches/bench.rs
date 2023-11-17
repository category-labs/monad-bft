use std::{fs::File, io::Read, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use monad_compress::{
    brotli::BrotliCompression, deflate::DeflateCompression, lz4::Lz4Compression, CompressionAlgo,
};

fn bench_txns(c: &mut Criterion) {
    let mut file = File::open("examples/txbatch.rlp").expect("file exists");
    let mut txns = Vec::new();
    file.read_to_end(&mut txns).expect("file read success");

    for compression_level in 0..=8 {
        c.bench_function(
            &format!("bench_brotli_compress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = BrotliCompression::new(compression_level, 22);
                    let mut compressed = Vec::new();
                    algo.compress(&txns, &mut compressed)
                        .expect("compression success");
                })
            },
        );

        let algo = BrotliCompression::new(compression_level, 22);
        let mut compressed = Vec::new();
        algo.compress(&txns, &mut compressed)
            .expect("compression success");

        c.bench_function(
            &format!("bench_brotli_decompress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = BrotliCompression::new(compression_level, 22);
                    let mut decompressed = Vec::new();
                    algo.decompress(&compressed, &mut decompressed)
                        .expect("decompression success");
                })
            },
        );
    }

    for compression_level in 0..=8 {
        c.bench_function(
            &format!("bench_deflate_compress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = DeflateCompression::new(compression_level, 0);
                    let mut compressed = Vec::new();
                    algo.compress(&txns, &mut compressed)
                        .expect("compression success");
                })
            },
        );

        let algo = DeflateCompression::new(compression_level, 0);
        let mut compressed = Vec::new();
        algo.compress(&txns, &mut compressed)
            .expect("compression success");

        c.bench_function(
            &format!("bench_deflate_decompress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = DeflateCompression::new(compression_level, 0);
                    let mut decompressed = Vec::new();
                    algo.decompress(&compressed, &mut decompressed)
                        .expect("compression success");
                })
            },
        );
    }

    for compression_level in 0..=10 {
        c.bench_function(
            &format!("bench_lz4_compress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = Lz4Compression::new(compression_level, 0);
                    let mut compressed = Vec::new();
                    algo.compress(&txns, &mut compressed)
                        .expect("compression success");
                })
            },
        );

        let algo = Lz4Compression::new(compression_level, 0);
        let mut compressed = Vec::new();
        algo.compress(&txns, &mut compressed)
            .expect("compression success");

        c.bench_function(
            &format!("bench_lz4_decompress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = Lz4Compression::new(compression_level, 0);
                    let mut decompressed = Vec::new();
                    algo.decompress(&compressed, &mut decompressed)
                        .expect("compression success");
                })
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
