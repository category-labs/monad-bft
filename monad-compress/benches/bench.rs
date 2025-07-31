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

use std::{fs::File, io::Read, time::Duration};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use monad_compress::{
    brotli::BrotliCompression, deflate::DeflateCompression, lz4::Lz4Compression,
    util::BoundedWriter, CompressionAlgo,
};

fn bench_txns(c: &mut Criterion) {
    let mut file = File::open("examples/txbatch.rlp").expect("file exists");
    let mut txns = Vec::new();
    file.read_to_end(&mut txns).expect("file read success");

    let mut dictionary_file = File::open("examples/example.dict").expect("file exists");
    let mut dictionary = Vec::new();
    dictionary_file
        .read_to_end(&mut dictionary)
        .expect("file read success");

    for compression_level in 0..=monad_compress::brotli::MAX_COMPRESSION_LEVEL {
        c.bench_function(
            &format!("bench_brotli_compress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = BrotliCompression::new(compression_level, 22, Vec::new());
                    let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.compress(&txns, &mut compressed_writer)
                        .expect("compression success");
                })
            },
        );

        let algo = BrotliCompression::new(compression_level, 22, Vec::new());
        let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
        algo.compress(&txns, &mut compressed_writer)
            .expect("compression success");
        let compressed: Bytes = compressed_writer.into();

        c.bench_function(
            &format!("bench_brotli_decompress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = BrotliCompression::new(compression_level, 22, Vec::new());
                    let mut decompressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.decompress(&compressed, &mut decompressed_writer)
                        .expect("decompression success");
                })
            },
        );
    }

    for compression_level in 0..=monad_compress::brotli::MAX_COMPRESSION_LEVEL {
        c.bench_function(
            &format!("bench_brotli_compress_with_dict_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = BrotliCompression::new(compression_level, 22, dictionary.clone());
                    let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.compress(&txns, &mut compressed_writer)
                        .expect("compression success");
                })
            },
        );

        let algo = BrotliCompression::new(compression_level, 22, dictionary.clone());
        let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
        algo.compress(&txns, &mut compressed_writer)
            .expect("compression success");
        let compressed: Bytes = compressed_writer.into();

        c.bench_function(
            &format!("bench_brotli_decompress_with_dict_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = BrotliCompression::new(compression_level, 22, dictionary.clone());
                    let mut decompressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.decompress(&compressed, &mut decompressed_writer)
                        .expect("decompression success");
                })
            },
        );
    }

    for compression_level in 0..=monad_compress::deflate::MAX_COMPRESSION_LEVEL {
        c.bench_function(
            &format!("bench_deflate_compress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = DeflateCompression::new(compression_level, 0, Vec::new());
                    let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.compress(&txns, &mut compressed_writer)
                        .expect("compression success");
                })
            },
        );

        let algo = DeflateCompression::new(compression_level, 0, Vec::new());
        let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
        algo.compress(&txns, &mut compressed_writer)
            .expect("compression success");
        let compressed: Bytes = compressed_writer.into();

        c.bench_function(
            &format!("bench_deflate_decompress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = DeflateCompression::new(compression_level, 0, Vec::new());
                    let mut decompressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.decompress(&compressed, &mut decompressed_writer)
                        .expect("decompression success");
                })
            },
        );
    }

    for compression_level in 0..=monad_compress::lz4::MAX_COMPRESSION_LEVEL {
        c.bench_function(
            &format!("bench_lz4_compress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = Lz4Compression::new(compression_level, 0, Vec::new());
                    let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.compress(&txns, &mut compressed_writer)
                        .expect("compression success");
                })
            },
        );

        let algo = Lz4Compression::new(compression_level, 0, Vec::new());
        let mut compressed_writer = BoundedWriter::new(txns.len() as u32);
        algo.compress(&txns, &mut compressed_writer)
            .expect("compression success");
        let compressed: Bytes = compressed_writer.into();

        c.bench_function(
            &format!("bench_lz4_decompress_txns_level_{compression_level}"),
            |b| {
                b.iter(|| {
                    let algo = Lz4Compression::new(compression_level, 0, Vec::new());
                    let mut decompressed_writer = BoundedWriter::new(txns.len() as u32);
                    algo.decompress(&compressed, &mut decompressed_writer)
                        .expect("decompression success");
                })
            },
        );
    }
}

fn nop_bench(_c: &mut Criterion) {}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(10));
    targets = nop_bench,
    // targets = bench_txns,
}

criterion_main!(benches);
