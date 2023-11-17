use std::{env, fs::File, io::Read};

use monad_compress::{
    brotli::BrotliCompression, deflate::DeflateCompression, lz4::Lz4Compression, CompressionAlgo,
};
use peak_alloc::PeakAlloc;

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <file_path>", args[0]);
        return;
    }
    let file_path = &args[1];
    let mut file = File::open(file_path).expect("file exists");
    let mut txns = Vec::new();
    file.read_to_end(&mut txns).expect("file read success");

    let bg_mem_usage = PEAK_ALLOC.current_usage_as_mb();

    println!("{:-^50}", "brotli");
    println!(
        "{:<10} | {:<10} | {:<10} | {:<10}",
        "level", "ratio", "mem (MB)", "compressed (KB)"
    );

    for compression_level in 0..=11 {
        // compress txns
        PEAK_ALLOC.reset_peak_usage();
        let algo = BrotliCompression::new(compression_level, 22);
        let mut compressed = Vec::new();
        algo.compress(&txns, &mut compressed)
            .expect("compression success");

        println!(
            "{:<10} | {:<10.3} | {:<10.3} | {:<10}",
            compression_level,
            compressed.len() as f64 / txns.len() as f64,
            PEAK_ALLOC.peak_usage_as_mb() - bg_mem_usage,
            compressed.len() / 1024
        );
    }

    println!("{:-^50}", "deflate");
    println!(
        "{:<10} | {:<10} | {:<10} | {:<10}",
        "level", "ratio", "mem (MB)", "compressed (KB)"
    );
    for compression_level in 0..=11 {
        // compress txns
        PEAK_ALLOC.reset_peak_usage();
        let algo = DeflateCompression::new(compression_level, 22);
        let mut compressed = Vec::new();
        algo.compress(&txns, &mut compressed)
            .expect("compression success");

        println!(
            "{:<10} | {:<10.3} | {:<10.3} | {:<10}",
            compression_level,
            compressed.len() as f64 / txns.len() as f64,
            PEAK_ALLOC.peak_usage_as_mb() - bg_mem_usage,
            compressed.len() / 1024
        );
    }

    println!("{:-^50}", "lz4");
    println!(
        "{:<10} | {:<10} | {:<10} | {:<10}",
        "level", "ratio", "mem (MB)", "compressed (KB)"
    );
    for compression_level in 0..=16 {
        // compress txns
        PEAK_ALLOC.reset_peak_usage();
        let algo = Lz4Compression::new(compression_level, 90);
        let mut compressed = Vec::new();
        algo.compress(&txns, &mut compressed)
            .expect("compression success");

        println!(
            "{:<10} | {:<10.3} | {:<10.3} | {:<10}",
            compression_level,
            compressed.len() as f64 / txns.len() as f64,
            PEAK_ALLOC.peak_usage_as_mb() - bg_mem_usage,
            compressed.len() / 1024
        );
    }
}
