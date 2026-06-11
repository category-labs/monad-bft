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

//! Throughput + latency benchmark for the enclave signer's hot secp path.
//!
//! Connects to a (provisioned, or `--generate`-d) signer and hammers `SignSecp`
//! from N threads for a fixed duration, reporting signs/sec and p50/p99/max
//! round-trip latency. The headline question this answers: can the local
//! crossing sustain RaptorCast's per-merkle-batch signing rate?

use std::{
    process::exit,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use clap::Parser;
use monad_enclave_signer_proto::{EnclaveSigner, TransportConfig};

#[derive(Parser, Debug)]
#[command(about = "Benchmark the enclave signer hot secp path")]
struct Args {
    #[arg(long)]
    vsock_cid: Option<u32>,
    #[arg(long)]
    vsock_port: Option<u32>,
    #[arg(long)]
    unix: Option<std::path::PathBuf>,

    /// Number of concurrent signing threads.
    #[arg(long, default_value_t = default_threads())]
    threads: usize,

    /// Benchmark duration in seconds.
    #[arg(long, default_value_t = 5)]
    duration_secs: u64,

    /// Connection pool size (defaults to thread count).
    #[arg(long)]
    pool: Option<usize>,
}

fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

fn main() {
    let args = Args::parse();

    let transport = match (args.vsock_cid, args.vsock_port, args.unix.as_ref()) {
        (Some(cid), Some(port), None) => TransportConfig::Vsock { cid, port },
        (None, None, Some(path)) => TransportConfig::Unix { path: path.clone() },
        _ => {
            eprintln!("error: specify (--vsock-cid N --vsock-port N) or --unix <PATH>");
            exit(2);
        }
    };

    let pool = args.pool.unwrap_or(args.threads);
    let signer = Arc::new(EnclaveSigner::connect(transport, pool).unwrap_or_else(|e| {
        eprintln!("error: cannot reach signer: {e}");
        exit(1);
    }));

    // Fail early if the signer has no key (must be provisioned or --generate'd).
    if let Err(e) = signer.pubkeys() {
        eprintln!("error: signer not provisioned: {e}");
        exit(1);
    }

    let digest = [0xab_u8; 32];
    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU64::new(0));

    println!(
        "benchmarking SignSecp: {} threads, pool {}, {}s",
        args.threads, pool, args.duration_secs
    );

    let start = Instant::now();
    let handles: Vec<_> = (0..args.threads)
        .map(|_| {
            let signer = Arc::clone(&signer);
            let stop = Arc::clone(&stop);
            let count = Arc::clone(&count);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match signer.sign_secp(&digest) {
                        Ok(_) => {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("sign error: {e}");
                            break;
                        }
                    }
                }
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(args.duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total = count.load(Ordering::Relaxed);
    println!("\n--- results ---");
    println!(
        "total signs: {total}   throughput: {:.0} signs/sec   over {:.2}s",
        total as f64 / elapsed,
        elapsed
    );
    println!("{}", signer.latency_report());
}
