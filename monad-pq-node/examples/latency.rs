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

//! Latency read back from a ledger (§14.6): slot finalization after the
//! deadline, by path, and tx end-to-end (`finalized_at − sent_at`) for every
//! tx stamped with `sent_at_ns`, by payload-prefix label and path.
//!
//! `cargo run --example latency -- --ledger-dir D [--since SLOT]
//!  [--label NAME=PAYLOAD_PREFIX]...`

use std::{collections::BTreeMap, path::PathBuf};

use monad_mcp_chorus::ledger::{FinalizationPath, LedgerReader, decode_batch};
use monad_pq_chorus::{PqPath, PqProof};

type Error = Box<dyn std::error::Error>;

struct Args {
    ledger_dir: PathBuf,
    since: u64,
    labels: Vec<(String, Vec<u8>)>,
}

fn parse_args() -> Result<Args, Error> {
    let usage = "usage: latency --ledger-dir D [--since SLOT] [--label NAME=PAYLOAD_PREFIX]...";
    let mut ledger_dir = None;
    let mut since = 0;
    let mut labels = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(usage);
        match arg.as_str() {
            "--ledger-dir" => ledger_dir = Some(PathBuf::from(value()?)),
            "--since" => since = value()?.parse()?,
            "--label" => {
                let label = value()?;
                let (name, prefix) = label.split_once('=').ok_or(usage)?;
                labels.push((name.to_owned(), prefix.as_bytes().to_vec()));
            }
            _ => return Err(usage.into()),
        }
    }
    let ledger_dir = ledger_dir.ok_or(usage)?;
    Ok(Args {
        ledger_dir,
        since,
        labels,
    })
}

// nearest-rank percentile of sorted samples
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn report(name: &str, samples: &mut [f64]) {
    if samples.is_empty() {
        println!("{name:<28} n=0");
        return;
    }
    samples.sort_by(f64::total_cmp);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    println!(
        "{name:<28} n={:<6} p50={:>7.1} p90={:>7.1} p99={:>7.1} max={:>7.1} mean={:>7.1} ms",
        samples.len(),
        percentile(samples, 50.0),
        percentile(samples, 90.0),
        percentile(samples, 99.0),
        samples[samples.len() - 1],
        mean,
    );
}

fn ms(nanos: i128) -> f64 {
    nanos as f64 / 1e6
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let reader = LedgerReader::new(&args.ledger_dir);

    let mut slot_latency: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut tx_latency: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut slots = 0usize;
    for slot in reader.scan()? {
        if slot < args.since {
            continue;
        }
        let meta = reader.read_meta(slot)?;
        slots += 1;
        let path = match alloy_rlp::decode_exact::<PqProof>(&reader.read_proof(slot)?) {
            Ok(proof) => format!("{:?}", proof.path),
            Err(_) => match meta.path {
                FinalizationPath::Fast => format!("{:?}", PqPath::Fast),
                FinalizationPath::Fallback => "Fallback".into(),
            },
        };
        let finalized_at = meta.finalized_at_ns as i128;
        if let Some(deadline) = meta.deadline_ns {
            let after = ms(finalized_at - deadline as i128);
            slot_latency.entry("all".into()).or_default().push(after);
            slot_latency.entry(path.clone()).or_default().push(after);
        }
        for lane in meta.lanes.iter().filter(|lane| lane.tx_count > 0) {
            let Some(payload) = reader.read_lane(slot, lane.index)? else {
                continue;
            };
            for tx in decode_batch(&payload)? {
                if tx.sent_at_ns == 0 {
                    continue;
                }
                let latency = ms(finalized_at - i128::from(tx.sent_at_ns));
                let label = args
                    .labels
                    .iter()
                    .find(|(_, prefix)| tx.payload.starts_with(prefix))
                    .map_or("other", |(name, _)| name.as_str());
                for key in [label.to_owned(), format!("{label}/{path}")] {
                    tx_latency.entry(key).or_default().push(latency);
                }
            }
        }
    }

    println!(
        "ledger {} ({slots} blocks from slot {})",
        args.ledger_dir.display(),
        args.since
    );
    println!("slot finalization after deadline:");
    for (name, samples) in &mut slot_latency {
        report(&format!("  slot {name}"), samples);
    }
    println!("tx end-to-end (sent -> finalized):");
    for (name, samples) in &mut tx_latency {
        report(&format!("  tx {name}"), samples);
    }
    Ok(())
}
