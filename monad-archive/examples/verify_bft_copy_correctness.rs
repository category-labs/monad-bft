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

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use clap::Parser;
use eyre::{bail, eyre, Context, Result};
use monad_archive::{
    cli::ArchiveArgs,
    kvstore::KVReader,
    metrics::Metrics,
    model::{bft_ledger::BftBlockHeader, block_data_archive::BlockDataArchive},
    prelude::Decodable,
};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[clap(about = "Verify a copy-data BFT migration sink against source bytes")]
struct Args {
    #[clap(help = "Sink root path used in `--sink \"fs <path>\"`")]
    sink_root: PathBuf,

    #[clap(
        long,
        default_value = "aws mainnet-deu-009-0",
        help = "Archive args for source (e.g. `aws mainnet-deu-009-0`)"
    )]
    source: String,

    #[clap(
        long,
        help = "If set, fail unless marker file count matches this value"
    )]
    expected_markers: Option<usize>,

    #[clap(
        long,
        default_value_t = 8,
        help = "Target samples per marker across committed interval"
    )]
    samples_per_marker: usize,
}

#[derive(Debug, Deserialize)]
struct IndexedRangeMarker {
    #[serde(rename = "start_num")]
    head_num: u64,
    #[serde(rename = "end_num")]
    tail_num: u64,
    #[serde(rename = "start_id")]
    head_id: String,
    #[serde(rename = "end_id")]
    tail_id: String,
}

fn normalize_hex_id(input: &str) -> Result<String> {
    let value = input.trim().strip_prefix("0x").unwrap_or(input.trim());
    if value.len() != 64 {
        bail!("invalid hex id length {}: {input}", value.len());
    }
    if !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid hex id characters: {input}");
    }
    Ok(value.to_ascii_lowercase())
}

fn read_trimmed(path: &Path) -> Result<String> {
    let bytes = fs::read(path).wrap_err_with(|| format!("read failed: {}", path.display()))?;
    let value = String::from_utf8(bytes)
        .wrap_err_with(|| format!("utf8 decode failed: {}", path.display()))?;
    Ok(value.trim().to_string())
}

fn build_samples(min_num: u64, max_num: u64, requested_samples: usize) -> BTreeSet<u64> {
    let mut samples = BTreeSet::new();
    if min_num > max_num {
        return samples;
    }

    let target = requested_samples.max(2);
    let span = max_num.saturating_sub(min_num);
    samples.insert(min_num);
    samples.insert(max_num);

    for idx in 0..target {
        let denominator = (target - 1) as u64;
        let num = if denominator == 0 {
            min_num
        } else {
            min_num + span.saturating_mul(idx as u64) / denominator
        };
        samples.insert(num);
    }

    samples
}

async fn verify_sample(
    sink_root: &Path,
    source: &BlockDataArchive,
    block_num: u64,
) -> Result<(String, String)> {
    let index_path = sink_root
        .join("blocks")
        .join("bft")
        .join("index")
        .join(block_num.to_string());
    let index_id = normalize_hex_id(&read_trimmed(&index_path)?)?;

    let header_path = sink_root
        .join("blocks")
        .join("bft")
        .join("ledger")
        .join("headers")
        .join(&index_id);
    let local_header = fs::read(&header_path)
        .wrap_err_with(|| format!("missing local header for block {block_num}, id {index_id}"))?;

    let source_header_key = format!("bft_block/{index_id}.header");
    let source_header = source
        .store
        .get(&source_header_key)
        .await?
        .ok_or_else(|| eyre!("missing source header key {source_header_key}"))?;
    if source_header.as_ref() != local_header.as_slice() {
        bail!("header bytes mismatch for block {block_num}, id {index_id}");
    }

    let header = BftBlockHeader::decode(&mut &local_header[..])
        .wrap_err_with(|| format!("failed to decode local header for block {block_num}"))?;
    if header.seq_num.0 != block_num {
        bail!(
            "header seq mismatch: index block_num={}, decoded seq={}",
            block_num,
            header.seq_num.0
        );
    }

    let decoded_id = hex::encode(header.get_id().0);
    if decoded_id != index_id {
        bail!(
            "header id mismatch at block {block_num}: index_id={index_id}, decoded_id={decoded_id}"
        );
    }

    let body_id = hex::encode(header.block_body_id.0);
    let body_path = sink_root
        .join("blocks")
        .join("bft")
        .join("ledger")
        .join("bodies")
        .join(&body_id);
    let local_body = fs::read(&body_path)
        .wrap_err_with(|| format!("missing local body for block {block_num}, body {body_id}"))?;

    let source_body_key = format!("bft_block/{body_id}.body");
    let source_body = source
        .store
        .get(&source_body_key)
        .await?
        .ok_or_else(|| eyre!("missing source body key {source_body_key}"))?;
    if source_body.as_ref() != local_body.as_slice() {
        bail!("body bytes mismatch for block {block_num}, body {body_id}");
    }

    Ok((index_id, body_id))
}

async fn load_markers(markers_dir: &Path) -> Result<Vec<IndexedRangeMarker>> {
    let mut markers = Vec::new();
    let entries = fs::read_dir(markers_dir)
        .wrap_err_with(|| format!("missing markers dir {}", markers_dir.display()))?;
    for entry in entries {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let marker_bytes =
            fs::read(&path).wrap_err_with(|| format!("failed reading {}", path.display()))?;
        let marker: IndexedRangeMarker = serde_json::from_slice(&marker_bytes)
            .wrap_err_with(|| format!("invalid marker json {}", path.display()))?;
        markers.push(marker);
    }
    Ok(markers)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source_args: ArchiveArgs = args.source.parse()?;
    let source = source_args
        .build_block_data_archive(&Metrics::none())
        .await
        .wrap_err("failed to build source archive")?;

    let markers_dir = args
        .sink_root
        .join("blocks")
        .join("bft")
        .join("index_markers");
    let mut markers = load_markers(&markers_dir).await?;
    if let Some(expected) = args.expected_markers {
        if markers.len() != expected {
            bail!("expected {expected} markers, found {}", markers.len());
        }
    }
    if markers.is_empty() {
        bail!("no marker files found in {}", markers_dir.display());
    }

    markers.sort_by_key(|marker| marker.tail_num.saturating_add(1));

    let mut verified_samples = 0usize;
    let mut prev_interval_end = None::<u64>;
    for marker in &markers {
        let head_id = normalize_hex_id(&marker.head_id)?;
        let _tail_id = normalize_hex_id(&marker.tail_id)?;

        let head_index_path = args
            .sink_root
            .join("blocks")
            .join("bft")
            .join("index")
            .join(marker.head_num.to_string());
        let head_index_id = normalize_hex_id(&read_trimmed(&head_index_path)?)?;
        if head_index_id != head_id {
            bail!(
                "head marker mismatch at block {}: marker={}, index={}",
                marker.head_num,
                head_id,
                head_index_id
            );
        }

        let interval_start = marker.tail_num.saturating_add(1);
        let interval_end = marker.head_num;
        if interval_start > interval_end {
            continue;
        }

        if let Some(previous_end) = prev_interval_end {
            if interval_start <= previous_end {
                bail!(
                    "marker ranges overlap: previous_end={}, current_start={}",
                    previous_end,
                    interval_start
                );
            }
        }
        prev_interval_end = Some(interval_end);

        for num in build_samples(interval_start, interval_end, args.samples_per_marker) {
            verify_sample(&args.sink_root, &source, num)
                .await
                .wrap_err_with(|| {
                    format!("verification failed in marker head {}", marker.head_num)
                })?;
            verified_samples += 1;
        }
    }

    println!(
        "OK: verified {verified_samples} sampled blocks across {} markers in {}",
        markers.len(),
        args.sink_root.display()
    );
    Ok(())
}
