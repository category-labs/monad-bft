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
    collections::{BTreeSet, HashMap},
    hash::{Hash, Hasher},
};

use eyre::Result;
use monad_archive::prelude::*;

use crate::{
    checker::fetch_block_data,
    model::{CheckerModel, Fault},
    CHUNK_SIZE,
};

/// Displays status summary of the checker model
pub async fn status(model: &CheckerModel) -> Result<()> {
    println!("Archive Checker Status");
    println!("====================");

    // Get replicas
    let replicas: Vec<&str> = model
        .block_data_readers
        .keys()
        .map(String::as_str)
        .collect();
    println!("\nReplicas: {}", replicas.len());
    for replica in &replicas {
        println!("  - {}", replica);
    }

    // Get latest checked for each replica
    println!("\nLatest Checked Blocks:");
    let mut min_checked = u64::MAX;
    let mut max_checked = 0;

    for replica in &replicas {
        let latest = model.get_latest_checked_for_replica(replica).await?;
        println!("  {}: {}", replica, latest);
        min_checked = min_checked.min(latest);
        max_checked = max_checked.max(latest);
    }

    if !replicas.is_empty() {
        println!("\n  Min: {}", min_checked);
        println!("  Max: {}", max_checked);
        println!("  Lag: {}", max_checked.saturating_sub(min_checked));
    }

    // Count fault chunks using scan_prefix
    println!("\nFault Chunks:");
    let fault_keys = model
        .store
        .scan_prefix(crate::model::FAULTS_CHUNK_PREFIX)
        .await?;

    // Group by replica
    let mut faults_by_replica: HashMap<String, usize> = HashMap::new();
    for key in &fault_keys {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() >= 3 {
            let replica = parts[1];
            *faults_by_replica.entry(replica.to_string()).or_default() += 1;
        }
    }

    println!("  Total fault chunks: {}", fault_keys.len());
    for replica in &replicas {
        let count = faults_by_replica.get(*replica).unwrap_or(&0);
        println!("  {}: {} chunks", replica, count);
    }

    // Get latest available to check
    if let Ok(latest_to_check) = model.latest_to_check().await {
        println!("\nLatest available to check: {}", latest_to_check);
        println!(
            "Blocks behind tip: {}",
            latest_to_check.saturating_sub(max_checked)
        );
    }

    Ok(())
}

/// Lists all fault ranges across replicas, collapsed to start-end ranges
pub async fn list_fault_ranges(
    model: &CheckerModel,
    summary: bool,
    start: Option<u64>,
    end: Option<u64>,
    target_replica: Option<String>,
) -> Result<()> {
    println!("Fault Summary by Replica:");
    println!("========================");

    let chunk_starts = if let Some(target_replica) = &target_replica {
        model
            .find_chunk_starts_with_faults_by_replica(target_replica, start, end)
            .await?
    } else {
        model.find_chunk_starts_with_faults().await?
    };

    let mut all_faults_by_replica =
        futures::stream::iter(chunk_starts.into_iter().filter(|chunk_start| {
            if let Some(start) = start {
                if chunk_start < &start {
                    return false;
                }
            }
            if let Some(end) = end {
                if chunk_start > &end {
                    return false;
                }
            }
            true
        }))
        .map(|chunk_start| async move { model.get_faults_chunks_all_replicas(chunk_start).await })
        .buffer_unordered(10)
        .try_fold(
            HashMap::new(),
            |mut acc: HashMap<String, Vec<Fault>>, faults| {
                for (replica, faults) in faults {
                    if let Some(target_replica) = &target_replica {
                        if &replica != target_replica {
                            continue;
                        }
                    }
                    let vec = acc.entry(replica).or_default();
                    for fault in faults {
                        if let Some(start) = start {
                            if fault.block_num < start {
                                continue;
                            }
                        }
                        if let Some(end) = end {
                            if fault.block_num > end {
                                continue;
                            }
                        }
                        vec.push(fault);
                    }
                }
                futures::future::ready(Ok(acc))
            },
        )
        .await
        .wrap_err("Failed to collect faults for chunk starts")?;

    for replica in model.block_data_readers.keys() {
        let mut all_faults = all_faults_by_replica.remove(replica).unwrap_or_default();

        if all_faults.is_empty() {
            println!("\n{}: No faults found", replica);
            continue;
        }

        // Sort faults by block number
        all_faults.sort_by_key(|f| f.block_num);

        // Group by fault type
        let mut faults_by_type: HashMap<String, Vec<u64>> = HashMap::new();
        for fault in &all_faults {
            faults_by_type
                .entry(format!("{:?}", fault.fault))
                .or_default()
                .push(fault.block_num);
        }
        let mut v = faults_by_type
            .iter()
            .map(|(k, v)| (v.len(), k))
            .collect::<Vec<_>>();
        v.sort();
        v.reverse();
        let iter = v
            .iter()
            .map(|(_, typ)| (typ, faults_by_type.get(*typ).unwrap()));

        println!("\n{}: {} total faults", replica, all_faults.len());

        // Print collapsed ranges by fault type
        for (fault_type, block_nums) in iter {
            let ranges = collapse_to_ranges(block_nums);

            let total_blocks = ranges
                .iter()
                .map(|(start, end)| end - start + 1)
                .sum::<u64>();
            if summary && ranges.len() > 10 {
                // In summary mode, show only first 10 ranges

                let chunk_starts = block_nums
                    .iter()
                    .map(|x| x / CHUNK_SIZE)
                    .collect::<BTreeSet<_>>();

                let chunk_idx_ranges =
                    collapse_to_ranges(&chunk_starts.into_iter().collect::<Vec<_>>());

                println!(
                    "  {}: ({} total blocks, showing chunks idxs with faults: {})",
                    fault_type,
                    total_blocks,
                    format_ranges(&chunk_idx_ranges)
                );
            } else {
                println!(
                    "  {}: ({} total blocks): {}",
                    fault_type,
                    total_blocks,
                    format_ranges(&ranges)
                );
            }
        }
    }

    Ok(())
}

/// Collapses a sorted list of block numbers into ranges
fn collapse_to_ranges(block_nums: &[u64]) -> Vec<(u64, u64)> {
    if block_nums.is_empty() {
        return vec![];
    }

    let mut ranges = vec![];
    let mut start = block_nums[0];
    let mut end = block_nums[0];

    for &num in &block_nums[1..] {
        if num == end + 1 {
            end = num;
        } else {
            ranges.push((start, end));
            start = num;
            end = num;
        }
    }
    ranges.push((start, end));
    ranges
}

/// Formats ranges for display
fn format_ranges(ranges: &[(u64, u64)]) -> String {
    ranges
        .iter()
        .map(|(start, end)| {
            if start == end {
                format!("{}", start)
            } else {
                format!("{}-{}", start, end)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// Re-export the OutputFormat from cli module to avoid duplication
pub use crate::cli::InspectorOutputFormat as OutputFormat;

/// Inspects a single block across all replicas
pub async fn inspect_block(
    model: &CheckerModel,
    block_num: u64,
    format: OutputFormat,
    raw: bool,
) -> Result<()> {
    println!("Block {} Inspection Report", block_num);
    println!("=========================");

    // Fetch block data from all replicas
    let replicas: Vec<&str> = model
        .block_data_readers
        .keys()
        .map(String::as_str)
        .collect();
    let data_by_block = fetch_block_data(model, std::iter::once(block_num), &replicas, 1).await;

    let block_data = data_by_block
        .get(&block_num)
        .ok_or_else(|| eyre::eyre!("No data found for block {}", block_num))?;

    // Get good block info
    let chunk_start = (block_num / CHUNK_SIZE) * CHUNK_SIZE;
    let good_blocks = model.get_good_blocks(chunk_start).await?;
    let good_replica = good_blocks.block_num_to_replica.get(&block_num);

    println!(
        "\nGood Replica: {}",
        good_replica.as_ref().map(|s| s.as_str()).unwrap_or("None")
    );

    // Get fault info for each replica
    let mut replica_faults = HashMap::new();
    for replica in &replicas {
        let faults = model.get_faults_chunk(replica, chunk_start).await?;
        if let Some(fault) = faults.iter().find(|f| f.block_num == block_num) {
            replica_faults.insert(replica.to_string(), fault.fault.clone());
        }
    }

    println!("\nReplica Status:");
    println!("---------------");

    for (replica, data_opt) in block_data {
        let status: String = match data_opt {
            None => "Missing".into(),
            Some((block, receipts, traces)) => {
                // Basic validation checks
                let is_valid = block.header.number == block_num
                    && receipts.len() == block.body.transactions.len()
                    && traces.len() == block.body.transactions.len();

                if is_valid {
                    if good_replica.as_ref() == Some(&replica) {
                        "Valid (Good)".into()
                    } else if replica_faults.contains_key(replica) {
                        format!("Inconsistent: {:?}", replica_faults[replica])
                    } else {
                        "Valid".into()
                    }
                } else {
                    let mut issues = vec![];
                    if block.header.number != block_num {
                        issues.push(format!("wrong block number: {}", block.header.number));
                    }
                    if receipts.len() != block.body.transactions.len() {
                        issues.push(format!(
                            "receipt count mismatch: {} receipts, {} txs",
                            receipts.len(),
                            block.body.transactions.len()
                        ));
                    }
                    if traces.len() != block.body.transactions.len() {
                        issues.push(format!(
                            "trace count mismatch: {} traces, {} txs",
                            traces.len(),
                            block.body.transactions.len()
                        ));
                    }
                    format!("Invalid: {}", issues.join(", "))
                }
            }
        };

        let should_print = match format {
            OutputFormat::All => true,
            OutputFormat::FaultsOnly => replica_faults.contains_key(replica),
            OutputFormat::GoodOnly => good_replica.as_ref() == Some(&replica),
            OutputFormat::Summary => false,
        };

        println!("{}: {}", replica, status);

        if should_print {
            if let Some((block, receipts, traces)) = data_opt {
                if raw {
                    println!("\n  Raw data:");
                    println!("    Block: {:?}", &block);
                    println!("    Receipts: {:?}", &receipts);
                    let call_frames = decode_traces(traces);
                    println!("    Call Frames: {:?}", call_frames);
                } else {
                    println!("\n  Block Header:");
                    println!("    Number: {}", block.header.number);
                    println!("    Hash: {:?}", block.header.hash_slow());
                    println!("    Parent: {:?}", block.header.parent_hash);
                    println!("    State Root: {:?}", block.header.state_root);
                    println!("    Transactions: {}", block.body.transactions.len());
                    println!("    Gas Used: {}", block.header.gas_used);
                    println!("    Timestamp: {}", block.header.timestamp);

                    println!("\n  Receipts: {} total", receipts.len());
                    if !receipts.is_empty() {
                        println!(
                            "    First receipt status: {:?}",
                            receipts[0].receipt.status()
                        );
                        println!(
                            "    Last receipt status: {:?}",
                            receipts.last().unwrap().receipt.status()
                        );
                    }

                    println!("\n  Traces: {} total", traces.len());
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    traces.hash(&mut hasher);
                    let traces_hash = hasher.finish();
                    println!("    Traces Hash: {:?}", traces_hash);
                }
            }
        }
    }

    // if let Some(good_replica) = good_replica {
    //     if let Some(Some((_, _, good_traces))) = block_data.get(good_replica) {
    //         if let Ok(good_traces) = decode_traces(good_traces) {
    //             for (replica, data_opt) in block_data {
    //                 if replica == good_replica {
    //                     continue;
    //                 }
    //                 if let Some((_, _, replica_traces)) = data_opt {
    //                     let Ok(replica_traces) = decode_traces(replica_traces) else {
    //                         continue;
    //                     };
    //                     print_diff(&good_traces, &replica_traces);
    //                 }
    //             }
    //         }
    //     }
    // }

    // let good_replica = good_replica.unwrap();
    // let good_traces = &block_data.get(good_replica).unwrap().as_ref().unwrap().2;
    // let good_traces = decode_traces(good_traces).unwrap();

    // for (replica, data_opt) in block_data {
    //     if replica == good_replica {
    //         continue;
    //     }
    //     if let Some((_, _, replica_traces)) = data_opt {
    //         let Ok(replica_traces) = decode_traces(replica_traces) else {
    //             continue;
    //         };
    //         print_diff(&good_traces, &replica_traces);
    //     }
    // }

    if matches!(format, OutputFormat::Summary) {
        // Print summary statistics
        let total = block_data.len();
        let missing = block_data.values().filter(|d| d.is_none()).count();
        let valid = block_data.values().filter(|d| d.is_some()).count() - replica_faults.len();
        let faulty = replica_faults.len();

        println!("\nSummary:");
        println!("  Total replicas: {}", total);
        println!("  Valid: {}", valid);
        println!("  Faulty: {}", faulty);
        println!("  Faulty (Missing): {}", missing);
    }

    Ok(())
}

#[allow(dead_code)]
fn print_diff(replica_1: &Vec<Vec<Vec<CallFrame>>>, replica_2: &Vec<Vec<Vec<CallFrame>>>) {
    println!("\n\n=======");
    dbg!(replica_1 == replica_2);
    println!(
        "level 0: replica_1.len() = {}, replica_2.len() = {}",
        replica_1.len(),
        replica_2.len()
    );

    for (i_1, (replica_1, replica_2)) in replica_1.iter().zip(replica_2.iter()).enumerate() {
        dbg!(replica_1 == replica_2);
        println!(
            "level 1, i_1 = {}: replica_1.len() = {}, replica_2.len() = {}",
            i_1,
            replica_1.len(),
            replica_2.len()
        );
        for (i_2, (replica_1, replica_2)) in replica_1.iter().zip(replica_2.iter()).enumerate() {
            // dbg!(&replica_1 == &replica_2);
            for (i_3, (frame_1, frame_2)) in replica_1.iter().zip(replica_2.iter()).enumerate() {
                if frame_1.output != frame_2.output {
                    println!(
                        "Output mismatch at idx {},{},{}: {:?} != {:?}",
                        i_1, i_2, i_3, frame_1.output, frame_2.output
                    );
                } else if frame_1 != frame_2 {
                    println!(
                        "Frame mismatch at idx {},{},{}: {:?} != {:?}",
                        i_1, i_2, i_3, frame_1, frame_2
                    );
                } else {
                    println!("Frame match at idx {},{},{}", i_1, i_2, i_3);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collapse_to_ranges() {
        assert_eq!(collapse_to_ranges(&[]), vec![]);
        assert_eq!(collapse_to_ranges(&[1]), vec![(1, 1)]);
        assert_eq!(collapse_to_ranges(&[1, 2, 3]), vec![(1, 3)]);
        assert_eq!(
            collapse_to_ranges(&[1, 2, 3, 5, 6, 10]),
            vec![(1, 3), (5, 6), (10, 10)]
        );
    }

    #[test]
    fn test_format_ranges() {
        assert_eq!(format_ranges(&[]), "");
        assert_eq!(format_ranges(&[(1, 1)]), "1");
        assert_eq!(format_ranges(&[(1, 3)]), "1-3");
        assert_eq!(format_ranges(&[(1, 3), (5, 6), (10, 10)]), "1-3, 5-6, 10");
    }
}
