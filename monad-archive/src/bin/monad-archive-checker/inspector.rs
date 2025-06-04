use std::collections::HashMap;

use eyre::Result;

use crate::{checker::fetch_block_data, model::CheckerModel, CHUNK_SIZE};

/// Lists all fault ranges across replicas, collapsed to start-end ranges
pub async fn list_fault_ranges(model: &CheckerModel) -> Result<()> {
    println!("Fault Summary by Replica:");
    println!("========================");

    for replica in model.block_data_readers.keys() {
        let latest_checked = model.get_latest_checked_for_replica(replica).await?;
        let mut all_faults = Vec::new();

        // Collect all faults for this replica
        for idx in 0..=(latest_checked / CHUNK_SIZE) {
            let chunk_start = idx * CHUNK_SIZE;
            let faults = model.get_faults_chunk(replica, chunk_start).await?;
            all_faults.extend(faults);
        }

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

        println!("\n{}: {} total faults", replica, all_faults.len());

        // Print collapsed ranges by fault type
        for (fault_type, block_nums) in faults_by_type {
            let ranges = collapse_to_ranges(&block_nums);
            println!("  {}: {}", fault_type, format_ranges(&ranges));
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
    print_data: bool,
) -> Result<()> {
    println!("Block {} Inspection Report", block_num);
    println!("=========================");

    // Fetch block data from all replicas
    let replicas: Vec<&str> = model
        .block_data_readers
        .keys()
        .map(String::as_str)
        .collect();
    let data_by_block = fetch_block_data(model, std::iter::once(block_num), &replicas).await;

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
        let status = match data_opt {
            None => "Missing".to_string(),
            Some((block, receipts, traces)) => {
                // Basic validation checks
                let is_valid = block.header.number == block_num
                    && receipts.len() == block.body.transactions.len()
                    && traces.len() == block.body.transactions.len();

                if is_valid {
                    if good_replica.as_ref() == Some(&replica) {
                        "Valid (Good)".to_string()
                    } else if replica_faults.contains_key(replica) {
                        format!("Inconsistent: {:?}", replica_faults[replica])
                    } else {
                        "Valid".to_string()
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

        if should_print && print_data {
            if let Some((block, receipts, traces)) = data_opt {
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
            }
        }
    }

    if matches!(format, OutputFormat::Summary) {
        // Print summary statistics
        let total = block_data.len();
        let missing = block_data.values().filter(|d| d.is_none()).count();
        let valid = block_data.values().filter(|d| d.is_some()).count() - replica_faults.len();
        let faulty = replica_faults.len();

        println!("\nSummary:");
        println!("  Total replicas: {}", total);
        println!("  Missing: {}", missing);
        println!("  Valid: {}", valid);
        println!("  Faulty: {}", faulty);
    }

    Ok(())
}

/// Lists blocks with faults in a given range
pub async fn list_faulty_blocks(
    model: &CheckerModel,
    start_block: Option<u64>,
    end_block: Option<u64>,
) -> Result<()> {
    let mut all_faulty_blocks = std::collections::HashSet::new();

    for replica in model.block_data_readers.keys() {
        let latest_checked = model.get_latest_checked_for_replica(replica).await?;

        for idx in 0..=(latest_checked / CHUNK_SIZE) {
            let chunk_start = idx * CHUNK_SIZE;
            let faults = model.get_faults_chunk(replica, chunk_start).await?;

            for fault in faults {
                if let Some(start) = start_block {
                    if fault.block_num < start {
                        continue;
                    }
                }
                if let Some(end) = end_block {
                    if fault.block_num > end {
                        continue;
                    }
                }
                all_faulty_blocks.insert(fault.block_num);
            }
        }
    }

    let mut blocks: Vec<u64> = all_faulty_blocks.into_iter().collect();
    blocks.sort();

    println!("Faulty blocks in range:");
    let ranges = collapse_to_ranges(&blocks);
    println!("{}", format_ranges(&ranges));
    println!("Total: {} blocks", blocks.len());

    Ok(())
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
