use std::collections::HashMap;

use eyre::{eyre, Context, Result};
use monad_archive::prelude::*;
use opentelemetry::KeyValue;
use tracing::{debug, error, info, warn};

use crate::{
    model::{CheckerModel, Fault, FaultKind},
    rechecker::recheck_fault_chunk,
    CHUNK_SIZE,
};

/// Runs the fault fixer once across all replicas or specified replicas
/// Returns summary statistics of fixed/failed counts
pub async fn run_fixer(
    model: &CheckerModel,
    metrics: &Metrics,
    dry_run: bool,
    verify: bool,
    specific_replicas: Option<Vec<String>>,
) -> Result<(usize, usize)> {
    let replicas: Vec<String> = if let Some(replicas) = specific_replicas {
        // Validate that specified replicas exist
        for replica in &replicas {
            if !model.block_data_readers.contains_key(replica) {
                return Err(eyre!("Specified replica '{}' does not exist", replica));
            }
        }
        replicas
    } else {
        model.block_data_readers.keys().cloned().collect()
    };

    let mut total_fixed = 0;
    let mut total_failed = 0;

    for replica in replicas {
        let latest_checked = model.get_latest_checked_for_replica(&replica).await?;

        // If dry run, print header for this replica
        if dry_run {
            info!("DRY RUN: Scanning replica '{}' for faults...", replica);
        } else {
            info!("Fixing faults for replica '{}'...", replica);
        }

        let mut replica_fixed = 0;
        let mut replica_failed = 0;

        // Process chunks of CHUNK_SIZE blocks at a time
        for idx in 0..(latest_checked / CHUNK_SIZE) {
            let chunk_start = idx * CHUNK_SIZE;

            // Get current faults for this chunk
            let faults = model.get_faults_chunk(&replica, chunk_start).await?;
            if faults.is_empty() {
                continue;
            }

            debug!(
                num_faults = faults.len(),
                chunk_start, "Retrieved faults for chunk"
            );

            // Attempt to fix faults in this chunk (or simulate in dry run)
            let (fixed, failed) =
                fix_faults_in_range(model, chunk_start, &replica, metrics, dry_run, &faults)
                    .await?;

            replica_fixed += fixed;
            replica_failed += failed;

            // If verification is requested and not in dry run, verify the fixes
            if verify && !dry_run && fixed > 0 {
                info!("Verifying fixes for chunk starting at {}...", chunk_start);
                let remaining_faults = recheck_fault_chunk(model, chunk_start, &replica).await?;

                if !remaining_faults.is_empty() {
                    warn!(
                        "Verification found {} remaining faults in chunk {} after fixing",
                        remaining_faults.len(),
                        chunk_start
                    );

                    // Store the updated fault list
                    model
                        .set_faults_chunk(&replica, chunk_start, remaining_faults)
                        .await?;
                } else {
                    info!(
                        "All faults in chunk {} successfully fixed and verified",
                        chunk_start
                    );
                }
            }
        }

        // Print summary for this replica
        if dry_run {
            info!(
                "DRY RUN: Would fix {} faults for replica '{}' ({} would fail)",
                replica_fixed, replica, replica_failed
            );
        } else {
            info!(
                "Fixed {} faults for replica '{}' ({} failed)",
                replica_fixed, replica, replica_failed
            );
        }

        total_fixed += replica_fixed;
        total_failed += replica_failed;

        // Update metrics with fix attempt results (even for dry run)
        if !dry_run {
            metrics.counter_with_attrs(
                "replica_faults__fixed",
                replica_fixed as u64,
                &[KeyValue::new("replica", replica.clone())],
            );

            metrics.counter_with_attrs(
                "replica_faults__fix_failed",
                replica_failed as u64,
                &[KeyValue::new("replica", replica)],
            );
        }
    }

    // Return overall summary statistics
    Ok((total_fixed, total_failed))
}

/// Attempts to fix a specific set of faults for a replica
/// If dry_run is true, it only simulates the fixes without making changes
async fn fix_faults_in_range(
    model: &CheckerModel,
    chunk_start: u64,
    replica: &str,
    metrics: &Metrics,
    dry_run: bool,
    faults: &[Fault],
) -> Result<(usize, usize)> {
    if faults.is_empty() {
        return Ok((0, 0));
    }

    // Get the mapping of which replica has the "good" version of each block
    let good_block_mapping = &model
        .get_good_blocks(chunk_start)
        .await?
        .block_num_to_replica;

    let mut fixed_count = 0;
    let mut failed_count = 0;

    // Process each fault
    for fault in faults {
        // If dry run, just check if we would be able to fix it
        if dry_run {
            // Check if we have a good replica for this block
            if good_block_mapping.contains_key(&fault.block_num) {
                let good_replica = good_block_mapping.get(&fault.block_num).unwrap();

                // Check if the good replica actually has the block data
                match model
                    .fetch_block_data_for_replica(fault.block_num, good_replica)
                    .await
                {
                    Some(_) => {
                        debug!(
                            "DRY RUN: Would fix {} fault for block {} in replica {} (using good data from {})",
                            fault.fault.variant_name(),
                            fault.block_num,
                            replica,
                            good_replica
                        );
                        fixed_count += 1;
                    }
                    None => {
                        warn!(
                            "DRY RUN: Would fail to fix block {} in replica {} (good replica {} missing data)",
                            fault.block_num,
                            replica,
                            good_replica
                        );
                        failed_count += 1;
                    }
                }
            } else {
                warn!(
                    "DRY RUN: Would fail to fix block {} in replica {} (no good replica found)",
                    fault.block_num, replica
                );
                failed_count += 1;
            }
            continue;
        }

        // Attempt to fix the fault
        let result = fix_fault(model, fault, good_block_mapping).await;

        if let Err(e) = result {
            error!(
                "Failed to fix fault for block {} in replica {}: {:?}",
                fault.block_num, replica, e
            );

            // Report error metric
            metrics.counter_with_attrs(
                "replica_faults__fix_error",
                1,
                &[
                    KeyValue::new("replica", replica.to_owned()),
                    KeyValue::new("block_num", fault.block_num.to_string()),
                    KeyValue::new("error", e.to_string()),
                ],
            );

            // Fail fast on errors
            return Err(e.wrap_err(format!(
                "Failed to fix fault for block {} in replica {}",
                fault.block_num, replica
            )));
        } else {
            fixed_count += 1;

            // Report success metric
            metrics.counter_with_attrs(
                "replica_faults__fix_success",
                1,
                &[
                    KeyValue::new("replica", replica.to_owned()),
                    KeyValue::new("block_num", fault.block_num.to_string()),
                    KeyValue::new("fault_type", fault.fault.variant_name()),
                ],
            );
        }
    }

    Ok((fixed_count, failed_count))
}

/// Fixes a single fault by copying the good block data to the faulty replica
async fn fix_fault(
    model: &CheckerModel,
    fault: &Fault,
    good_block_mapping: &HashMap<u64, String>,
) -> Result<()> {
    // Get the replica with the good version of this block
    let good_replica = good_block_mapping
        .get(&fault.block_num)
        .ok_or_else(|| eyre!("No good replica found for block {}", fault.block_num))?;

    // Get the good block data
    let good_block_data = model
        .fetch_block_data_for_replica(fault.block_num, good_replica)
        .await
        .ok_or_else(|| {
            eyre!(
                "Failed to fetch good block data for block {}",
                fault.block_num
            )
        })?;

    // Get the archiver for the faulty replica
    let faulty_replica_archiver = model
        .block_data_readers
        .get(&fault.replica)
        .ok_or_else(|| eyre!("Block data reader not found for replica {}", fault.replica))?;

    // Extract components from the good block data
    let (block, receipts, traces) = good_block_data;

    // Archive the block
    faulty_replica_archiver
        .archive_block(block.clone())
        .await
        .with_context(|| {
            format!(
                "Failed to archive block {} for replica {}",
                fault.block_num, fault.replica
            )
        })?;

    // Archive the receipts
    faulty_replica_archiver
        .archive_receipts(receipts.clone(), fault.block_num)
        .await
        .with_context(|| {
            format!(
                "Failed to archive receipts for block {} for replica {}",
                fault.block_num, fault.replica
            )
        })?;

    // Archive the traces
    faulty_replica_archiver
        .archive_traces(traces.clone(), fault.block_num)
        .await
        .with_context(|| {
            format!(
                "Failed to archive traces for block {} for replica {}",
                fault.block_num, fault.replica
            )
        })?;

    // Update the latest uploaded block if necessary
    // This is only needed for previously missing blocks
    if matches!(fault.fault, FaultKind::MissingBlock) {
        // Get the current latest uploaded block
        let latest = faulty_replica_archiver
            .get_latest(LatestKind::Uploaded)
            .await?
            .unwrap_or(0);

        // Update the latest if this block is newer
        if latest < fault.block_num {
            faulty_replica_archiver
                .update_latest(fault.block_num, LatestKind::Uploaded)
                .await?;
        }
    }

    info!(
        "Fixed {} fault for block {} in replica {}",
        fault.fault.variant_name(),
        fault.block_num,
        fault.replica
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use monad_archive::prelude::LatestKind;

    use super::*;
    use crate::{
        checker::tests::{create_test_block_data, setup_test_model},
        model::{GoodBlocks, InconsistentBlockReason},
        rechecker::recheck_fault_chunk,
    };

    #[tokio::test]
    async fn test_fix_fault() {
        // Setup test model with memory storage
        let model = setup_test_model();
        let chunk_start = 100;
        let replica_name = "replica1";
        let good_replica_name = "replica2";
        let block_num = chunk_start + 1;

        // Create good blocks mapping
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Set replica2 as having the "good" version of the block
        good_blocks
            .block_num_to_replica
            .insert(block_num, good_replica_name.to_owned());

        // Store the good blocks mapping
        model
            .set_good_blocks(chunk_start, good_blocks)
            .await
            .unwrap();

        // Add the "good" block to replica2
        if let Some(archiver) = model.block_data_readers.get(good_replica_name) {
            // Add block data
            let (block, receipts, traces) = create_test_block_data(block_num, 1);
            archiver.archive_block(block).await.unwrap();
            archiver
                .archive_receipts(receipts, block_num)
                .await
                .unwrap();
            archiver.archive_traces(traces, block_num).await.unwrap();

            archiver
                .update_latest(block_num, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        // Create a fault record (missing block)
        let fault = Fault {
            block_num,
            replica: replica_name.to_owned(),
            fault: FaultKind::MissingBlock,
        };

        // Get the good blocks mapping
        let good_blocks = model.get_good_blocks(chunk_start).await.unwrap();

        // Fix the fault
        fix_fault(&model, &fault, &good_blocks.block_num_to_replica)
            .await
            .unwrap();

        // Verify the block was copied to the faulty replica
        let fixed_block = model
            .fetch_block_data_for_replica(block_num, replica_name)
            .await;

        assert!(
            fixed_block.is_some(),
            "Block should now be present in the previously faulty replica"
        );

        // Fetch the good block for comparison
        let good_block = model
            .fetch_block_data_for_replica(block_num, good_replica_name)
            .await
            .unwrap();

        // Verify the fixed block matches the good block
        assert_eq!(
            fixed_block.unwrap(),
            good_block,
            "Fixed block should match the good block"
        );
    }

    #[tokio::test]
    async fn test_fix_fault_chunk() {
        // Setup test model with memory storage
        let model = setup_test_model();
        let metrics = Metrics::none();
        let chunk_start = 100;
        let replica_name = "replica1";
        let good_replica_name = "replica2";

        // Create some initial faults
        let initial_faults = vec![
            Fault {
                block_num: chunk_start + 1,
                replica: replica_name.to_owned(),
                fault: FaultKind::MissingBlock,
            },
            Fault {
                block_num: chunk_start + 2,
                replica: replica_name.to_owned(),
                fault: FaultKind::InconsistentBlock(InconsistentBlockReason::Header),
            },
        ];

        // Store the initial faults
        model
            .set_faults_chunk(replica_name, chunk_start, initial_faults.clone())
            .await
            .unwrap();

        // Create good blocks mapping
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Set replica2 as having the "good" versions of the blocks
        good_blocks
            .block_num_to_replica
            .insert(chunk_start + 1, good_replica_name.to_owned());
        good_blocks
            .block_num_to_replica
            .insert(chunk_start + 2, good_replica_name.to_owned());

        // Store the good blocks mapping
        model
            .set_good_blocks(chunk_start, good_blocks)
            .await
            .unwrap();

        // Add the "good" blocks to replica2
        if let Some(archiver) = model.block_data_readers.get(good_replica_name) {
            // Add block data for both blocks
            for block_num in [chunk_start + 1, chunk_start + 2] {
                let (block, receipts, traces) = create_test_block_data(block_num, 1);
                archiver.archive_block(block).await.unwrap();
                archiver
                    .archive_receipts(receipts, block_num)
                    .await
                    .unwrap();
                archiver.archive_traces(traces, block_num).await.unwrap();
            }

            archiver
                .update_latest(chunk_start + 2, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        // Add an inconsistent block to replica1
        if let Some(archiver) = model.block_data_readers.get(replica_name) {
            // Add the second block with incorrect data
            let (block, receipts, traces) = create_test_block_data(chunk_start + 2, 2); // Different data
            archiver.archive_block(block).await.unwrap();
            archiver
                .archive_receipts(receipts, chunk_start + 2)
                .await
                .unwrap();
            archiver
                .archive_traces(traces, chunk_start + 2)
                .await
                .unwrap();

            archiver
                .update_latest(chunk_start + 2, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        let (fixed, failed) = {
            let faults = model
                .get_faults_chunk(replica_name, chunk_start)
                .await
                .unwrap();

            fix_faults_in_range(&model, chunk_start, replica_name, &metrics, false, &faults)
                .await
                .unwrap()
        };

        // Verify both faults were fixed
        assert_eq!(fixed, 2);
        assert_eq!(failed, 0);

        // Verify the blocks are now consistent
        let replica1_block1 = model
            .fetch_block_data_for_replica(chunk_start + 1, replica_name)
            .await;
        let replica2_block1 = model
            .fetch_block_data_for_replica(chunk_start + 1, good_replica_name)
            .await;

        assert!(
            replica1_block1.is_some(),
            "Previously missing block should be present"
        );
        assert_eq!(replica1_block1, replica2_block1, "Blocks should match");

        // Check that block 2 is now consistent
        let replica1_block2 = model
            .fetch_block_data_for_replica(chunk_start + 2, replica_name)
            .await;
        let replica2_block2 = model
            .fetch_block_data_for_replica(chunk_start + 2, good_replica_name)
            .await;

        assert_eq!(
            replica1_block2, replica2_block2,
            "Previously inconsistent blocks should now match"
        );

        // Run the rechecker to update the fault records
        let updated_faults = recheck_fault_chunk(&model, chunk_start, replica_name)
            .await
            .unwrap();

        // Store the updated faults
        model
            .set_faults_chunk(replica_name, chunk_start, updated_faults)
            .await
            .unwrap();

        // Recheck to ensure no faults remain
        let faults = model
            .get_faults_chunk(replica_name, chunk_start)
            .await
            .unwrap();
        assert!(faults.is_empty(), "No faults should remain after fixing");
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        // Setup test model with memory storage
        let model = setup_test_model();
        let metrics = Metrics::none();
        let chunk_start = 100;
        let replica_name = "replica1";
        let good_replica_name = "replica2";

        // Create some initial faults
        let initial_faults = vec![Fault {
            block_num: chunk_start + 1,
            replica: replica_name.to_owned(),
            fault: FaultKind::MissingBlock,
        }];

        // Store the initial faults
        model
            .set_faults_chunk(replica_name, chunk_start, initial_faults.clone())
            .await
            .unwrap();

        // Create good blocks mapping
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Set replica2 as having the "good" version of the block
        good_blocks
            .block_num_to_replica
            .insert(chunk_start + 1, good_replica_name.to_owned());

        // Store the good blocks mapping
        model
            .set_good_blocks(chunk_start, good_blocks)
            .await
            .unwrap();

        // Add the "good" block to replica2
        if let Some(archiver) = model.block_data_readers.get(good_replica_name) {
            // Add block data
            let (block, receipts, traces) = create_test_block_data(chunk_start + 1, 1);
            archiver.archive_block(block).await.unwrap();
            archiver
                .archive_receipts(receipts, chunk_start + 1)
                .await
                .unwrap();
            archiver
                .archive_traces(traces, chunk_start + 1)
                .await
                .unwrap();

            archiver
                .update_latest(chunk_start + 1, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        // Simulate in dry-run mode
        let (fixed, failed) = fix_faults_in_range(
            &model,
            chunk_start,
            replica_name,
            &metrics,
            true, // dry run
            &initial_faults,
        )
        .await
        .unwrap();

        // Verify the fault was counted as fixable
        assert_eq!(fixed, 1);
        assert_eq!(failed, 0);

        // Verify the block was NOT actually fixed
        let block_data = model
            .fetch_block_data_for_replica(chunk_start + 1, replica_name)
            .await;
        assert!(
            block_data.is_none(),
            "Block should still be missing in dry-run mode"
        );
    }
}
