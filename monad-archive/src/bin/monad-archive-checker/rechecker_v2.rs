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

use eyre::{Context, Result};
use monad_archive::prelude::*;
use opentelemetry::KeyValue;
use tokio::time::interval;

use crate::{
    checker::{fetch_block_data, process_blocks, store_checking_results},
    model::{CheckerModel, Fault, FaultKind, GoodBlocks},
    CHUNK_SIZE,
};

/// Worker function that periodically rechecks previously found faults by reprocessing
/// entire chunks from scratch using the original checker logic
pub async fn rechecker_v2_worker(
    recheck_freq: Duration,
    model: CheckerModel,
    metrics: Metrics,
    concurrency: usize,
) -> Result<()> {
    info!(
        "Starting rechecker v2 worker with frequency {:?}",
        recheck_freq
    );
    let mut interval = interval(recheck_freq);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        info!("Starting recheck cycle for all replicas");
        recheck_all_faults(&model, &metrics, false, None, None, concurrency).await?;
        info!("Recheck cycle completed, waiting for next interval");

        interval.tick().await;
    }
}

/// Worker function for standalone rechecker v2 with optional parameters
#[allow(clippy::too_many_arguments)]
pub async fn rechecker_v2_standalone(
    recheck_freq: Duration,
    model: CheckerModel,
    metrics: Metrics,
    dry_run: bool,
    start_block: Option<u64>,
    end_block: Option<u64>,
    force_recheck: bool,
    worker_mode: bool,
    concurrency: usize,
) -> Result<()> {
    info!(
        "Starting standalone rechecker v2 with frequency {:?}, dry_run: {}, start: {:?}, end: {:?}, force_recheck: {}, worker_mode: {}",
        recheck_freq, dry_run, start_block, end_block, force_recheck, worker_mode
    );

    if worker_mode {
        // Run continuously as a worker
        let mut interval = interval(recheck_freq);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            info!("Starting recheck cycle");
            if force_recheck {
                recheck_all_chunks(
                    &model,
                    &metrics,
                    dry_run,
                    start_block,
                    end_block,
                    concurrency,
                )
                .await?;
            } else {
                recheck_all_faults(
                    &model,
                    &metrics,
                    dry_run,
                    start_block,
                    end_block,
                    concurrency,
                )
                .await?;
            }
            info!("Recheck cycle completed, waiting for next interval");
        }
    } else {
        // Run once and exit
        info!("Running single recheck cycle");
        if force_recheck {
            recheck_all_chunks(
                &model,
                &metrics,
                dry_run,
                start_block,
                end_block,
                concurrency,
            )
            .await?;
        } else {
            recheck_all_faults(
                &model,
                &metrics,
                dry_run,
                start_block,
                end_block,
                concurrency,
            )
            .await?;
        }
        info!("Recheck completed, exiting");
        Ok(())
    }
}

/// Rechecks all chunks that contain faults by reprocessing them from scratch
async fn recheck_all_faults(
    model: &CheckerModel,
    metrics: &Metrics,
    dry_run: bool,
    start_block: Option<u64>,
    end_block: Option<u64>,
    concurrency: usize,
) -> Result<()> {
    // Collect all chunks that have faults across all replicas
    let mut fault_chunks = collect_fault_chunk_starts(model, start_block, end_block)
        .await?
        .peekable();

    if fault_chunks.peek().is_none() {
        info!("No fault chunks found to recheck");
    } else {
        info!("Found chunks with faults to recheck");
    }

    // Process each fault chunk
    let new_faults_by_replica = futures::stream::iter(fault_chunks)
        .map(async |chunk_start| {
            info!(chunk_start, dry_run, "Rechecking fault chunk");
            let (new_faults_by_replica_chunk, _) =
                recheck_chunk_from_scratch(model, chunk_start, dry_run, concurrency)
                    .await
                    .unwrap();
            new_faults_by_replica_chunk
            // futures::stream::iter(new_faults_by_replica_chunk)
        })
        .buffered(2)
        .flat_map(futures::stream::iter)
        .collect()
        .await;

    // Update metrics after all chunks are rechecked (skip in dry run)
    if !dry_run {
        update_fault_metrics(metrics, &new_faults_by_replica)?;
    }

    Ok(())
}

/// Rechecks all chunks in the specified range, regardless of whether they have faults
async fn recheck_all_chunks(
    model: &CheckerModel,
    metrics: &Metrics,
    dry_run: bool,
    start_block: Option<u64>,
    end_block: Option<u64>,
    concurrency: usize,
) -> Result<()> {
    // Collect all chunks in the specified range
    let all_chunks = collect_all_chunk_idxs(model, start_block, end_block).await?;

    if all_chunks.is_empty() {
        info!("No chunks found to recheck in the specified range");
        return Ok(());
    }

    // Process each chunk
    let mut new_faults_by_replica = HashMap::new();
    let mut old_faults_by_replica = HashMap::new();
    for chunk_idx in all_chunks {
        let chunk_start = chunk_idx * CHUNK_SIZE;
        info!(chunk_start, dry_run, "Rechecking chunk");
        let (new_faults_by_replica_chunk, old_faults_by_replica_chunk) =
            recheck_chunk_from_scratch(model, chunk_start, dry_run, concurrency).await?;
        new_faults_by_replica.extend(new_faults_by_replica_chunk);
        old_faults_by_replica.extend(old_faults_by_replica_chunk);
    }

    // Update metrics after all chunks are rechecked (skip in dry run)
    if !dry_run {
        update_fault_metrics(metrics, &new_faults_by_replica)?;
    }

    Ok(())
}

/// Collects all chunks in the specified range, regardless of fault status
async fn collect_all_chunk_idxs(
    model: &CheckerModel,
    start_block: Option<u64>,
    end_block: Option<u64>,
) -> Result<RangeInclusive<u64>> {
    // Get the minimum latest checked across all replicas to determine the upper bound
    let min_latest_checked = model.min_latest_checked().await?;

    // Filter chunks based on block range if provided
    let start_chunk = start_block.map(|b| b / CHUNK_SIZE).unwrap_or(0);
    let end_chunk = end_block
        .map(|b| b / CHUNK_SIZE)
        .unwrap_or(min_latest_checked / CHUNK_SIZE);

    // Ensure we don't go beyond what has been checked
    let effective_end_chunk = end_chunk.min(min_latest_checked / CHUNK_SIZE);

    // Collect all chunks in the range
    let chunks = RangeInclusive::new(start_chunk, effective_end_chunk);

    println!(
        "Collected {} chunks to recheck (from block {} to {})",
        chunks.end() - chunks.start() + 1,
        start_chunk * CHUNK_SIZE,
        (effective_end_chunk + 1) * CHUNK_SIZE - 1
    );

    Ok(chunks)
}

/// Collects all unique chunk starts that have faults across all replicas
async fn collect_fault_chunk_starts(
    model: &CheckerModel,
    start_block: Option<u64>,
    end_block: Option<u64>,
) -> Result<impl Iterator<Item = u64> + Send> {
    let chunks_in_range = collect_all_chunk_idxs(model, start_block, end_block).await?;

    debug!(
        "Checking {} chunks across {} replicas for faults",
        chunks_in_range.end() - chunks_in_range.start() + 1,
        model.block_data_readers.len()
    );

    let fault_chunk_starts = model.find_chunk_starts_with_faults().await?;
    Ok(chunks_in_range
        .map(|chunk_idx| chunk_idx * CHUNK_SIZE)
        .filter(move |chunk_start| fault_chunk_starts.contains(chunk_start)))
}

/// Rechecks a chunk from scratch using the original checker logic
pub async fn recheck_chunk_from_scratch(
    model: &CheckerModel,
    chunk_start: u64,
    dry_run: bool,
    concurrency: usize,
) -> Result<(HashMap<String, Vec<Fault>>, HashMap<String, Vec<Fault>>)> {
    let end_block = chunk_start + CHUNK_SIZE - 1;

    info!(
        chunk_start,
        end_block, dry_run, "Rechecking chunk from scratch"
    );

    // Fetch block data from all replicas using the same logic as the checker
    let replicas = model
        .block_data_readers
        .keys()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    let data_by_block_num =
        fetch_block_data(model, chunk_start..=end_block, &replicas, concurrency).await;

    // Process blocks to find faults and good blocks using original checker logic
    let (new_faults_by_replica, new_good_blocks) =
        process_blocks(&data_by_block_num, chunk_start, end_block);

    // Get old results for comparison
    let old_faults_by_replica = model.get_faults_chunks_all_replicas(chunk_start).await?;

    let old_good_blocks = model.get_good_blocks(chunk_start).await?;

    // Compare with old results and log differences
    let has_changes = log_recheck_differences_detailed(
        chunk_start,
        &old_faults_by_replica,
        &new_faults_by_replica,
        &old_good_blocks,
        &new_good_blocks,
    );

    if dry_run {
        if has_changes {
            info!(
                chunk_start,
                "DRY RUN: Would update chunk with changes shown above"
            );
        } else {
            info!(chunk_start, "DRY RUN: No changes found for chunk");
        }
    } else {
        // Always backup old results before overwriting
        if has_changes {
            backup_old_results(model, chunk_start, &old_faults_by_replica, &old_good_blocks)
                .await?;
        }

        // Always store the new results to ensure fault clearing logic runs
        // This is critical for clearing fixed faults from S3 storage
        store_checking_results(model, chunk_start, &new_faults_by_replica, new_good_blocks)
            .await?;

        if has_changes {
            info!(chunk_start, "Chunk recheck completed with updates");
        } else {
            info!(chunk_start, "Chunk recheck completed with no changes");
        }
    }

    Ok((new_faults_by_replica, old_faults_by_replica))
}

/// Backs up old fault and good block data before rechecking
async fn backup_old_results(
    model: &CheckerModel,
    chunk_start: u64,
    old_faults_by_replica: &HashMap<String, Vec<Fault>>,
    old_good_blocks: &GoodBlocks,
) -> Result<()> {
    // Backup old faults for each replica
    for (replica, old_faults) in old_faults_by_replica {
        if !old_faults.is_empty() {
            // Store old faults with a special key indicating they are backups
            let backup_key = format!("old_faults_chunk/{}/{}", replica, chunk_start);
            let data =
                serde_json::to_vec(&old_faults).wrap_err("Failed to serialize old faults")?;
            model.store.put(&backup_key, data).await?;

            debug!(
                replica = %replica,
                chunk_start,
                fault_count = old_faults.len(),
                "Backed up old faults"
            );
        }
    }

    // Backup old good blocks
    if !old_good_blocks.block_num_to_replica.is_empty() {
        let backup_key = format!("old_good_blocks/{}", chunk_start);
        let data =
            serde_json::to_vec(&old_good_blocks).wrap_err("Failed to serialize old good blocks")?;
        model.store.put(&backup_key, data).await?;

        debug!(
            chunk_start,
            block_count = old_good_blocks.block_num_to_replica.len(),
            "Backed up old good blocks"
        );
    }

    Ok(())
}

/// Logs detailed differences between old and new recheck results and returns if there are changes
fn log_recheck_differences_detailed(
    chunk_start: u64,
    old_faults_by_replica: &HashMap<String, Vec<Fault>>,
    new_faults_by_replica: &HashMap<String, Vec<Fault>>,
    old_good_blocks: &GoodBlocks,
    new_good_blocks: &GoodBlocks,
) -> bool {
    let mut has_changes = false;

    // Check for fault differences
    let all_replicas: HashSet<&String> = old_faults_by_replica
        .keys()
        .chain(new_faults_by_replica.keys())
        .collect();

    for replica in all_replicas {
        let old_faults = old_faults_by_replica
            .get(replica)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let new_faults = new_faults_by_replica
            .get(replica)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        if old_faults.len() != new_faults.len() {
            has_changes = true;
            info!(
                replica = %replica,
                chunk_start,
                old_fault_count = old_faults.len(),
                new_fault_count = new_faults.len(),
                "Fault count changed after recheck"
            );
        }

        // Find fixed faults (in old but not in new)
        let old_fault_set: HashSet<(u64, FaultKind)> = old_faults
            .iter()
            .map(|f| (f.block_num, f.fault.clone()))
            .collect();

        let new_fault_set: HashSet<(u64, FaultKind)> = new_faults
            .iter()
            .map(|f| (f.block_num, f.fault.clone()))
            .collect();

        let fixed_faults: Vec<_> = old_fault_set.difference(&new_fault_set).collect();
        let new_issues: Vec<_> = new_fault_set.difference(&old_fault_set).collect();

        if !fixed_faults.is_empty() {
            has_changes = true;
            info!(
                replica = %replica,
                chunk_start,
                fixed_count = fixed_faults.len(),
                "Faults fixed after recheck"
            );

            for (block_num, fault_kind) in fixed_faults.iter().take(10) {
                info!(
                    replica = %replica,
                    block_num,
                    fault_kind = ?fault_kind,
                    "Fixed fault"
                );
            }
        }

        if !new_issues.is_empty() {
            has_changes = true;
            warn!(
                replica = %replica,
                chunk_start,
                new_issue_count = new_issues.len(),
                "New faults found after recheck"
            );

            for (block_num, fault_kind) in new_issues.iter().take(10) {
                warn!(
                    replica = %replica,
                    block_num,
                    fault_kind = ?fault_kind,
                    "New fault found"
                );
            }
        }
    }

    // Check for good block differences
    let old_good_set: HashSet<_> = old_good_blocks.block_num_to_replica.iter().collect();
    let new_good_set: HashSet<_> = new_good_blocks.block_num_to_replica.iter().collect();

    if old_good_set != new_good_set {
        has_changes = true;
        info!(
            chunk_start,
            old_good_count = old_good_blocks.block_num_to_replica.len(),
            new_good_count = new_good_blocks.block_num_to_replica.len(),
            "Good blocks mapping changed after recheck"
        );
    }

    has_changes
}

/// Updates fault metrics after rechecking
fn update_fault_metrics(
    metrics: &Metrics,
    faults_by_replica: &HashMap<String, Vec<Fault>>,
) -> Result<()> {
    for (replica, total_faults) in faults_by_replica {
        // Update total faults metric
        metrics.periodic_gauge_with_attrs(
            MetricNames::REPLICA_FAULTS_TOTAL,
            total_faults.len() as u64,
            vec![KeyValue::new("replica", replica.to_owned())],
        );

        // Group faults by kind and update per-kind metrics
        let mut fault_counts: HashMap<String, u64> = HashMap::new();
        for fault in total_faults {
            *fault_counts
                .entry(fault.fault.metric_name().to_string())
                .or_insert(0) += 1;
        }

        for (fault_kind, count) in fault_counts {
            metrics.periodic_gauge_with_attrs(
                MetricNames::REPLICA_FAULTS_BY_KIND,
                count,
                vec![
                    KeyValue::new("replica", replica.to_owned()),
                    KeyValue::new("kind", fault_kind),
                ],
            );
        }

        info!(
            replica = %replica,
            total_faults = total_faults.len(),
            missing = total_faults.iter()
                .filter(|f| matches!(f.fault, FaultKind::MissingBlock))
                .count(),
            inconsistent = total_faults.iter()
                .filter(|f| matches!(f.fault, FaultKind::InconsistentBlock(_)))
                .count(),
            "Updated fault metrics for replica"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use monad_archive::prelude::LatestKind;

    use super::*;
    use crate::{
        checker::tests::{create_test_block_data, create_test_block_data_range, setup_test_model},
        model::{GoodBlocks, InconsistentBlockReason},
    };

    #[tokio::test]
    async fn test_rechecker_v2_full_recheck() {
        // Setup test model
        let model = setup_test_model(2);
        let chunk_start = 100;

        // Create initial faults for replica1
        let initial_faults = vec![
            Fault {
                block_num: chunk_start + 1,
                replica: "replica1".to_owned(),
                fault: FaultKind::MissingBlock,
            },
            Fault {
                block_num: chunk_start + 2,
                replica: "replica1".to_owned(),
                fault: FaultKind::InconsistentBlock(InconsistentBlockReason::Header),
            },
        ];

        model
            .set_faults_chunk("replica1", chunk_start, initial_faults)
            .await
            .unwrap();

        // Set good blocks mapping
        let mut good_blocks = GoodBlocks::default();
        good_blocks
            .block_num_to_replica
            .insert(chunk_start + 1, "replica2".to_owned());
        good_blocks
            .block_num_to_replica
            .insert(chunk_start + 2, "replica2".to_owned());

        model
            .set_good_blocks(chunk_start, good_blocks)
            .await
            .unwrap();

        // Add good data to replica2
        let blocks = create_test_block_data_range(chunk_start + 1, [1, 2]);
        if let Some(archiver) = model.block_data_readers.get("replica2") {
            for block_num in [chunk_start + 1, chunk_start + 2] {
                let (block, receipts, traces) = blocks.get(&block_num).unwrap().clone();
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

        // Set latest checked for replica1
        model
            .set_latest_checked_for_replica("replica1", chunk_start + CHUNK_SIZE - 1)
            .await
            .unwrap();

        // Now fix the first block in replica1 (was missing)
        if let Some(archiver) = model.block_data_readers.get("replica1") {
            let (block, receipts, traces) = create_test_block_data(chunk_start + 1, 1, None);
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

        // Run rechecker v2
        recheck_chunk_from_scratch(&model, chunk_start, false, 1)
            .await
            .unwrap();

        // Verify old faults were backed up
        let backup_key = format!("old_faults_chunk/{}/{}", "replica1", chunk_start);
        let backed_up_data = model.store.get(&backup_key).await.unwrap().unwrap();
        let backed_up_faults: Vec<Fault> = serde_json::from_slice(&backed_up_data).unwrap();
        assert_eq!(backed_up_faults.len(), 2);

        // Verify new faults - when rechecking the entire chunk, all blocks except the ones we added will be missing
        let new_faults = model
            .get_faults_chunk("replica1", chunk_start)
            .await
            .unwrap();
        // We should have CHUNK_SIZE - 1 faults (all blocks are missing except block 101 which we fixed)
        assert_eq!(new_faults.len(), (CHUNK_SIZE - 1) as usize);

        // The block we fixed (chunk_start + 1) should not be in the faults
        assert!(!new_faults.iter().any(|f| f.block_num == chunk_start + 1));

        // The block that's still missing (chunk_start + 2) should be in the faults
        assert!(new_faults
            .iter()
            .any(|f| f.block_num == chunk_start + 2 && matches!(f.fault, FaultKind::MissingBlock)));
    }

    #[tokio::test]
    async fn test_collect_fault_chunks() {
        let model = setup_test_model(3);

        // Add faults to different chunks for different replicas
        model
            .set_faults_chunk(
                "replica1",
                0,
                vec![Fault {
                    block_num: 1,
                    replica: "replica1".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        model
            .set_faults_chunk(
                "replica2",
                1000,
                vec![Fault {
                    block_num: 1001,
                    replica: "replica2".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        model
            .set_faults_chunk(
                "replica1",
                2000,
                vec![Fault {
                    block_num: 2001,
                    replica: "replica1".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        // Set latest checked
        model
            .set_latest_checked_for_replica("replica1", 2999)
            .await
            .unwrap();
        model
            .set_latest_checked_for_replica("replica2", 1999)
            .await
            .unwrap();
        model
            .set_latest_checked_for_replica("replica3", 999)
            .await
            .unwrap(); // replica3 hasn't checked past first chunk

        // Collect fault chunks
        let chunks = collect_fault_chunk_starts(&model, None, None)
            .await
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(chunks, vec![0]);
    }

    #[tokio::test]
    async fn test_rechecker_v2_dry_run() {
        // Setup test model
        let model = setup_test_model(3);
        let chunk_start = 100;

        // Create initial faults for replica1
        let initial_faults = vec![Fault {
            block_num: chunk_start + 1,
            replica: "replica1".to_owned(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", chunk_start, initial_faults.clone())
            .await
            .unwrap();

        // Set latest checked
        model
            .set_latest_checked_for_replica("replica1", chunk_start + CHUNK_SIZE - 1)
            .await
            .unwrap();

        // Add the missing block to replica1 (fix the fault)
        if let Some(archiver) = model.block_data_readers.get("replica1") {
            let (block, receipts, traces) = create_test_block_data(chunk_start + 1, 1, None);
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

        // Run rechecker v2 in DRY RUN mode
        recheck_chunk_from_scratch(&model, chunk_start, true, 10)
            .await
            .unwrap();

        // Verify that old faults were NOT backed up (dry run doesn't backup)
        let backup_key = format!("old_faults_chunk/{}/{}", "replica1", chunk_start);
        let backed_up_data = model.store.get(&backup_key).await.unwrap();
        assert!(backed_up_data.is_none(), "Dry run should not backup data");

        // Verify that faults were NOT updated (still has the original fault)
        let current_faults = model
            .get_faults_chunk("replica1", chunk_start)
            .await
            .unwrap();
        assert_eq!(current_faults.len(), 1);
        assert_eq!(current_faults[0].block_num, chunk_start + 1);
    }

    #[tokio::test]
    async fn test_rechecker_v2_no_backup_when_no_changes() {
        // Setup test model
        let model = setup_test_model(3);
        let chunk_start = 200;

        // Create faults for ALL blocks in the chunk for ALL replicas (simulating a fully checked chunk)
        for replica in ["replica1", "replica2", "replica3"] {
            let mut faults = Vec::new();
            for i in 0..CHUNK_SIZE {
                faults.push(Fault {
                    block_num: chunk_start + i,
                    replica: replica.to_owned(),
                    fault: FaultKind::MissingBlock,
                });
            }

            model
                .set_faults_chunk(replica, chunk_start, faults)
                .await
                .unwrap();

            model
                .set_latest_checked_for_replica(replica, chunk_start + CHUNK_SIZE - 1)
                .await
                .unwrap();
        }

        // Set empty good blocks (no blocks are good since all are missing)
        let good_blocks = GoodBlocks::default();
        model
            .set_good_blocks(chunk_start, good_blocks)
            .await
            .unwrap();

        // Run rechecker v2 (all blocks are still missing, so no changes)
        recheck_chunk_from_scratch(&model, chunk_start, false, 10)
            .await
            .unwrap();

        // Verify NO backup was created since there were no changes
        let backup_key = format!("old_faults_chunk/{}/{}", "replica1", chunk_start);
        let backed_up_data = model.store.get(&backup_key).await.unwrap();
        assert!(
            backed_up_data.is_none(),
            "Should not backup when no changes"
        );
    }

    #[tokio::test]
    async fn test_recheck_all_faults_with_block_range() {
        let model = setup_test_model(1);

        // Add faults to different chunks
        model
            .set_faults_chunk(
                "replica1",
                0,
                vec![Fault {
                    block_num: 1,
                    replica: "replica1".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        model
            .set_faults_chunk(
                "replica1",
                1000,
                vec![Fault {
                    block_num: 1001,
                    replica: "replica1".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        model
            .set_faults_chunk(
                "replica1",
                2000,
                vec![Fault {
                    block_num: 2001,
                    replica: "replica1".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        model
            .set_faults_chunk(
                "replica1",
                3000,
                vec![Fault {
                    block_num: 3001,
                    replica: "replica1".to_owned(),
                    fault: FaultKind::MissingBlock,
                }],
            )
            .await
            .unwrap();

        // Set latest checked
        model
            .set_latest_checked_for_replica("replica1", 3999)
            .await
            .unwrap();
        model
            .set_latest_checked_for_replica("replica2", 3999)
            .await
            .unwrap();
        model
            .set_latest_checked_for_replica("replica3", 3999)
            .await
            .unwrap();

        // Test 1: Recheck only blocks 1000-2500 (should process chunks 1000 and 2000)
        // We'll do a dry run and count how many chunks would be processed

        // Mock the processing by collecting which chunks would be processed
        let fault_chunks = collect_fault_chunk_starts(&model, None, None)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        let mut filtered_chunks = fault_chunks.clone();
        println!("fault_chunks: {:?}", fault_chunks);

        // Apply start filter
        let start_chunk = (1000 / CHUNK_SIZE) * CHUNK_SIZE; // 1000
        filtered_chunks.retain(|&chunk| chunk >= start_chunk);

        // Apply end filter
        let end_chunk = (2500 / CHUNK_SIZE) * CHUNK_SIZE; // 2000
        filtered_chunks.retain(|&chunk| chunk <= end_chunk);

        assert_eq!(filtered_chunks, vec![1000, 2000]);

        // Test 2: Start block only (should process chunks >= 2000)
        let mut filtered_chunks2 = fault_chunks.clone();
        let start_chunk2 = (2000 / CHUNK_SIZE) * CHUNK_SIZE;
        filtered_chunks2.retain(|&chunk| chunk >= start_chunk2);

        assert_eq!(filtered_chunks2, vec![2000, 3000]);

        // Test 3: End block only (should process chunks <= 1000)
        let mut filtered_chunks3 = fault_chunks;
        let end_chunk3 = (1500 / CHUNK_SIZE) * CHUNK_SIZE;
        filtered_chunks3.retain(|&chunk| chunk <= end_chunk3);

        assert_eq!(filtered_chunks3, vec![0, 1000]);
    }

    #[tokio::test]
    async fn test_log_recheck_differences_detailed() {
        let chunk_start = 100;

        // Test case 1: No changes
        let old_faults_by_replica = HashMap::new();
        let new_faults_by_replica = HashMap::new();
        let old_good_blocks = GoodBlocks::default();
        let new_good_blocks = GoodBlocks::default();

        let has_changes = log_recheck_differences_detailed(
            chunk_start,
            &old_faults_by_replica,
            &new_faults_by_replica,
            &old_good_blocks,
            &new_good_blocks,
        );
        assert!(!has_changes);

        // Test case 2: Fault fixed
        let mut old_faults = HashMap::new();
        old_faults.insert(
            "replica1".to_string(),
            vec![Fault {
                block_num: chunk_start + 1,
                replica: "replica1".to_owned(),
                fault: FaultKind::MissingBlock,
            }],
        );
        let new_faults = HashMap::new();

        let has_changes = log_recheck_differences_detailed(
            chunk_start,
            &old_faults,
            &new_faults,
            &old_good_blocks,
            &new_good_blocks,
        );
        assert!(has_changes);

        // Test case 3: New fault found
        let old_faults = HashMap::new();
        let mut new_faults = HashMap::new();
        new_faults.insert(
            "replica1".to_string(),
            vec![Fault {
                block_num: chunk_start + 2,
                replica: "replica1".to_owned(),
                fault: FaultKind::MissingBlock,
            }],
        );

        let has_changes = log_recheck_differences_detailed(
            chunk_start,
            &old_faults,
            &new_faults,
            &old_good_blocks,
            &new_good_blocks,
        );
        assert!(has_changes);

        // Test case 4: Good blocks changed
        let mut old_good_blocks = GoodBlocks::default();
        old_good_blocks
            .block_num_to_replica
            .insert(chunk_start + 1, "replica1".to_string());

        let mut new_good_blocks = GoodBlocks::default();
        new_good_blocks
            .block_num_to_replica
            .insert(chunk_start + 1, "replica2".to_string());

        let has_changes = log_recheck_differences_detailed(
            chunk_start,
            &HashMap::new(),
            &HashMap::new(),
            &old_good_blocks,
            &new_good_blocks,
        );
        assert!(has_changes);
    }

    #[tokio::test]
    async fn test_rechecker_v2_clears_fixed_faults() {
        // This test demonstrates the bug where fixed faults are not cleared from S3
        let model = setup_test_model(3);
        let chunk_start = 300;

        // Setup initial state: replica1 and replica2 have faults
        let initial_faults_replica1 = vec![
            Fault {
                block_num: chunk_start + 1,
                replica: "replica1".to_owned(),
                fault: FaultKind::MissingBlock,
            },
            Fault {
                block_num: chunk_start + 2,
                replica: "replica1".to_owned(),
                fault: FaultKind::MissingBlock,
            },
        ];

        let initial_faults_replica2 = vec![Fault {
            block_num: chunk_start + 1,
            replica: "replica2".to_owned(),
            fault: FaultKind::MissingBlock,
        }];

        // Store initial faults
        model
            .set_faults_chunk("replica1", chunk_start, initial_faults_replica1.clone())
            .await
            .unwrap();
        model
            .set_faults_chunk("replica2", chunk_start, initial_faults_replica2.clone())
            .await
            .unwrap();

        // Set latest checked for all replicas
        for replica in ["replica1", "replica2", "replica3"] {
            model
                .set_latest_checked_for_replica(replica, chunk_start + CHUNK_SIZE - 1)
                .await
                .unwrap();
        }

        // Now fix the issues in replica1 by adding ALL blocks in the chunk
        let blocks =
            create_test_block_data_range(chunk_start, std::iter::repeat_n(1, CHUNK_SIZE as usize));
        if let Some(archiver) = model.block_data_readers.get("replica1") {
            for block_num in chunk_start..(chunk_start + CHUNK_SIZE) {
                let (block, receipts, traces) = blocks.get(&block_num).unwrap().clone();
                archiver.archive_block(block).await.unwrap();
                archiver
                    .archive_receipts(receipts, block_num)
                    .await
                    .unwrap();
                archiver.archive_traces(traces, block_num).await.unwrap();
            }
            archiver
                .update_latest(chunk_start + CHUNK_SIZE - 1, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        // Add blocks to replica2 and replica3 as well so there's consensus
        for replica in ["replica2", "replica3"] {
            if let Some(archiver) = model.block_data_readers.get(replica) {
                for block_num in chunk_start..(chunk_start + CHUNK_SIZE) {
                    let (block, receipts, traces) = blocks.get(&block_num).unwrap().clone();
                    archiver.archive_block(block).await.unwrap();
                    archiver
                        .archive_receipts(receipts, block_num)
                        .await
                        .unwrap();
                    archiver.archive_traces(traces, block_num).await.unwrap();
                }
                archiver
                    .update_latest(chunk_start + CHUNK_SIZE - 1, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }
        }

        // Run rechecker v2
        recheck_chunk_from_scratch(&model, chunk_start, false, 10)
            .await
            .unwrap();

        // Check that replica1's faults have been cleared
        let replica1_faults = model
            .get_faults_chunk("replica1", chunk_start)
            .await
            .unwrap();

        assert!(
            replica1_faults.is_empty(),
            "Replica1 should have no faults after fixing the missing blocks, but found: {:?}",
            replica1_faults
        );

        // Replica2 should also have no faults now
        let replica2_faults = model
            .get_faults_chunk("replica2", chunk_start)
            .await
            .unwrap();
        assert!(replica2_faults.is_empty());
    }

    #[tokio::test]
    async fn test_collect_all_chunks() {
        let model = setup_test_model(3);

        // Set latest checked for all replicas
        model
            .set_latest_checked_for_replica("replica1", 5999)
            .await
            .unwrap();
        model
            .set_latest_checked_for_replica("replica2", 4999)
            .await
            .unwrap();
        model
            .set_latest_checked_for_replica("replica3", 3999)
            .await
            .unwrap();

        // Test 1: No range specified - should collect all chunks up to min latest checked (3999)
        let chunks = collect_all_chunk_idxs(&model, None, None)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks, vec![0, 1, 2, 3]);

        // Test 2: With start and end range
        let chunks = collect_all_chunk_idxs(&model, Some(1500), Some(3500))
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks, vec![1, 2, 3]);

        // Test 3: End block beyond latest checked - should cap at min latest checked
        let chunks = collect_all_chunk_idxs(&model, Some(2000), Some(10000))
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks, vec![2, 3]);

        // Test 4: Exact chunk boundaries
        let chunks = collect_all_chunk_idxs(&model, Some(1000), Some(1999))
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks, vec![1]);
    }

    #[tokio::test]
    async fn test_force_recheck_all_chunks() {
        // Setup test model
        let model = setup_test_model(3);
        let metrics = Metrics::none();

        // Set latest checked for all replicas
        for replica in ["replica1", "replica2", "replica3"] {
            model
                .set_latest_checked_for_replica(replica, 2999)
                .await
                .unwrap();
        }

        // Add complete data for all replicas for chunks 0, 1000, 2000
        // NOTE: this takes ~2 secs bc generating 3k pubkeys takes a long time
        //       could use threadpool here if this is an issue, or use fewer blocks or no txs per block
        for chunk_start in [0, 1000, 2000] {
            let blocks = create_test_block_data_range(
                chunk_start,
                std::iter::repeat_n(1, CHUNK_SIZE as usize),
            );
            for i in 0..CHUNK_SIZE {
                let block_num = chunk_start + i;
                let (block, receipts, traces) = blocks.get(&block_num).unwrap().clone();
                for replica in ["replica1", "replica2", "replica3"] {
                    if let Some(archiver) = model.block_data_readers.get(replica) {
                        archiver.archive_block(block.clone()).await.unwrap();
                        archiver
                            .archive_receipts(receipts.clone(), block_num)
                            .await
                            .unwrap();
                        archiver
                            .archive_traces(traces.clone(), block_num)
                            .await
                            .unwrap();
                    }
                }
            }
        }

        for replica in ["replica1", "replica2", "replica3"] {
            if let Some(archiver) = model.block_data_readers.get(replica) {
                archiver
                    .update_latest(2999, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }
        }

        // Initially no faults should exist
        for chunk_start in [0, 1000, 2000] {
            for replica in ["replica1", "replica2", "replica3"] {
                let faults = model.get_faults_chunk(replica, chunk_start).await.unwrap();
                assert!(faults.is_empty());
            }
        }

        // Force recheck all chunks in range 1000-2999
        recheck_all_chunks(&model, &metrics, false, Some(1000), Some(2999), 10)
            .await
            .unwrap();

        // After force recheck, there should still be no faults (all data is consistent)
        for chunk_start in [1000, 2000] {
            for replica in ["replica1", "replica2", "replica3"] {
                let faults = model.get_faults_chunk(replica, chunk_start).await.unwrap();
                assert!(
                    faults.is_empty(),
                    "No faults should exist for replica {} chunk {}",
                    replica,
                    chunk_start
                );
            }

            // Good blocks should be set
            let good_blocks = model.get_good_blocks(chunk_start).await.unwrap();
            assert_eq!(good_blocks.block_num_to_replica.len(), CHUNK_SIZE as usize);
        }

        // Chunk 0 should not have been rechecked (outside range)
        let good_blocks_0 = model.get_good_blocks(0).await.unwrap();
        assert!(good_blocks_0.block_num_to_replica.is_empty());
    }

    #[tokio::test]
    async fn test_store_checking_results_clears_empty_fault_chunks() {
        // Direct test of store_checking_results to verify it handles empty fault lists
        let model = setup_test_model(3);
        let chunk_start = 400;

        // First, store some faults for all replicas
        for replica in ["replica1", "replica2", "replica3"] {
            let faults = vec![Fault {
                block_num: chunk_start + 1,
                replica: replica.to_owned(),
                fault: FaultKind::MissingBlock,
            }];
            model
                .set_faults_chunk(replica, chunk_start, faults)
                .await
                .unwrap();
        }

        // Verify faults are stored
        for replica in ["replica1", "replica2", "replica3"] {
            let faults = model.get_faults_chunk(replica, chunk_start).await.unwrap();
            assert_eq!(faults.len(), 1);
        }

        // Now call store_checking_results with empty faults for replica1 and replica2
        // but with faults for replica3
        let mut new_faults_by_replica = HashMap::new();
        new_faults_by_replica.insert(
            "replica3".to_string(),
            vec![Fault {
                block_num: chunk_start + 2,
                replica: "replica3".to_owned(),
                fault: FaultKind::MissingBlock,
            }],
        );

        let good_blocks = GoodBlocks::default();
        store_checking_results(&model, chunk_start, &new_faults_by_replica, good_blocks)
            .await
            .unwrap();

        // Check that replica1 and replica2 no longer have faults stored
        let replica1_faults = model
            .get_faults_chunk("replica1", chunk_start)
            .await
            .unwrap();
        let replica2_faults = model
            .get_faults_chunk("replica2", chunk_start)
            .await
            .unwrap();

        // BUG: These assertions should pass but will fail
        assert!(
            replica1_faults.is_empty(),
            "Replica1 should have no faults after store_checking_results, but found: {:?}",
            replica1_faults
        );
        assert!(
            replica2_faults.is_empty(),
            "Replica2 should have no faults after store_checking_results, but found: {:?}",
            replica2_faults
        );

        // Replica3 should have its new fault
        let replica3_faults = model
            .get_faults_chunk("replica3", chunk_start)
            .await
            .unwrap();
        assert_eq!(replica3_faults.len(), 1);
        assert_eq!(replica3_faults[0].block_num, chunk_start + 2);
    }
}
