use eyre::{Context, Result};
use futures::{stream, StreamExt};
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
) -> Result<()> {
    info!(
        "Starting rechecker v2 worker with frequency {:?}",
        recheck_freq
    );
    let mut interval = interval(recheck_freq);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        info!("Starting recheck cycle for all replicas");
        recheck_all_faults(&model, &metrics, false, None, None).await?;
        info!("Recheck cycle completed, waiting for next interval");

        interval.tick().await;
    }
}

/// Worker function for standalone rechecker v2 with optional parameters
pub async fn rechecker_v2_standalone(
    recheck_freq: Duration,
    model: CheckerModel,
    metrics: Metrics,
    dry_run: bool,
    start_block: Option<u64>,
    end_block: Option<u64>,
    force_recheck: bool,
    worker_mode: bool,
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
                recheck_all_chunks(&model, &metrics, dry_run, start_block, end_block).await?;
            } else {
                recheck_all_faults(&model, &metrics, dry_run, start_block, end_block).await?;
            }
            info!("Recheck cycle completed, waiting for next interval");
        }
    } else {
        // Run once and exit
        info!("Running single recheck cycle");
        if force_recheck {
            recheck_all_chunks(&model, &metrics, dry_run, start_block, end_block).await?;
        } else {
            recheck_all_faults(&model, &metrics, dry_run, start_block, end_block).await?;
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
) -> Result<()> {
    // Collect all chunks that have faults across all replicas
    let fault_chunks = collect_fault_chunks(model, start_block, end_block).await?;

    if fault_chunks.is_empty() {
        info!("No fault chunks found to recheck");
        return Ok(());
    }

    info!("Found {} chunks with faults to recheck", fault_chunks.len());

    // Process each fault chunk
    for chunk_start in fault_chunks {
        info!(chunk_start, dry_run, "Rechecking fault chunk");
        recheck_chunk_from_scratch(model, chunk_start, metrics, dry_run).await?;
    }

    // Update metrics after all chunks are rechecked (skip in dry run)
    if !dry_run {
        update_fault_metrics(model, metrics).await?;
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
) -> Result<()> {
    // Collect all chunks in the specified range
    let all_chunks = collect_all_chunks(model, start_block, end_block).await?;

    if all_chunks.is_empty() {
        info!("No chunks found to recheck in the specified range");
        return Ok(());
    }

    info!("Found {} chunks to recheck (force mode)", all_chunks.len());

    // Process each chunk
    for chunk_start in all_chunks {
        info!(chunk_start, dry_run, "Force rechecking chunk");
        recheck_chunk_from_scratch(model, chunk_start, metrics, dry_run).await?;
    }

    // Update metrics after all chunks are rechecked (skip in dry run)
    if !dry_run {
        update_fault_metrics(model, metrics).await?;
    }

    Ok(())
}

/// Collects all chunks in the specified range, regardless of fault status
async fn collect_all_chunks(
    model: &CheckerModel,
    start_block: Option<u64>,
    end_block: Option<u64>,
) -> Result<Vec<u64>> {
    // Get the minimum latest checked across all replicas to determine the upper bound
    let min_latest_checked = model.min_latest_checked().await?;

    // Filter chunks based on block range if provided
    let start_chunk = start_block
        .map(|b| (b / CHUNK_SIZE) * CHUNK_SIZE)
        .unwrap_or(0);
    let end_chunk = end_block
        .map(|b| (b / CHUNK_SIZE) * CHUNK_SIZE)
        .unwrap_or((min_latest_checked / CHUNK_SIZE) * CHUNK_SIZE);

    // Ensure we don't go beyond what has been checked
    let effective_end_chunk = end_chunk.min((min_latest_checked / CHUNK_SIZE) * CHUNK_SIZE);

    // Collect all chunks in the range
    let mut chunks = Vec::new();
    let mut current_chunk = start_chunk;
    while current_chunk <= effective_end_chunk {
        chunks.push(current_chunk);
        current_chunk += CHUNK_SIZE;
    }

    info!(
        "Collected {} chunks to force recheck (from block {} to {})",
        chunks.len(),
        start_chunk,
        effective_end_chunk + CHUNK_SIZE - 1
    );

    Ok(chunks)
}

/// Collects all unique chunk starts that have faults across all replicas
async fn collect_fault_chunks(
    model: &CheckerModel,
    start_block: Option<u64>,
    end_block: Option<u64>,
) -> Result<Vec<u64>> {
    // Filter chunks based on block range if provided
    let start_chunk = start_block.map(|b| b / CHUNK_SIZE).unwrap_or(0);
    let end_chunk = end_block.map(|b| b / CHUNK_SIZE).unwrap_or(u64::MAX);

    // Collect all (replica, chunk_start) pairs to check
    let mut chunks_to_check = Vec::new();
    for replica in model.block_data_readers.keys() {
        let latest_checked = model.get_latest_checked_for_replica(replica).await?;
        let latest_checked_chunk = latest_checked / CHUNK_SIZE;
        let effective_end = end_chunk.min(latest_checked_chunk);

        for idx in start_chunk..=effective_end {
            if idx > latest_checked_chunk {
                break;
            }
            let chunk_start = idx * CHUNK_SIZE;
            chunks_to_check.push((replica.clone(), chunk_start));
        }
    }

    debug!(
        "Checking {} chunks across {} replicas for faults",
        chunks_to_check.len(),
        model.block_data_readers.len()
    );

    // Process chunks in parallel with concurrency limit of 20
    const CONCURRENT_REQUESTS: usize = 20;

    let fault_chunks: std::collections::HashSet<u64> = stream::iter(chunks_to_check)
        .map(|(replica, chunk_start)| async move {
            let faults = model.get_faults_chunk(&replica, chunk_start).await?;
            Ok::<_, eyre::Error>(if !faults.is_empty() {
                Some(chunk_start)
            } else {
                None
            })
        })
        .buffered(CONCURRENT_REQUESTS)
        .try_fold(
            std::collections::HashSet::new(),
            |mut acc, chunk_opt| async move {
                if let Some(chunk) = chunk_opt {
                    acc.insert(chunk);
                }
                Ok(acc)
            },
        )
        .await?;

    let mut chunks: Vec<u64> = fault_chunks.into_iter().collect();
    chunks.sort();
    Ok(chunks)
}

/// Rechecks a chunk from scratch using the original checker logic
async fn recheck_chunk_from_scratch(
    model: &CheckerModel,
    chunk_start: u64,
    _metrics: &Metrics,
    dry_run: bool,
) -> Result<()> {
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

    let data_by_block_num = fetch_block_data(model, chunk_start..=end_block, &replicas).await;

    // Process blocks to find faults and good blocks using original checker logic
    let (new_faults_by_replica, new_good_blocks) =
        process_blocks(&data_by_block_num, chunk_start, end_block);

    // Get old results for comparison
    let mut old_faults_by_replica = HashMap::new();
    for replica in model.block_data_readers.keys() {
        let old_faults = model.get_faults_chunk(replica, chunk_start).await?;
        if !old_faults.is_empty() {
            old_faults_by_replica.insert(replica.clone(), old_faults);
        }
    }
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
        // Only backup if we're about to overwrite and there are differences
        if has_changes {
            backup_old_results(model, chunk_start).await?;

            // Store the new results (this will overwrite the old ones)
            store_checking_results(model, chunk_start, new_faults_by_replica, new_good_blocks)
                .await?;

            info!(chunk_start, "Chunk recheck completed with updates");
        } else {
            info!(chunk_start, "Chunk recheck completed with no changes");
        }
    }

    Ok(())
}

/// Backs up old fault and good block data before rechecking
async fn backup_old_results(model: &CheckerModel, chunk_start: u64) -> Result<()> {
    // Backup old faults for each replica
    for replica in model.block_data_readers.keys() {
        let old_faults = model.get_faults_chunk(replica, chunk_start).await?;

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
    let old_good_blocks = model.get_good_blocks(chunk_start).await?;
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
async fn update_fault_metrics(model: &CheckerModel, metrics: &Metrics) -> Result<()> {
    for replica in model.block_data_readers.keys() {
        let latest_checked = model.get_latest_checked_for_replica(replica).await?;
        let mut total_faults = Vec::new();

        // Collect all faults for this replica
        for idx in 0..(latest_checked / CHUNK_SIZE) {
            let chunk_start = idx * CHUNK_SIZE;
            let faults = model.get_faults_chunk(replica, chunk_start).await?;
            total_faults.extend(faults);
        }

        // Update total faults metric
        metrics.periodic_gauge_with_attrs(
            MetricNames::REPLICA_FAULTS_TOTAL,
            total_faults.len() as u64,
            vec![KeyValue::new("replica", replica.to_owned())],
        );

        // Group faults by kind and update per-kind metrics
        let mut fault_counts: HashMap<String, u64> = HashMap::new();
        for fault in &total_faults {
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
        checker::tests::{create_test_block_data, setup_test_model},
        model::{GoodBlocks, InconsistentBlockReason},
    };

    #[tokio::test]
    async fn test_rechecker_v2_full_recheck() {
        // Setup test model
        let model = setup_test_model();
        let chunk_start = 100;
        let metrics = Metrics::new(None::<String>, "test", "", Duration::from_secs(60)).unwrap();

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
        if let Some(archiver) = model.block_data_readers.get("replica2") {
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

        // Set latest checked for replica1
        model
            .set_latest_checked_for_replica("replica1", chunk_start + CHUNK_SIZE - 1)
            .await
            .unwrap();

        // Now fix the first block in replica1 (was missing)
        if let Some(archiver) = model.block_data_readers.get("replica1") {
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

        // Run rechecker v2
        recheck_chunk_from_scratch(&model, chunk_start, &metrics, false)
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
        let model = setup_test_model();

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
        let chunks = collect_fault_chunks(&model, None, None).await.unwrap();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks, vec![0, 1000, 2000]);
    }

    #[tokio::test]
    async fn test_rechecker_v2_dry_run() {
        // Setup test model
        let model = setup_test_model();
        let chunk_start = 100;
        let metrics = Metrics::new(None::<String>, "test", "", Duration::from_secs(60)).unwrap();

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

        // Run rechecker v2 in DRY RUN mode
        recheck_chunk_from_scratch(&model, chunk_start, &metrics, true)
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
        let model = setup_test_model();
        let chunk_start = 200;
        let metrics = Metrics::new(None::<String>, "test", "", Duration::from_secs(60)).unwrap();

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
        recheck_chunk_from_scratch(&model, chunk_start, &metrics, false)
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
        let model = setup_test_model();

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

        // Test 1: Recheck only blocks 1000-2500 (should process chunks 1000 and 2000)
        // We'll do a dry run and count how many chunks would be processed

        // Mock the processing by collecting which chunks would be processed
        let fault_chunks = collect_fault_chunks(&model, None, None).await.unwrap();
        let mut filtered_chunks = fault_chunks.clone();

        // Apply start filter
        let start_chunk = (1000 / CHUNK_SIZE) * CHUNK_SIZE; // 1000
        filtered_chunks.retain(|&chunk| chunk >= start_chunk);

        // Apply end filter
        let end_chunk = (2500 / CHUNK_SIZE) * CHUNK_SIZE; // 2000
        filtered_chunks.retain(|&chunk| chunk <= end_chunk);

        assert_eq!(filtered_chunks.len(), 2);
        assert_eq!(filtered_chunks, vec![1000, 2000]);

        // Test 2: Start block only (should process chunks >= 2000)
        let mut filtered_chunks2 = fault_chunks.clone();
        let start_chunk2 = (2000 / CHUNK_SIZE) * CHUNK_SIZE;
        filtered_chunks2.retain(|&chunk| chunk >= start_chunk2);

        assert_eq!(filtered_chunks2.len(), 2);
        assert_eq!(filtered_chunks2, vec![2000, 3000]);

        // Test 3: End block only (should process chunks <= 1000)
        let mut filtered_chunks3 = fault_chunks;
        let end_chunk3 = (1500 / CHUNK_SIZE) * CHUNK_SIZE;
        filtered_chunks3.retain(|&chunk| chunk <= end_chunk3);

        assert_eq!(filtered_chunks3.len(), 2);
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
        let model = setup_test_model();
        let chunk_start = 300;
        let metrics = Metrics::new(None::<String>, "test", "", Duration::from_secs(60)).unwrap();

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
        if let Some(archiver) = model.block_data_readers.get("replica1") {
            for block_num in chunk_start..(chunk_start + CHUNK_SIZE) {
                let (block, receipts, traces) = create_test_block_data(block_num, 1);
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
                    let (block, receipts, traces) = create_test_block_data(block_num, 1);
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
        recheck_chunk_from_scratch(&model, chunk_start, &metrics, false)
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
        let model = setup_test_model();

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
        let chunks = collect_all_chunks(&model, None, None).await.unwrap();
        assert_eq!(chunks.len(), 4); // 0, 1000, 2000, 3000
        assert_eq!(chunks, vec![0, 1000, 2000, 3000]);

        // Test 2: With start and end range
        let chunks = collect_all_chunks(&model, Some(1500), Some(3500))
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3); // 1000, 2000, 3000
        assert_eq!(chunks, vec![1000, 2000, 3000]);

        // Test 3: End block beyond latest checked - should cap at min latest checked
        let chunks = collect_all_chunks(&model, Some(2000), Some(10000))
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2); // 2000, 3000
        assert_eq!(chunks, vec![2000, 3000]);

        // Test 4: Exact chunk boundaries
        let chunks = collect_all_chunks(&model, Some(1000), Some(1999))
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1); // Only chunk 1000
        assert_eq!(chunks, vec![1000]);
    }

    #[tokio::test]
    async fn test_force_recheck_all_chunks() {
        // Setup test model
        let model = setup_test_model();
        let metrics = Metrics::new(None::<String>, "test", "", Duration::from_secs(60)).unwrap();

        // Set latest checked for all replicas
        for replica in ["replica1", "replica2", "replica3"] {
            model
                .set_latest_checked_for_replica(replica, 2999)
                .await
                .unwrap();
        }

        // Add complete data for all replicas for chunks 0, 1000, 2000
        for replica in ["replica1", "replica2", "replica3"] {
            if let Some(archiver) = model.block_data_readers.get(replica) {
                for chunk_start in [0, 1000, 2000] {
                    for i in 0..CHUNK_SIZE {
                        let block_num = chunk_start + i;
                        let (block, receipts, traces) = create_test_block_data(block_num, 1);
                        archiver.archive_block(block).await.unwrap();
                        archiver
                            .archive_receipts(receipts, block_num)
                            .await
                            .unwrap();
                        archiver.archive_traces(traces, block_num).await.unwrap();
                    }
                }
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
        recheck_all_chunks(&model, &metrics, false, Some(1000), Some(2999))
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
        let model = setup_test_model();
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
        store_checking_results(&model, chunk_start, new_faults_by_replica, good_blocks)
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
