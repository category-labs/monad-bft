use eyre::{Context, Result};
use monad_archive::prelude::*;
use opentelemetry::KeyValue;
use tokio::time::interval;

use crate::{
    checker::{fetch_block_data, process_blocks, store_checking_results},
    model::{CheckerModel, Fault, FaultKind},
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
        recheck_all_faults(&model, &metrics).await?;
        info!("Recheck cycle completed, waiting for next interval");

        interval.tick().await;
    }
}

/// Rechecks all chunks that contain faults by reprocessing them from scratch
async fn recheck_all_faults(model: &CheckerModel, metrics: &Metrics) -> Result<()> {
    // Collect all chunks that have faults across all replicas
    let fault_chunks = collect_fault_chunks(model).await?;

    if fault_chunks.is_empty() {
        info!("No fault chunks found to recheck");
        return Ok(());
    }

    info!("Found {} chunks with faults to recheck", fault_chunks.len());

    // Process each fault chunk
    for chunk_start in fault_chunks {
        info!(chunk_start, "Rechecking fault chunk");
        recheck_chunk_from_scratch(model, chunk_start, metrics).await?;
    }

    // Update metrics after all chunks are rechecked
    update_fault_metrics(model, metrics).await?;

    Ok(())
}

/// Collects all unique chunk starts that have faults across all replicas
async fn collect_fault_chunks(model: &CheckerModel) -> Result<Vec<u64>> {
    let mut fault_chunks = std::collections::HashSet::new();

    for replica in model.block_data_readers.keys() {
        let latest_checked = model.get_latest_checked_for_replica(replica).await?;

        // Check each chunk up to latest checked
        for idx in 0..=(latest_checked / CHUNK_SIZE) {
            let chunk_start = idx * CHUNK_SIZE;
            let faults = model.get_faults_chunk(replica, chunk_start).await?;

            if !faults.is_empty() {
                fault_chunks.insert(chunk_start);
            }
        }
    }

    let mut chunks: Vec<u64> = fault_chunks.into_iter().collect();
    chunks.sort();
    Ok(chunks)
}

/// Rechecks a chunk from scratch using the original checker logic
async fn recheck_chunk_from_scratch(
    model: &CheckerModel,
    chunk_start: u64,
    _metrics: &Metrics,
) -> Result<()> {
    let end_block = chunk_start + CHUNK_SIZE - 1;

    info!(chunk_start, end_block, "Rechecking chunk from scratch");

    // First, backup old faults and good blocks before rechecking
    backup_old_results(model, chunk_start).await?;

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

    // Compare with old results and log differences
    log_recheck_differences(model, chunk_start, &new_faults_by_replica).await?;

    // Store the new results (this will overwrite the old ones)
    store_checking_results(model, chunk_start, new_faults_by_replica, new_good_blocks).await?;

    info!(chunk_start, "Chunk recheck completed");

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

/// Logs differences between old and new recheck results
async fn log_recheck_differences(
    model: &CheckerModel,
    chunk_start: u64,
    new_faults_by_replica: &HashMap<String, Vec<Fault>>,
) -> Result<()> {
    for replica in model.block_data_readers.keys() {
        let old_faults = model.get_faults_chunk(replica, chunk_start).await?;
        let new_faults = new_faults_by_replica
            .get(replica)
            .cloned()
            .unwrap_or_default();

        let old_count = old_faults.len();
        let new_count = new_faults.len();

        if old_count != new_count {
            info!(
                replica = %replica,
                chunk_start,
                old_fault_count = old_count,
                new_fault_count = new_count,
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
            info!(
                replica = %replica,
                chunk_start,
                fixed_count = fixed_faults.len(),
                "Faults fixed after recheck"
            );

            for (block_num, fault_kind) in fixed_faults.iter().take(10) {
                debug!(
                    replica = %replica,
                    block_num,
                    fault_kind = ?fault_kind,
                    "Fixed fault"
                );
            }
        }

        if !new_issues.is_empty() {
            warn!(
                replica = %replica,
                chunk_start,
                new_issue_count = new_issues.len(),
                "New faults found after recheck"
            );

            for (block_num, fault_kind) in new_issues.iter().take(10) {
                debug!(
                    replica = %replica,
                    block_num,
                    fault_kind = ?fault_kind,
                    "New fault found"
                );
            }
        }
    }

    Ok(())
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
        recheck_chunk_from_scratch(&model, chunk_start, &metrics)
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
        let chunks = collect_fault_chunks(&model).await.unwrap();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks, vec![0, 1000, 2000]);
    }
}
