use eyre::Result;
use futures::future::join_all;
use monad_archive::prelude::*;
use opentelemetry::KeyValue;
use tokio::time::interval;

use crate::{
    model::{CheckerModel, Fault, FaultKind, InconsistentBlockReason},
    CHUNK_SIZE,
};

/// Worker function that periodically rechecks previously found faults to ensure
/// they are still valid
pub async fn recheck_worker(
    recheck_freq: Duration,
    model: CheckerModel,
    metrics: Metrics,
) -> Result<()> {
    info!(
        "Starting fault recheck worker with frequency {:?}",
        recheck_freq
    );
    let mut interval = interval(recheck_freq);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        // Run a complete recheck cycle for all replicas
        info!("Starting recheck cycle for all replicas");
        recheck(&model, &metrics).await?;
        info!("Recheck cycle completed, waiting for next interval");

        interval.tick().await;
    }
}

/// Rechecks previously identified faults across all replicas
async fn recheck(model: &CheckerModel, metrics: &Metrics) -> Result<()> {
    for replica in model.block_data_readers.keys() {
        let latest_checked = model.get_latest_checked_for_replica(replica).await?;
        info!(
            %replica,
            latest_checked,
            "Rechecking faults for replica"
        );

        let mut total_faults = Vec::new();

        // Process chunks of CHUNK_SIZE blocks at a time
        let chunks_to_process = latest_checked / CHUNK_SIZE;
        info!(
            %replica,
            chunks = chunks_to_process,
            "Processing chunks for replica"
        );

        for idx in 0..(latest_checked / CHUNK_SIZE) {
            let chunk_start = idx * CHUNK_SIZE;
            debug!(
                %replica,
                chunk_start,
                "Rechecking chunk"
            );

            // Recheck this chunk and get any current faults
            let new_faults = recheck_fault_chunk(model, chunk_start, replica).await?;

            if new_faults.is_empty() {
                debug!(
                    %replica,
                    chunk_start,
                    "Deleting empty faults chunk"
                );
                model.delete_faults_chunk(replica, chunk_start).await?;
            } else {
                debug!(
                    %replica,
                    chunk_start,
                    fault_count = new_faults.len(),
                    "Found faults in chunk"
                );

                // Update fault statistics
                total_faults.extend(new_faults.iter().cloned());

                // Store updated fault information back to S3
                model
                    .set_faults_chunk(replica, chunk_start, new_faults)
                    .await?;
            }
        }

        // Update metrics with current fault counts
        metrics.periodic_gauge_with_attrs(
            "replica_faults_total",
            total_faults.len() as u64,
            vec![KeyValue::new("replica", replica.to_owned())],
        );

        let grouped_by_faults = total_faults
            .iter()
            .map(|f| (f.fault.clone(), f.block_num))
            .collect::<HashMap<FaultKind, u64>>();

        for (fault, count) in &grouped_by_faults {
            metrics.periodic_gauge_with_attrs(
                "replica_faults_by_kind",
                *count,
                vec![
                    KeyValue::new("replica", replica.to_owned()),
                    KeyValue::new("kind", fault.metric_name()),
                ],
            );
        }

        info!(
            %replica,
            total_faults = total_faults.len(),
            missing = grouped_by_faults.get(&FaultKind::MissingBlock).unwrap_or(&0),
            inconsistent = grouped_by_faults
                .iter()
                .filter(|(kind, _)| matches!(kind, FaultKind::InconsistentBlock(_)))
                .map(|(_, count)| count)
                .sum::<u64>(),
            "Recheck completed for replica"
        );
    }

    Ok(())
}

/// Rechecks a specific chunk of blocks for faults in a specific replica
pub(crate) async fn recheck_fault_chunk(
    model: &CheckerModel,
    chunk_start: u64,
    replica: &str,
) -> Result<Vec<Fault>> {
    // Get previously identified faults for this chunk
    let faults = model.get_faults_chunk(replica, chunk_start).await?;
    if faults.is_empty() {
        debug!(
            %replica,
            chunk_start,
            "No existing faults found in chunk"
        );
        return Ok(vec![]);
    }

    debug!(
        %replica,
        chunk_start,
        fault_count = faults.len(),
        "Rechecking faults in chunk"
    );

    let mut new_faults = Vec::with_capacity(faults.len());

    // Try to fetch each previously faulty block
    let faulty_blocks = faults.iter().map(|fault| async {
        (
            fault.block_num,
            model
                .fetch_block_data_for_replica(fault.block_num, replica)
                .await,
        )
    });

    // Process fetched blocks, recording any that are still missing
    let faulty_blocks = join_all(faulty_blocks)
        .await
        .into_iter()
        .filter_map(|(block_num, data)| {
            if data.is_none() {
                // Block is still missing - record the fault
                debug!(
                    %replica,
                    block_num,
                    "Block still missing during recheck"
                );
                new_faults.push(Fault {
                    block_num,
                    replica: replica.to_owned(),
                    fault: FaultKind::MissingBlock,
                });
            } else {
                debug!(
                    %replica,
                    block_num,
                    "Previously missing block is now available"
                );
            }
            data
        })
        .collect::<Vec<_>>();

    if faulty_blocks.is_empty() {
        if !new_faults.is_empty() {
            debug!(
                %replica,
                chunk_start,
                missing_count = new_faults.len(),
                "All blocks still missing in chunk"
            );
        }
        return Ok(new_faults);
    }

    // Get mapping of which replica has the "good" version of each block
    let good_block_mapping = &model
        .get_good_blocks(chunk_start)
        .await?
        .block_num_to_replica;

    // Fetch the corresponding "good" blocks to compare against
    let faulty_block_nums = faulty_blocks
        .iter()
        .map(|(block, _, _)| block.header.number);

    let good_blocks = faulty_block_nums.map(|block_num| async move {
        let good_replica = good_block_mapping.get(&block_num).unwrap();
        debug!(
            block_num,
            %good_replica,
            "Fetching good block for comparison"
        );
        model
            .fetch_block_data_for_replica(block_num, good_replica)
            .await
            .expect("Should not fail fetching good block")
    });
    // Todo: investigate why stream::iter and buffered cause lifetime issue
    let good_blocks = join_all(good_blocks).await.into_iter().collect::<Vec<_>>();

    // Ensure we have a good block for each faulty block
    eyre::ensure!(
        good_blocks.len() == faulty_blocks.len(),
        "Should have a good block for every faulty block"
    );

    // Compare blocks and record any inconsistencies
    let mut fixed_count = 0;
    let mut still_inconsistent = 0;
    for (good, faulty) in good_blocks.into_iter().zip(faulty_blocks) {
        let block_num = faulty.0.header.number;

        if good != faulty {
            new_faults.push(Fault {
                block_num,
                replica: replica.to_owned(),
                fault: FaultKind::InconsistentBlock(find_inconsistent_reason(
                    (&good.0, &good.1, &good.2),
                    (&faulty.0, &faulty.1, &faulty.2),
                    replica,
                    block_num,
                )),
            });
            still_inconsistent += 1;
        } else {
            debug!(
                %replica,
                block_num,
                "Previously inconsistent block is now fixed"
            );
            fixed_count += 1;
        }
    }

    if fixed_count > 0 {
        info!(
            %replica,
            chunk_start,
            fixed_count,
            still_inconsistent,
            "Found fixed and inconsistent blocks in chunk"
        );
    }

    Ok(new_faults)
}

pub fn find_inconsistent_reason(
    good: (&Block, &BlockReceipts, &BlockTraces),
    faulty: (&Block, &BlockReceipts, &BlockTraces),
    replica: &str,
    block_num: u64,
) -> InconsistentBlockReason {
    debug!(
        %replica,
        block_num,
        "Replica still inconsistent with good replica"
    );
    let (good_block, good_receipts, good_traces) = good;
    let (faulty_block, faulty_receipts, faulty_traces) = faulty;

    if good_block != faulty_block {
        debug!(
            %replica,
            block_num,
            "Block does not match good replica"
        );
        if good_block.header != faulty_block.header {
            debug!(
                %replica,
                block_num,
                "Block header does not match good replica"
            );
            InconsistentBlockReason::Header
        } else if good_block.body.transactions.len() != faulty_block.body.transactions.len() {
            debug!(
                %replica,
                block_num,
                good_len = good_block.body.transactions.len(),
                faulty_len = faulty_block.body.transactions.len(),
                "Block body length does not match good replica"
            );
            InconsistentBlockReason::BodyLen
        } else {
            debug!(
                %replica,
                block_num,
                "Block body does not match good replica"
            );
            let mut count = 0;
            for (good_tx, faulty_tx) in good_block
                .body
                .transactions
                .iter()
                .zip(faulty_block.body.transactions.iter())
            {
                if let (Ok(good_tx), Ok(faulty_tx)) = (
                    serde_json::to_string(good_tx),
                    serde_json::to_string(faulty_tx),
                ) {
                    debug!(
                        %replica,
                        block_num,
                        count,
                        max_count = 10,
                        good_tx,
                        faulty_tx,
                        "Transaction does not match good replica"
                    );
                }
                count += 1;
                if count > 10 {
                    break;
                }
            }
            InconsistentBlockReason::BodyContents
        }
    } else if good_receipts.len() != faulty_receipts.len() {
        debug!(
            %replica,
            block_num,
            good_len = good_receipts.len(),
            faulty_len = faulty_receipts.len(),
            "Receipts length does not match good replica"
        );
        InconsistentBlockReason::ReceiptsLen
    } else if good_receipts != faulty_receipts {
        debug!(
            %replica,
            block_num,
            "Receipts do not match good replica"
        );
        let mut count = 0;
        for (good_receipt, faulty_receipt) in good_receipts.iter().zip(faulty_receipts.iter()) {
            if let (Ok(good_receipt), Ok(faulty_receipt)) = (
                serde_json::to_string(good_receipt),
                serde_json::to_string(faulty_receipt),
            ) {
                debug!(
                    %replica,
                    block_num,
                    count,
                    max_count = 10,
                    good_receipt,
                    faulty_receipt,
                    "Receipt does not match good replica"
                );
            }
            count += 1;
            if count > 10 {
                break;
            }
        }
        InconsistentBlockReason::ReceiptsContents
    } else if good_traces.len() != faulty_traces.len() {
        debug!(
            %replica,
            block_num,
            good_len = good_traces.len(),
            faulty_len = faulty_traces.len(),
            "Traces length does not match good replica"
        );
        InconsistentBlockReason::TracesLen
    } else {
        debug!(
            %replica,
            block_num,
            "Traces do not match good replica"
        );
        let mut count = 0;
        for (good_trace, faulty_trace) in good_traces.iter().zip(faulty_traces.iter()) {
            if let (Ok(good_trace), Ok(faulty_trace)) = (
                serde_json::to_string(good_trace),
                serde_json::to_string(faulty_trace),
            ) {
                debug!(
                    %replica,
                    block_num,
                    count,
                    max_count = 10,
                    good_trace,
                    faulty_trace,
                    "Trace does not match good replica"
                );
            }
            count += 1;
            if count > 10 {
                break;
            }
        }
        InconsistentBlockReason::TracesContents
    }
}

#[cfg(test)]
mod tests {
    use monad_archive::prelude::LatestKind;

    use super::*;
    use crate::{
        checker::tests::{
            create_test_block_data, create_test_block_data_with_len, setup_test_model,
        },
        model::GoodBlocks,
    };

    #[tokio::test]
    async fn test_recheck_fault_chunk() {
        // Setup test model with memory storage
        let model = setup_test_model();
        let chunk_start = 100;
        let replica_name = "replica1";

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
            .insert(chunk_start + 1, "replica2".to_owned());
        good_blocks
            .block_num_to_replica
            .insert(chunk_start + 2, "replica2".to_owned());

        // Store the good blocks mapping
        model
            .set_good_blocks(chunk_start, good_blocks)
            .await
            .unwrap();

        // Add the "good" blocks to replica2
        if let Some(archiver) = model.block_data_readers.get("replica2") {
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

        // First recheck - both blocks should still be faulty since we haven't fixed anything
        let faults = recheck_fault_chunk(&model, chunk_start, replica_name)
            .await
            .unwrap();

        // Verify both faults are still present
        assert_eq!(faults.len(), 2);
        assert!(faults
            .iter()
            .any(|f| f.block_num == chunk_start + 1 && matches!(f.fault, FaultKind::MissingBlock)));
        assert!(faults
            .iter()
            .any(|f| f.block_num == chunk_start + 2 && matches!(f.fault, FaultKind::MissingBlock)));

        // Now fix the missing block but make it inconsistent
        if let Some(archiver) = model.block_data_readers.get(replica_name) {
            // Add the first block (was missing)
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

            // Add the second block but with different data (will be inconsistent)
            let (block, receipts, traces) = create_test_block_data(chunk_start + 2, 2); // Different variant
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

        // Recheck again - first block should be fixed, second should be inconsistent
        let faults = recheck_fault_chunk(&model, chunk_start, replica_name)
            .await
            .unwrap();

        // Verify only the inconsistent fault remains
        assert_eq!(faults.len(), 1);
        assert!(faults.iter().any(|f| f.block_num == chunk_start + 2
            && matches!(
                f.fault,
                FaultKind::InconsistentBlock(InconsistentBlockReason::Header)
            )));

        // Change the 2nd to have different receipts len to test correct reason
        if let Some(archiver) = model.block_data_readers.get(replica_name) {
            // Add the second block but with different data (will be inconsistent)
            let (block, receipts, traces) =
                create_test_block_data_with_len(chunk_start + 2, 1, 3, 2); // Different variant
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

        // Recheck again - first block should be fixed, second should be inconsistent
        let faults = recheck_fault_chunk(&model, chunk_start, replica_name)
            .await
            .unwrap();

        // Verify only the inconsistent fault remains
        assert_eq!(faults.len(), 1);
        assert!(faults.iter().any(|f| f.block_num == chunk_start + 2
            && matches!(
                f.fault,
                FaultKind::InconsistentBlock(InconsistentBlockReason::TracesLen)
            )));

        // Now fix the inconsistent block
        if let Some(archiver) = model.block_data_readers.get(replica_name) {
            // Add the second block with correct data
            let (block, receipts, traces) = create_test_block_data(chunk_start + 2, 1); // Same as the "good" block
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

        // Final recheck - all blocks should be fixed
        let faults = recheck_fault_chunk(&model, chunk_start, replica_name)
            .await
            .unwrap();

        // Verify no faults remain
        assert_eq!(faults.len(), 0);
    }
}
