use std::collections::HashMap;

use eyre::Result;
use futures::stream;
use monad_archive::prelude::*;

use crate::{
    model::{CheckerModel, Fault, FaultKind, GoodBlocks, InconsistentBlockReason},
    rechecker_legacy::find_inconsistent_reason,
    CHUNK_SIZE,
};

/// Main checker worker function that continuously verifies blocks across all replicas
///
/// This function:
/// 1. Fetches blocks, receipts, and traces from all replicas
/// 2. Performs validation on individual blocks
/// 3. Compares data across replicas to find inconsistencies
/// 4. Records good blocks and faults in S3
pub async fn checker_worker(
    model: CheckerModel,
    min_lag_from_tip: u64,
    metrics: Metrics,
) -> Result<()> {
    info!(min_lag_from_tip, "Starting checker worker");

    let mut next_to_check = model.min_latest_checked().await?;
    if next_to_check != 0 {
        next_to_check += 1;
    }

    info!("Initial next block to check: {}", next_to_check);

    loop {
        let latest_to_check = model.latest_to_check().await?;
        let required_blocks = next_to_check + min_lag_from_tip + CHUNK_SIZE;

        metrics.gauge(MetricNames::LATEST_TO_CHECK, latest_to_check);
        metrics.gauge(MetricNames::NEXT_TO_CHECK, next_to_check);

        if latest_to_check < required_blocks {
            info!(
                latest_to_check,
                next_to_check,
                required_blocks,
                "Nothing to check. Waiting for CHUNK_SIZE block gap"
            );
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }

        let end_block = next_to_check + CHUNK_SIZE - 1;
        info!(next_to_check, end_block, "Processing block batch");

        // Process a batch of blocks and update the min_latest_checked
        next_to_check = process_block_batch(&model, next_to_check, end_block).await?;
        info!(new_next_to_check = next_to_check, "Block batch processed");
    }
}

/// Processes a batch of blocks, finds consensus, and stores results
///
/// This function:
/// 1. Fetches block data from all replicas for the given range
/// 2. Processes blocks to find faults and good blocks
/// 3. Stores results in S3
/// 4. Updates the latest checked block for each replica
///
/// Returns the next block number to check
async fn process_block_batch(
    model: &CheckerModel,
    start_block: u64,
    end_block: u64,
) -> Result<u64> {
    // Fetch block data from all replicas
    let replicas = model
        .block_data_readers
        .keys()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    info!(
        replica_count = replicas.len(),
        "Fetching block data from replicas"
    );

    let data_by_block_num = fetch_block_data(model, start_block..=end_block, &replicas).await;

    debug!("Fetched data for {} blocks", data_by_block_num.len());

    // Process blocks to find faults and good blocks
    info!("Processing blocks to find faults and good blocks");
    let (faults_by_replica, good_blocks) =
        process_blocks(&data_by_block_num, start_block, end_block);

    // Count total faults and good blocks
    let total_faults: usize = faults_by_replica.values().map(|v| v.len()).sum();
    let total_good_blocks = good_blocks.block_num_to_replica.len();

    info!(
        total_faults,
        total_good_blocks, "Found faults and good blocks"
    );

    // Store results in S3
    info!("Storing checking results");
    store_checking_results(model, start_block, faults_by_replica, good_blocks).await?;

    // Update the latest checked block for each replica
    info!(end_block, "Updating latest checked block");
    for replica_name in model.block_data_readers.keys() {
        model
            .set_latest_checked_for_replica(replica_name, end_block)
            .await?;
    }

    Ok(end_block + 1)
}

/// Fetches block data from all replicas for a range of blocks.
pub async fn fetch_block_data(
    model: &CheckerModel,
    block_nums: impl IntoIterator<Item = u64>,
    replicas: &[&str],
) -> HashMap<u64, HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>> {
    debug!("Fetching block data for {} replicas", replicas.len());

    stream::iter(block_nums)
        .map(|block_num| async move {
            let mut block_data = HashMap::new();

            debug!(block_num, "Fetching data for block");

            // For each block number, fetch data from all replicas
            for &replica_name in replicas {
                let data = model
                    .fetch_block_data_for_replica(block_num, replica_name)
                    .await;

                block_data.insert(replica_name.to_owned(), data);
            }

            (block_num, block_data)
        })
        .buffered(100) // Process up to 100 blocks in parallel
        .collect::<Vec<(
            u64,
            HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>,
        )>>()
        .await
        .into_iter()
        .collect()
}

/// Processes blocks to find faults and good blocks by comparing data across replicas.
pub fn process_blocks(
    data_by_block_num: &HashMap<u64, HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>>,
    start_block: u64,
    end_block: u64,
) -> (HashMap<String, Vec<Fault>>, GoodBlocks) {
    let mut faults_by_replica: HashMap<String, Vec<Fault>> = HashMap::new();
    let mut good_blocks = GoodBlocks {
        block_num_to_replica: HashMap::new(),
    };

    debug!(start_block, end_block, "Processing blocks");

    for block_num in start_block..=end_block {
        if let Some(replica_data) = data_by_block_num.get(&block_num) {
            debug!(block_num, "Processing block");
            process_single_block(
                block_num,
                replica_data,
                &mut faults_by_replica,
                &mut good_blocks,
            );
        } else {
            debug!(block_num, "No data found for block");
        }
    }

    // Log summary of processed blocks
    let total_faults: usize = faults_by_replica.values().map(|v| v.len()).sum();
    let good_block_count = good_blocks.block_num_to_replica.len();

    debug!(total_faults, good_block_count, "Processing complete");

    (faults_by_replica, good_blocks)
}

/// Processes a single block across all replicas
pub fn process_single_block(
    block_num: u64,
    replica_data: &HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>,
    faults_by_replica: &mut HashMap<String, Vec<Fault>>,
    good_blocks: &mut GoodBlocks,
) {
    // Track which replicas have valid data for consensus comparison
    let mut valid_replicas = HashMap::new();

    // Step 1: Record any missing or corrupted blocks
    for (replica_name, block_data_opt) in replica_data {
        if block_data_opt.is_none() {
            // Missing or already failed to parse
            debug!(
                block_num,
                %replica_name,
                "Missing block in replica"
            );

            faults_by_replica
                .entry(replica_name.clone())
                .or_default()
                .push(Fault {
                    block_num,
                    replica: replica_name.clone(),
                    fault: FaultKind::MissingBlock,
                });
            continue;
        }

        let (block, receipts, traces) = block_data_opt.as_ref().unwrap();

        // Step 2: Perform basic verifications on parsed blocks
        if let Err(verification_fault) = verify_block(block_num, block, receipts, traces) {
            debug!(
                block_num,
                %replica_name,
                ?verification_fault,
                "Block verification failed"
            );

            faults_by_replica
                .entry(replica_name.clone())
                .or_default()
                .push(Fault {
                    block_num,
                    replica: replica_name.clone(),
                    fault: verification_fault,
                });
            continue;
        }

        // Store valid blocks for comparison
        valid_replicas.insert(replica_name.clone(), (block, receipts, traces));
    }

    // Step 3: Group replicas by equivalent block data to find consensus
    if !valid_replicas.is_empty() {
        debug!(
            block_num,
            valid_count = valid_replicas.len(),
            "Finding consensus among valid replicas"
        );
        find_consensus(block_num, &valid_replicas, faults_by_replica, good_blocks);
    } else {
        debug!(block_num, "No valid replicas found for block");
    }
}

/// Finds consensus among replicas for a given block
fn find_consensus(
    block_num: u64,
    valid_replicas: &HashMap<String, (&Block, &BlockReceipts, &BlockTraces)>,
    faults_by_replica: &mut HashMap<String, Vec<Fault>>,
    good_blocks: &mut GoodBlocks,
) {
    // Group replicas by equivalent data
    let mut equivalence_groups: Vec<Vec<String>> = Vec::new();

    for (replica_name, (block, receipts, traces)) in valid_replicas {
        // Check if this block data matches any existing group
        let mut found_match = false;

        for group in &mut equivalence_groups {
            // Check against the first replica in the group
            let first_replica = group.first().unwrap();
            let (first_block, first_receipts, first_traces) =
                valid_replicas.get(first_replica).unwrap();

            // Direct equality comparison to group identical blocks
            if block == first_block && receipts == first_receipts && traces == first_traces {
                // Add to this group
                group.push(replica_name.clone());
                found_match = true;
                break;
            }
        }

        if !found_match {
            // Create a new group if no match was found
            equivalence_groups.push(vec![replica_name.clone()]);
        }
    }

    debug!(
        block_num,
        group_count = equivalence_groups.len(),
        "Found equivalence groups for block"
    );

    // Find the largest equivalence group - the majority consensus
    if let Some(largest_group) = equivalence_groups.iter().max_by_key(|group| group.len()) {
        // Sort the group for deterministic selection
        let mut sorted_group = largest_group.clone();
        sorted_group.sort(); // Sort lexicographically by replica name

        debug!(
            block_num,
            group_size = largest_group.len(),
            "Largest consensus group"
        );

        // The first replica in the sorted largest group is our "good" one
        if let Some(good_replica) = sorted_group.first() {
            debug!(
                block_num,
                %good_replica,
                "Selected good replica for block"
            );

            good_blocks
                .block_num_to_replica
                .insert(block_num, good_replica.clone());

            // Mark replicas not in this group as inconsistent
            for replica_name in valid_replicas.keys() {
                if !largest_group.contains(replica_name) {
                    debug!(
                        block_num,
                        %replica_name,
                        "Replica has inconsistent block data"
                    );

                    faults_by_replica
                        .entry(replica_name.clone())
                        .or_default()
                        .push(Fault {
                            block_num,
                            replica: replica_name.clone(),
                            fault: FaultKind::InconsistentBlock(find_inconsistent_reason(
                                *valid_replicas.get(good_replica).unwrap(),
                                *valid_replicas.get(replica_name).unwrap(),
                                replica_name,
                                block_num,
                            )),
                        });
                }
            }
        }
    } else {
        // This should not happen with proper consensus, but log critical error if it does
        error!(
            block_num,
            "No equivalence group found - unable to determine correct block data. Manual intervention required!"
        );

        // Mark all replicas as having unresolvable inconsistencies
        for replica_name in valid_replicas.keys() {
            faults_by_replica
                .entry(replica_name.clone())
                .or_default()
                .push(Fault {
                    block_num,
                    replica: replica_name.clone(),
                    fault: FaultKind::InconsistentBlock(InconsistentBlockReason::NoConsensus),
                });
        }
    }
}

/// Stores checking results in S3, including faults and good block references.
///
/// This function handles both storing new faults and clearing old faults that no longer exist.
/// For each replica, it either:
/// - Stores new faults if any were found
/// - Deletes the fault chunk if no faults exist but old faults were previously stored
/// - Does nothing if no faults exist and none were previously stored
///
/// # Arguments
/// * `model` - The checker model for accessing storage
/// * `starting_block_num` - The starting block number of the chunk
/// * `faults_by_replica` - Map of replica names to their faults
/// * `good_blocks` - Reference to replicas with correct data for each block
pub async fn store_checking_results(
    model: &CheckerModel,
    starting_block_num: u64,
    faults_by_replica: HashMap<String, Vec<Fault>>,
    good_blocks: GoodBlocks,
) -> Result<()> {
    // First, handle all replicas to ensure we clear faults for replicas that no longer have any
    for replica in model.block_data_readers.keys() {
        match faults_by_replica.get(replica) {
            Some(faults) if !faults.is_empty() => {
                // Replica has faults - store them
                info!(
                    %replica,
                    fault_count = faults.len(),
                    starting_block = starting_block_num,
                    "Storing faults for replica"
                );

                model
                    .set_faults_chunk(replica, starting_block_num, faults.clone())
                    .await?;
            }
            _ => {
                // Replica has no faults - check if we need to clear old faults
                let old_faults = model.get_faults_chunk(replica, starting_block_num).await?;
                if !old_faults.is_empty() {
                    info!(
                        %replica,
                        old_fault_count = old_faults.len(),
                        starting_block = starting_block_num,
                        "Clearing previously stored faults for replica"
                    );

                    // Delete the fault chunk since there are no faults anymore
                    model
                        .delete_faults_chunk(replica, starting_block_num)
                        .await?;
                } else {
                    debug!(
                        %replica,
                        starting_block = starting_block_num,
                        "No faults to store or clear for replica"
                    );
                }
            }
        }
    }

    // Store good blocks reference in S3
    info!(
        good_block_count = good_blocks.block_num_to_replica.len(),
        starting_block = starting_block_num,
        "Storing good block references"
    );

    model
        .set_good_blocks(starting_block_num, good_blocks)
        .await?;

    Ok(())
}

/// Verifies the internal consistency of a block and its associated data
fn verify_block(
    block_num: u64,
    block: &Block,
    receipts: &BlockReceipts,
    traces: &BlockTraces,
) -> Result<(), FaultKind> {
    // Verify block number
    if block.header.number != block_num {
        debug!(
            expected = block_num,
            actual = block.header.number,
            "Invalid block number"
        );
        return Err(FaultKind::InvalidBlockNumber {
            expected: block_num,
            actual: block.header.number,
        });
    }

    // Verify receipts match the block
    if receipts.len() != block.body.transactions.len() {
        debug!(
            block_num,
            tx_count = block.body.transactions.len(),
            receipt_count = receipts.len(),
            "Receipt count mismatch"
        );
        return Err(FaultKind::ReceiptCountMismatch {
            tx_count: block.body.transactions.len(),
            receipt_count: receipts.len(),
        });
    }

    // Verify traces match the block
    if traces.len() != block.body.transactions.len() {
        debug!(
            block_num,
            tx_count = block.body.transactions.len(),
            trace_count = traces.len(),
            "Trace count mismatch"
        );
        return Err(FaultKind::TraceCountMismatch {
            tx_count: block.body.transactions.len(),
            trace_count: traces.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use alloy_consensus::TxEnvelope;
    use alloy_primitives::Bytes;
    use alloy_rlp::Encodable;
    use monad_archive::{
        kvstore::memory::MemoryStorage,
        test_utils::{mock_block, mock_rx, mock_tx},
    };

    use super::*;
    use crate::model::InconsistentBlockReason;

    #[test]
    fn test_process_blocks_empty() {
        let data_by_block_num = HashMap::new();
        let (faults, good_blocks) = process_blocks(&data_by_block_num, 100, 110);

        assert!(faults.is_empty());
        assert!(good_blocks.block_num_to_replica.is_empty());
    }

    #[test]
    fn test_verify_block() {
        // Valid block case
        let block_num = 123;
        let block = create_test_block(block_num);
        let receipts = create_test_receipts(3); // 3 transactions
        let traces = create_test_traces(3);

        // Valid case should return Ok
        assert!(verify_block(block_num, &block, &receipts, &traces).is_ok());

        // Invalid block number
        let wrong_block_num = 456;
        match verify_block(wrong_block_num, &block, &receipts, &traces) {
            Err(FaultKind::InvalidBlockNumber { expected, actual }) => {
                assert_eq!(expected, wrong_block_num);
                assert_eq!(actual, block_num);
            }
            _ => panic!("Expected InvalidBlockNumber fault"),
        }

        // Receipt count mismatch
        let fewer_receipts = create_test_receipts(2); // Only 2 receipts for 3 transactions
        match verify_block(block_num, &block, &fewer_receipts, &traces) {
            Err(FaultKind::ReceiptCountMismatch {
                tx_count,
                receipt_count,
            }) => {
                assert_eq!(tx_count, 3);
                assert_eq!(receipt_count, 2);
            }
            _ => panic!("Expected ReceiptCountMismatch fault"),
        }
    }

    #[test]
    fn test_find_consensus() {
        let block_num = 123;
        let mut faults_by_replica = HashMap::new();
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Create three different valid blocks
        let (block1, receipts1, traces1) = create_test_block_data(block_num, 1);
        let (block2, receipts2, traces2) = create_test_block_data(block_num, 2);
        let (block3, receipts3, traces3) = create_test_block_data(block_num, 3);

        // Create a map with 3 replicas, two agreeing, one different
        let mut valid_replicas = HashMap::new();
        valid_replicas.insert("replica1".to_string(), (&block1, &receipts1, &traces1));
        valid_replicas.insert("replica2".to_string(), (&block1, &receipts1, &traces1)); // Same as replica1
        valid_replicas.insert("replica3".to_string(), (&block2, &receipts2, &traces2)); // Different

        find_consensus(
            block_num,
            &valid_replicas,
            &mut faults_by_replica,
            &mut good_blocks,
        );

        // Should select replica1 as the good replica (they're identical, but replica1 is chosen since it's first)
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));
        let selected_replica = good_blocks.block_num_to_replica.get(&block_num).unwrap();
        assert!(selected_replica == "replica1");

        // replica3 should be marked as inconsistent
        assert!(faults_by_replica.contains_key("replica3"));
        let replica3_faults = faults_by_replica.get("replica3").unwrap();
        assert_eq!(replica3_faults.len(), 1);
        assert!(matches!(
            replica3_faults[0].fault,
            FaultKind::InconsistentBlock(InconsistentBlockReason::Header)
        ));

        // Add another test with three different blocks
        faults_by_replica.clear();
        good_blocks.block_num_to_replica.clear();

        valid_replicas.clear();
        valid_replicas.insert("replica1".to_string(), (&block1, &receipts1, &traces1));
        valid_replicas.insert("replica2".to_string(), (&block2, &receipts2, &traces2));
        valid_replicas.insert("replica3".to_string(), (&block3, &receipts3, &traces3));

        find_consensus(
            block_num,
            &valid_replicas,
            &mut faults_by_replica,
            &mut good_blocks,
        );

        // Should select one replica as the good one
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));

        // The other two should be marked as inconsistent
        assert_eq!(faults_by_replica.len(), 2);
    }

    #[test]
    fn test_process_single_block() {
        let block_num = 123;
        let mut faults_by_replica = HashMap::new();
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Create test data for each replica
        let (block1, receipts1, traces1) = create_test_block_data(block_num, 1);
        let (block2, receipts2, traces2) = create_test_block_data(block_num, 2);

        // Create replica data with one valid, one invalid, and one missing
        let mut replica_data = HashMap::new();
        replica_data.insert("replica1".to_string(), Some((block1, receipts1, traces1)));
        replica_data.insert("replica2".to_string(), Some((block2, receipts2, traces2)));
        replica_data.insert("replica3".to_string(), None); // Missing data

        process_single_block(
            block_num,
            &replica_data,
            &mut faults_by_replica,
            &mut good_blocks,
        );

        // replica3 should be marked as missing
        assert!(faults_by_replica.contains_key("replica3"));
        assert!(matches!(
            faults_by_replica.get("replica3").unwrap()[0].fault,
            FaultKind::MissingBlock
        ));

        // One of replica1 or replica2 should be selected as good
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));

        // The non-selected replica should be marked as inconsistent
        assert_eq!(faults_by_replica.len(), 2); // replica3 and one other
    }

    // Helper functions to create test data
    fn create_test_block(block_num: u64) -> Block {
        // Use the existing mock_block utility with 3 transactions
        mock_block(block_num, vec![mock_tx(1), mock_tx(2), mock_tx(3)])
    }

    fn create_test_receipts(count: usize) -> BlockReceipts {
        // Use the existing mock_rx utility
        (0..count).map(|i| mock_rx(i, 1000 + i as u128)).collect()
    }

    fn create_test_traces(count: usize) -> BlockTraces {
        // Simple trace creation
        vec![b"trace".to_vec(); count]
    }

    pub(crate) fn create_test_block_data_with_len(
        block_num: u64,
        header_variant: u8,
        receipts_len: usize,
        traces_len: usize,
    ) -> (Block, BlockReceipts, BlockTraces) {
        let mut block = create_test_block(block_num);
        // Make the block slightly different based on variant
        block.header.extra_data = Bytes::from(vec![header_variant]);

        let receipts = create_test_receipts(receipts_len);
        let traces = create_test_traces(traces_len);

        (block, receipts, traces)
    }

    pub(crate) fn create_test_block_data(
        block_num: u64,
        header_variant: u8,
    ) -> (Block, BlockReceipts, BlockTraces) {
        create_test_block_data_with_len(block_num, header_variant, 3, 3)
    }

    #[test]
    fn test_process_blocks_with_mixed_data() {
        let block_num = 100;
        let mut data_by_block_num = HashMap::new();

        // Create three blocks with data
        for i in block_num..block_num + 3 {
            let mut replica_data = HashMap::new();

            // All replicas have identical data for block 100
            if i == block_num {
                let (block, receipts, traces) = create_test_block_data(i, 1);
                replica_data.insert(
                    "replica1".to_string(),
                    Some((block.clone(), receipts.clone(), traces.clone())),
                );
                replica_data.insert(
                    "replica2".to_string(),
                    Some((block.clone(), receipts.clone(), traces.clone())),
                );
                replica_data.insert("replica3".to_string(), Some((block, receipts, traces)));
            }
            // Two replicas agree, one is different for block 101
            else if i == block_num + 1 {
                let (block1, receipts1, traces1) = create_test_block_data(i, 1);
                let (block2, receipts2, traces2) = create_test_block_data(i, 2);
                replica_data.insert(
                    "replica1".to_string(),
                    Some((block1.clone(), receipts1.clone(), traces1.clone())),
                );
                replica_data.insert("replica2".to_string(), Some((block1, receipts1, traces1)));
                replica_data.insert("replica3".to_string(), Some((block2, receipts2, traces2)));
            }
            // One replica has data, one is missing, one has invalid block number for block 102
            else if i == block_num + 2 {
                let (mut block, receipts, traces) = create_test_block_data(i, 1);
                let (valid_block, valid_receipts, valid_traces) = create_test_block_data(i, 1);

                // Make block's number invalid
                block.header.number = i + 100; // Wrong block number

                replica_data.insert(
                    "replica1".to_string(),
                    Some((valid_block, valid_receipts, valid_traces)),
                );
                replica_data.insert("replica2".to_string(), None); // Missing
                replica_data.insert("replica3".to_string(), Some((block, receipts, traces)));
                // Invalid
            }

            data_by_block_num.insert(i, replica_data);
        }

        let (faults_by_replica, good_blocks) =
            process_blocks(&data_by_block_num, block_num, block_num + 2);

        // Check good blocks
        assert_eq!(good_blocks.block_num_to_replica.len(), 3);
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));
        assert!(good_blocks
            .block_num_to_replica
            .contains_key(&(block_num + 1)));
        assert!(good_blocks
            .block_num_to_replica
            .contains_key(&(block_num + 2)));

        // Check faults
        assert!(faults_by_replica.contains_key("replica3")); // Should have fault for block 101 and 102
        assert!(faults_by_replica.contains_key("replica2")); // Should have fault for block 102

        if let Some(replica3_faults) = faults_by_replica.get("replica3") {
            assert_eq!(replica3_faults.len(), 2);

            // First fault should be for block 101, inconsistent
            assert_eq!(replica3_faults[0].block_num, block_num + 1);
            assert!(matches!(
                replica3_faults[0].fault,
                FaultKind::InconsistentBlock(InconsistentBlockReason::Header)
            ));

            // Second fault should be for block 102, invalid block number
            assert_eq!(replica3_faults[1].block_num, block_num + 2);
            assert!(matches!(
                replica3_faults[1].fault,
                FaultKind::InvalidBlockNumber { .. }
            ));
        } else {
            panic!("Expected faults for replica3");
        }

        if let Some(replica2_faults) = faults_by_replica.get("replica2") {
            assert_eq!(replica2_faults.len(), 1);
            assert_eq!(replica2_faults[0].block_num, block_num + 2);
            assert!(matches!(replica2_faults[0].fault, FaultKind::MissingBlock));
        } else {
            panic!("Expected faults for replica2");
        }
    }

    #[test]
    fn test_verify_block_additional_cases() {
        let block_num = 123;
        let block = create_test_block(block_num);

        // Test with mismatched receipt and trace counts
        let receipts = create_test_receipts(3); // 3 transactions
        let traces = create_test_traces(2); // Only 2 traces

        // Check invalid trace counts cause error
        assert!(verify_block(block_num, &block, &receipts, &traces).is_err());

        // Test with empty receipts
        let empty_receipts = create_test_receipts(0);
        match verify_block(block_num, &block, &empty_receipts, &traces) {
            Err(FaultKind::ReceiptCountMismatch {
                tx_count,
                receipt_count,
            }) => {
                assert_eq!(tx_count, 3);
                assert_eq!(receipt_count, 0);
            }
            _ => panic!("Expected ReceiptCountMismatch fault"),
        }
    }

    pub(crate) fn setup_test_model() -> CheckerModel {
        let store: KVStoreErased = MemoryStorage::new("test-store").into();

        let mut block_data_readers = HashMap::new();

        // Create memory-based readers for testing
        for i in 1..=3 {
            let reader_store: KVStoreErased = MemoryStorage::new(format!("reader-{}", i)).into();
            let reader = BlockDataArchive::new(reader_store);
            block_data_readers.insert(format!("replica{}", i), reader);
        }

        CheckerModel {
            store,
            block_data_readers,
        }
    }

    async fn populate_test_replicas(
        model: &CheckerModel,
        block_range: RangeInclusive<u64>,
        skip_3: bool,
    ) {
        // Add test blocks to all replicas, with some variations
        for block_num in block_range {
            // For replica1 and replica2, add identical data (consensus)
            let (block1, receipts1, traces1) = create_test_block_data(block_num, 1);

            if let Some(archiver) = model.block_data_readers.get("replica1") {
                archiver.archive_block(block1.clone()).await.unwrap();
                archiver
                    .archive_receipts(receipts1.clone(), block_num)
                    .await
                    .unwrap();
                archiver
                    .archive_traces(traces1.clone(), block_num)
                    .await
                    .unwrap();
                archiver
                    .update_latest(block_num, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }

            if let Some(archiver) = model.block_data_readers.get("replica2") {
                archiver.archive_block(block1.clone()).await.unwrap();
                archiver
                    .archive_receipts(receipts1.clone(), block_num)
                    .await
                    .unwrap();
                archiver
                    .archive_traces(traces1.clone(), block_num)
                    .await
                    .unwrap();
                archiver
                    .update_latest(block_num, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }

            if !skip_3 {
                // For replica3, add slightly different data (will be marked as inconsistent)
                let (block3, receipts3, traces3) = if block_num % 3 == 0 {
                    // Every third block is different
                    create_test_block_data(block_num, 2)
                } else {
                    // Otherwise identical to others
                    (block1, receipts1, traces1)
                };

                if let Some(archiver) = model.block_data_readers.get("replica3") {
                    archiver.archive_block(block3).await.unwrap();
                    archiver
                        .archive_receipts(receipts3, block_num)
                        .await
                        .unwrap();
                    archiver.archive_traces(traces3, block_num).await.unwrap();
                    archiver
                        .update_latest(block_num, LatestKind::Uploaded)
                        .await
                        .unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn test_process_block_batch() {
        // Setup test model with memory storage
        let model = setup_test_model();

        // Populate replicas with test data for blocks 100-110
        let start_block = 100;
        let end_block = 110;
        populate_test_replicas(&model, start_block..=end_block, false).await;

        // Process the batch
        let next_block = process_block_batch(&model, start_block, end_block)
            .await
            .unwrap();

        // Verify the result is the next block after end_block
        assert_eq!(next_block, end_block + 1);

        // Verify faults were stored for replica3 (every third block should be inconsistent)
        for block_num in start_block..=end_block {
            if block_num % 3 == 0 {
                // Check that faults were recorded for replica3
                let faults = model
                    .get_faults_chunk("replica3", start_block)
                    .await
                    .unwrap();
                let has_fault_for_block = faults.iter().any(|f| {
                    f.block_num == block_num
                        && matches!(
                            f.fault,
                            FaultKind::InconsistentBlock(InconsistentBlockReason::Header)
                        )
                });
                assert!(
                    has_fault_for_block,
                    "Expected fault for block {}",
                    block_num
                );
            }
        }

        // Verify good blocks were stored
        let good_blocks = model.get_good_blocks(start_block).await.unwrap();
        assert_eq!(
            good_blocks.block_num_to_replica.len(),
            (end_block - start_block + 1) as usize
        );

        // Verify latest checked was updated for all replicas
        for i in 1..=3 {
            let replica_name = format!("replica{}", i);
            let latest_checked = model
                .get_latest_checked_for_replica(&replica_name)
                .await
                .unwrap();
            assert_eq!(latest_checked, end_block);
        }
    }

    #[tokio::test]
    async fn test_process_block_batch_with_missing_data() {
        // Setup test model with memory storage
        let model = setup_test_model();

        // Populate replicas with test data for blocks 200-210
        let start_block = 200;
        let end_block = 210;
        populate_test_replicas(&model, start_block..=end_block, true).await;

        // For replica3, leave block 205 missing (don't populate it)
        let missing_block = 205;
        if let Some(archiver) = model.block_data_readers.get("replica3") {
            // For each block except the missing one, archive the data
            for block_num in start_block..=end_block {
                if block_num == missing_block {
                    continue; // Skip this block
                }

                let (block, receipts, traces) = if block_num % 3 == 0 {
                    create_test_block_data(block_num, 2)
                } else {
                    create_test_block_data(block_num, 1)
                };

                archiver.archive_block(block).await.unwrap();
                archiver
                    .archive_receipts(receipts, block_num)
                    .await
                    .unwrap();
                archiver.archive_traces(traces, block_num).await.unwrap();
            }
            archiver
                .update_latest(end_block, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        // Process the batch
        let next_block = process_block_batch(&model, start_block, end_block)
            .await
            .unwrap();

        // Verify the result is the next block after end_block
        assert_eq!(next_block, end_block + 1);

        // Verify missing block fault was stored for replica3
        let faults = model
            .get_faults_chunk("replica3", start_block)
            .await
            .unwrap();
        eprintln!("{faults:?}");
        let has_missing_fault = faults
            .iter()
            .any(|f| f.block_num == missing_block && matches!(f.fault, FaultKind::MissingBlock));
        assert!(
            has_missing_fault,
            "Expected MissingBlock fault for block {missing_block}"
        );
    }

    #[tokio::test]
    async fn test_process_block_batch_with_invalid_block_number() {
        // Setup test model with memory storage
        let model = setup_test_model();

        // Populate replicas with test data for blocks 300-310
        let start_block = 300;
        let end_block = 310;

        let invalid_block_num = 305;
        // First populate replica1 and replica2 with normal data
        for replica_name in ["replica1", "replica2", "replica3"] {
            if let Some(archiver) = model.block_data_readers.get(replica_name) {
                for block_num in start_block..=end_block {
                    let (block, receipts, traces) = create_test_block_data(block_num, 1);

                    if replica_name == "replica3" && block_num == invalid_block_num {
                        let block = alloy_consensus::Block::<TxEnvelope> {
                            header: Header {
                                number: 999,
                                timestamp: 1234567,
                                base_fee_per_gas: Some(100),
                                ..Default::default()
                            },
                            body: BlockBody {
                                transactions: Vec::<TxEnvelope>::new(),
                                ommers: vec![],
                                withdrawals: None,
                            },
                        };

                        // 1) Insert into block table
                        let block_key = archiver.block_key(invalid_block_num);

                        // 3) Encode into storage repr
                        let mut encoded_block = Vec::new();
                        block.encode(&mut encoded_block);

                        // must put the block directly
                        archiver.store.put(&block_key, encoded_block).await.unwrap();
                        archiver
                            .archive_receipts(receipts, block_num)
                            .await
                            .unwrap();
                        archiver.archive_traces(traces, block_num).await.unwrap();
                        continue;
                    }

                    archiver.archive_block(block).await.unwrap();
                    archiver
                        .archive_receipts(receipts, block_num)
                        .await
                        .unwrap();
                    archiver.archive_traces(traces, block_num).await.unwrap();
                }
                archiver
                    .update_latest(end_block, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }
        }

        // Process the batch
        let next_block = process_block_batch(&model, start_block, end_block)
            .await
            .unwrap();

        // Verify the result is the next block after end_block
        assert_eq!(next_block, end_block + 1);

        // Verify invalid block number fault was stored for replica3
        let faults = model
            .get_faults_chunk("replica3", start_block)
            .await
            .unwrap();
        eprintln!("{faults:?}");
        let has_invalid_block_fault = faults.iter().any(|f| {
            f.block_num == invalid_block_num
                && matches!(f.fault, FaultKind::InvalidBlockNumber { .. })
        });
        assert!(
            has_invalid_block_fault,
            "Expected InvalidBlockNumber fault for block {invalid_block_num}"
        );
    }
}
