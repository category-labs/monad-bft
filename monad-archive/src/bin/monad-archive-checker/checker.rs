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

use std::collections::HashMap;

use alloy_consensus::proofs::{calculate_receipt_root, calculate_transaction_root};
use eyre::Result;
use futures::stream;
use monad_archive::prelude::*;

use crate::{
    model::{CheckerModel, Fault, FaultKind, GoodBlocks, InconsistentBlockReason},
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
    concurrency: usize,
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
        next_to_check =
            process_block_batch(&model, next_to_check, end_block, concurrency, &metrics).await?;
        info!(new_next_to_check = next_to_check, "Block batch processed");
    }
}

/// Verifies a batch of blocks, stores the results, and returns the next block to check.
///
/// Concurrently prefetches every block's per-replica checksums, then walks the blocks in
/// order preferring the fast path: when all present replicas agree it downloads one body
/// to run `verify_block` and marks them good, eliminating the other N-1 fetches; on
/// disagreement it falls back to [`check_block_via_full_body_fetch`]. The walk is
/// sequential so each block's parent header threads into the next block's parent-link
/// check (mirroring [`get_prev_header`]).
async fn process_block_batch(
    model: &CheckerModel,
    start_block: u64,
    end_block: u64,
    concurrency: usize,
    metrics: &Metrics,
) -> Result<u64> {
    let replicas = model
        .block_data_readers
        .keys()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    info!(
        replica_count = replicas.len(),
        "Fetching block data checksums from replicas"
    );

    // Phase 1: concurrently prefetch every block's per-replica checksums.
    let checksums_by_block =
        fetch_block_data_checksums(model, start_block..=end_block, &replicas, concurrency).await;

    // Phase 2: walk blocks in order, deciding from the prefetched checksums.
    let mut faults_by_replica: HashMap<String, Vec<Fault>> = HashMap::new();
    let mut good_blocks = GoodBlocks {
        block_num_to_replica: HashMap::new(),
    };
    // Each replica's previous-block header for the parent-link check, rebuilt after every
    // block (only present replicas carry forward), mirroring `get_prev_header`. Starts empty.
    let mut prev_headers: HashMap<String, Header> = HashMap::new();

    for block_num in start_block..=end_block {
        let block_checksums = checksums_by_block.get(&block_num);

        let outcome =
            try_check_block_via_checksums(model, block_num, block_checksums, &prev_headers).await?;
        metrics.inc_counter(outcome.fast_path_metric());
        match outcome {
            FastPathOutcome::Verified {
                good_replicas,
                parent_faulted_replicas,
                missing_replicas,
                header,
            } => {
                // One canonical replica per good block: the lex-smallest good replica
                // (matches `choose_good_replica`, which sorts and takes the smallest).
                if let Some(good_replica) = good_replicas.iter().min() {
                    good_blocks
                        .block_num_to_replica
                        .insert(block_num, good_replica.clone());
                }
                for replica in &parent_faulted_replicas {
                    record_fault(
                        &mut faults_by_replica,
                        block_num,
                        replica,
                        FaultKind::InconsistentBlock(InconsistentBlockReason::InvalidParentHash),
                    );
                }
                record_missing(&mut faults_by_replica, block_num, &missing_replicas);

                prev_headers =
                    headers_for_present(&header, &good_replicas, &parent_faulted_replicas);
            }
            FastPathOutcome::VerifyFailed {
                fault,
                fault_replicas,
                missing_replicas,
                header,
            } => {
                for replica in &fault_replicas {
                    record_fault(&mut faults_by_replica, block_num, replica, fault.clone());
                }
                record_missing(&mut faults_by_replica, block_num, &missing_replicas);

                prev_headers = headers_for_present(&Some(header), &fault_replicas, &[]);
            }
            FastPathOutcome::Disagreement => {
                // Fall back to the full-download path; it records all faults/good blocks
                // and returns each replica's data, from which we thread the next parents.
                let replica_data = check_block_via_full_body_fetch(
                    model,
                    block_num,
                    &replicas,
                    &prev_headers,
                    &mut faults_by_replica,
                    &mut good_blocks,
                )
                .await;

                prev_headers = replica_data
                    .into_iter()
                    .filter_map(|(replica, data)| Some((replica, data?.0.header)))
                    .collect();
            }
        }
    }

    // Count total faults and good blocks
    let total_faults: usize = faults_by_replica.values().map(|v| v.len()).sum();
    let total_good_blocks = good_blocks.block_num_to_replica.len();

    info!(
        total_faults,
        total_good_blocks, "Found faults and good blocks"
    );

    // Store results in S3
    info!("Storing checking results");
    store_checking_results(model, start_block, &faults_by_replica, good_blocks).await?;

    // Update the latest checked block for each replica
    info!(end_block, "Updating latest checked block");
    for replica_name in model.block_data_readers.keys() {
        model
            .set_latest_checked_for_replica(replica_name, end_block)
            .await?;
    }

    Ok(end_block + 1)
}

fn record_fault(
    faults_by_replica: &mut HashMap<String, Vec<Fault>>,
    block_num: u64,
    replica: &str,
    fault: FaultKind,
) {
    faults_by_replica
        .entry(replica.to_owned())
        .or_default()
        .push(Fault {
            block_num,
            replica: replica.to_owned(),
            fault,
        });
}

fn record_missing(
    faults_by_replica: &mut HashMap<String, Vec<Fault>>,
    block_num: u64,
    missing_replicas: &[String],
) {
    for replica in missing_replicas {
        debug!(block_num, %replica, "Missing block in replica");
        record_fault(
            faults_by_replica,
            block_num,
            replica,
            FaultKind::MissingBlock,
        );
    }
}

/// Maps every present replica (good or parent-faulted) to the single shared header for
/// the next block's parent check; empty when no replica was present.
fn headers_for_present(
    header: &Option<Header>,
    good_replicas: &[String],
    parent_faulted_replicas: &[String],
) -> HashMap<String, Header> {
    let Some(header) = header else {
        return HashMap::new();
    };
    good_replicas
        .iter()
        .chain(parent_faulted_replicas)
        .map(|replica| (replica.clone(), header.clone()))
        .collect()
}

/// Concurrently fetches the per-replica [`BlockDataChecksums`] for a range of blocks,
/// mirroring [`fetch_block_data`] but transferring only checksums. A per-replica fetch
/// error is swallowed to `None` (that replica is treated as missing the block), so one
/// replica's backend outage records a `MissingBlock` rather than aborting the run.
pub async fn fetch_block_data_checksums(
    model: &CheckerModel,
    block_nums: impl IntoIterator<Item = u64>,
    replicas: &[&str],
    concurrency: usize,
) -> HashMap<u64, HashMap<String, Option<BlockDataChecksums>>> {
    stream::iter(block_nums)
        .map(|block_num| async move {
            // A per-replica error is logged and treated as that replica missing the block.
            let per_replica =
                futures::future::join_all(replicas.iter().map(|&replica_name| async move {
                    let checksums = match model
                        .fetch_block_data_checksums_for_replica(block_num, replica_name)
                        .await
                    {
                        Ok(checksums) => checksums,
                        Err(e) => {
                            warn!(
                                block_num,
                                %replica_name,
                                ?e,
                                "Failed to fetch block data checksums, treating replica as missing this block"
                            );
                            None
                        }
                    };
                    (replica_name.to_owned(), checksums)
                }))
                .await;
            (block_num, per_replica.into_iter().collect())
        })
        .buffered(concurrency)
        .collect::<Vec<(u64, HashMap<String, Option<BlockDataChecksums>>)>>()
        .await
        .into_iter()
        .collect()
}

/// Outcome of the checksum fast path for one block. Replicas are partitioned into
/// *present* (all three checksums found) and *missing* (`MissingBlock`). When all present
/// replicas agree we download one body and verify it; otherwise we report
/// [`Disagreement`] for the caller's full-body fallback.
///
/// The one downloaded `header` (shared by all present replicas, since they hold identical
/// bytes) is threaded into the next block's per-replica parent check; missing replicas
/// drop out.
enum FastPathOutcome {
    /// Present replicas agreed and the shared body passed intra-block checks; the
    /// parent-link check splits them into `good_replicas` and `parent_faulted_replicas`.
    /// `header` is `None` only when no replica was present.
    Verified {
        good_replicas: Vec<String>,
        parent_faulted_replicas: Vec<String>,
        missing_replicas: Vec<String>,
        header: Option<Header>,
    },
    /// Present replicas agreed but the shared body failed intra-block verification; every
    /// present replica gets `fault`.
    VerifyFailed {
        fault: FaultKind,
        fault_replicas: Vec<String>,
        missing_replicas: Vec<String>,
        header: Header,
    },
    /// Present replicas disagreed (or the canonical body failed to download); the caller
    /// runs [`check_block_via_full_body_fetch`], which records everything for this block.
    Disagreement,
}

impl FastPathOutcome {
    fn fast_path_metric(&self) -> MetricNames {
        match self {
            FastPathOutcome::Verified { .. } | FastPathOutcome::VerifyFailed { .. } => {
                MetricNames::ARCHIVE_CHECKER_FAST_PATH_HITS
            }
            FastPathOutcome::Disagreement => MetricNames::ARCHIVE_CHECKER_FAST_PATH_MISSES,
        }
    }
}

/// Verifies one block from checksums alone, downloading at most one body. `block_checksums`
/// is the prefetched per-replica checksums (`None` = not fully present); `prev_headers`
/// feeds the per-replica parent check. See [`FastPathOutcome`].
async fn try_check_block_via_checksums(
    model: &CheckerModel,
    block_num: u64,
    block_checksums: Option<&HashMap<String, Option<BlockDataChecksums>>>,
    prev_headers: &HashMap<String, Header>,
) -> Result<FastPathOutcome> {
    // Partition into present (Some checksums) and missing (None / absent).
    let mut present: Vec<(String, BlockDataChecksums)> = Vec::new();
    let mut missing_replicas: Vec<String> = Vec::new();
    if let Some(block_checksums) = block_checksums {
        for (replica, checksums) in block_checksums {
            match checksums {
                Some(checksums) => present.push((replica.clone(), *checksums)),
                None => missing_replicas.push(replica.clone()),
            }
        }
    }

    // No present replicas: nothing to verify, no good block, no header to thread.
    let Some((_, first_checksums)) = present.first() else {
        return Ok(FastPathOutcome::Verified {
            good_replicas: Vec::new(),
            parent_faulted_replicas: Vec::new(),
            missing_replicas,
            header: None,
        });
    };

    // Disagreement: present replicas do not all share identical checksums.
    if !present
        .iter()
        .all(|(_, checksums)| checksums == first_checksums)
    {
        debug!(block_num, "Replicas disagree on checksums, falling back");
        return Ok(FastPathOutcome::Disagreement);
    }

    // All present replicas agree: download exactly one body (the lex-smallest present
    // replica) and verify it. Its result is shared by every present replica because they
    // all hold identical bytes.
    let canonical = present
        .iter()
        .map(|(replica, _)| replica)
        .min()
        .expect("present is non-empty");

    let Some((block, receipts, traces)) = model
        .fetch_block_data_for_replica(block_num, canonical)
        .await
    else {
        // Checksums agreed but the canonical body would not download/decode. Treat as a
        // disagreement so the full-body fallback handles it robustly.
        debug!(
            block_num,
            %canonical,
            "Canonical body unavailable despite checksum agreement, falling back"
        );
        return Ok(FastPathOutcome::Disagreement);
    };

    // Intra-block checks only; the parent link is checked per replica below.
    //
    // NOTE: on intra-block failure every present replica gets that one fault. The old
    // `verify_block` checks the parent before the tx/receipt roots, so if all present
    // replicas hold byte-identical intra-invalid data *and* a replica's parent is broken,
    // the old code reported `InvalidParentHash` there while we report the intra reason --
    // same faulted set, only the reason differs, only under cluster-wide identical
    // corruption. Intentional, so roots are hashed once per block, not once per replica.
    if let Err(fault) = verify_block(block_num, &block, &receipts, &traces, None) {
        debug!(block_num, ?fault, "Shared body failed verification");
        let fault_replicas = present.into_iter().map(|(replica, _)| replica).collect();
        return Ok(FastPathOutcome::VerifyFailed {
            fault,
            fault_replicas,
            missing_replicas,
            header: block.header,
        });
    }

    // Per-replica parent-link check: all present replicas share `parent_hash`, but each is
    // linked against its own previous block, so divergent parents fault individually.
    let mut good_replicas = Vec::new();
    let mut parent_faulted_replicas = Vec::new();
    for (replica, _) in present {
        match prev_headers.get(&replica) {
            Some(parent) if block.header.parent_hash != parent.hash_slow() => {
                debug!(
                    block_num,
                    %replica,
                    expected = %parent.hash_slow(),
                    actual = %block.header.parent_hash,
                    "Invalid parent hash"
                );
                parent_faulted_replicas.push(replica);
            }
            _ => good_replicas.push(replica),
        }
    }

    Ok(FastPathOutcome::Verified {
        good_replicas,
        parent_faulted_replicas,
        missing_replicas,
        header: Some(block.header),
    })
}

/// The disagreement (or defensive download-failure) fallback: downloads every replica's
/// full body for `block_num` and runs the unchanged [`process_single_block`], recording
/// all faults/good blocks. Returns each replica's data so the caller can thread parents.
async fn check_block_via_full_body_fetch(
    model: &CheckerModel,
    block_num: u64,
    replicas: &[&str],
    prev_headers: &HashMap<String, Header>,
    faults_by_replica: &mut HashMap<String, Vec<Fault>>,
    good_blocks: &mut GoodBlocks,
) -> HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>> {
    // Fetch all replicas' bodies for this single block. Fanned out concurrently across
    // replicas; the resulting map is identical to what `fetch_block_data` produces.
    let fetched = futures::future::join_all(replicas.iter().map(|&replica_name| async move {
        let data = model
            .fetch_block_data_for_replica(block_num, replica_name)
            .await;
        (replica_name.to_owned(), data)
    }))
    .await;
    let replica_data: HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>> =
        fetched.into_iter().collect();

    process_single_block(
        block_num,
        &replica_data,
        prev_headers.clone(),
        faults_by_replica,
        good_blocks,
    );

    replica_data
}

/// Fetches block data from all replicas for a range of blocks.
pub async fn fetch_block_data(
    model: &CheckerModel,
    block_nums: impl IntoIterator<Item = u64>,
    replicas: &[&str],
    concurrency: usize,
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
        .buffered(concurrency)
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
            let prev_headers = get_prev_header(block_num, data_by_block_num);

            debug!(block_num, "Processing block");
            process_single_block(
                block_num,
                replica_data,
                prev_headers,
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

fn get_prev_header(
    block_num: u64,
    data_by_block_num: &HashMap<u64, HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>>,
) -> HashMap<String, Header> {
    let Some(prev_block_num) = block_num.checked_sub(1) else {
        return HashMap::new();
    };
    let Some(prev_block_data) = data_by_block_num.get(&prev_block_num) else {
        return HashMap::new();
    };
    prev_block_data
        .iter()
        .filter_map(|(replica_name, block_data)| {
            Some((replica_name.clone(), block_data.as_ref()?.0.header.clone()))
        })
        .collect()
}

/// Processes a single block across all replicas
pub fn process_single_block(
    block_num: u64,
    replica_data: &HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>,
    parents: HashMap<String, Header>,
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
        let parent = parents.get(replica_name);
        if let Err(verification_fault) = verify_block(block_num, block, receipts, traces, parent) {
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
    let mut equivalence_groups: Vec<Vec<(String, &BlockTraces)>> = Vec::new();

    for (replica_name, (block, receipts, traces)) in valid_replicas {
        // Check if this block data matches any existing group
        let mut found_match = false;

        for group in &mut equivalence_groups {
            // Check against the first replica in the group
            let (first_replica, _) = group.first().unwrap();
            let (first_block, first_receipts, first_traces) =
                valid_replicas.get(first_replica).unwrap();

            // Direct equality comparison to group identical blocks
            if block == first_block && receipts == first_receipts && traces == first_traces {
                // Add to this group
                group.push((replica_name.clone(), traces));
                found_match = true;
                break;
            }
        }

        if !found_match {
            // Create a new group if no match was found
            equivalence_groups.push(vec![(replica_name.clone(), traces)]);
        }
    }

    debug!(
        block_num,
        group_count = equivalence_groups.len(),
        "Found equivalence groups for block"
    );

    if let Some(good_group) = choose_good_replica(&equivalence_groups, block_num) {
        let good_replica = good_group.first().unwrap();

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
            if !good_group.contains(replica_name) {
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

/// The "good" replica group is the one with the fewest non-empty outputs.
/// If there are multiple groups with the same number of non-empty outputs,
/// the largest group is chosen.
/// The replica in the group with the lexicographically smallest name is chosen as the "good" replica from the group.
fn choose_good_replica(
    equivalence_groups: &Vec<Vec<(String, &BlockTraces)>>,
    block_num: u64,
) -> Option<Vec<String>> {
    let good_group = if equivalence_groups.len() > 1 {
        // Sort by number of non-empty outputs, then by group size, then by lexicographically smallest replica name
        equivalence_groups.iter().min_by_key(|group| {
            let (_replica, traces) = group.first().expect("group is empty");

            let x = match decode_traces(traces) {
                Ok(x) => x,
                Err(e) => {
                    warn!(?e, "decode_traces failed");
                    return (0, 0, None);
                }
            };
            let num_non_empty_outputs = x
                .iter()
                .flatten()
                .flatten()
                .filter(|x| !x.output.is_empty())
                .count();

            // Take the negative of the values to make the min_by_key function sort in ascending order
            (
                -(num_non_empty_outputs as isize),
                -(group.len() as isize),
                // Take the lexicographically smallest replica name
                group.iter().map(|(replica, _)| replica).min(),
            )
        })?
    } else {
        equivalence_groups.first()?
    };

    if good_group.is_empty() {
        return None;
    }

    let mut good_replicas = good_group
        .iter()
        .map(|(replica, _)| replica.clone())
        .collect::<Vec<_>>();

    debug!(block_num, ?good_replicas, "Good replicas");

    good_replicas.sort();
    Some(good_replicas)
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
    faults_by_replica: &HashMap<String, Vec<Fault>>,
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
        };
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
    parent: Option<&Header>,
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

    if let Some(parent) = parent {
        if block.header.parent_hash != parent.hash_slow() {
            debug!(
                expected = %parent.hash_slow(),
                actual = %block.header.parent_hash,
                "Invalid parent hash"
            );
            return Err(FaultKind::InconsistentBlock(
                InconsistentBlockReason::InvalidParentHash,
            ));
        }
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

    // Verify transaction root
    {
        let txs = block
            .body
            .transactions
            .iter()
            .map(|tx| tx.tx.clone())
            .collect::<Vec<_>>();
        let tx_root = calculate_transaction_root(&txs);
        if block.header.transactions_root != tx_root {
            debug!(
                expected = %tx_root,
                actual = %block.header.transactions_root,
                "Invalid transaction root"
            );
            return Err(FaultKind::InconsistentBlock(
                InconsistentBlockReason::InvalidTransactionRoot,
            ));
        }
    }

    // Verify receipts root
    {
        let receipts = receipts
            .iter()
            .map(|r| r.receipt.clone())
            .collect::<Vec<_>>();
        let receipt_root = calculate_receipt_root(&receipts);
        if block.header.receipts_root != receipt_root {
            debug!(
                expected = %receipt_root,
                actual = %block.header.receipts_root,
                "Invalid receipts root"
            );
            return Err(FaultKind::InconsistentBlock(
                InconsistentBlockReason::InvalidReceiptsRoot,
            ));
        }
    }
    Ok(())
}

/// Determines the specific reason why two blocks are inconsistent
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
    } else if good_traces.len() != faulty_traces.len()
        || good_traces.iter().flatten().count() != faulty_traces.iter().flatten().count()
    {
        debug!(
            %replica,
            block_num,
            good_len = good_traces.len(),
            faulty_len = faulty_traces.len(),
            good_total_len = good_traces.iter().flatten().count(),
            faulty_total_len = faulty_traces.iter().flatten().count(),
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
        for (i, (good_trace, faulty_trace)) in
            good_traces.iter().zip(faulty_traces.iter()).enumerate()
        {
            if good_trace == faulty_trace {
                continue;
            }

            let Ok(good_call_frames) = decode_trace(good_trace) else {
                return InconsistentBlockReason::TracesContents;
            };
            let Ok(faulty_call_frames) = decode_trace(faulty_trace) else {
                return InconsistentBlockReason::TracesContents;
            };

            if good_call_frames == faulty_call_frames {
                continue;
            }

            for (good_call_frame, faulty_call_frame) in
                good_call_frames.iter().zip(faulty_call_frames.iter())
            {
                for (g, f) in good_call_frame.iter().zip(faulty_call_frame.iter()) {
                    if g.typ != f.typ
                        || g.flags != f.flags
                        || g.from != f.from
                        || g.to != f.to
                        || g.value != f.value
                        || g.gas != f.gas
                        || g.gas_used != f.gas_used
                        || g.input != f.input
                    {
                        debug!(
                            %replica,
                            block_num,
                            trace_index = i,
                            "Trace contents differ"
                        );
                        return InconsistentBlockReason::TracesContents;
                    }

                    if g.output != f.output {
                        debug!(
                            %replica,
                            block_num,
                            trace_index = i,
                            "Trace output differs"
                        );
                        return InconsistentBlockReason::TracesOutputDiffers;
                    }
                }
            }

            debug!(
                %replica,
                block_num,
                count,
                trace_index = i,
                ?good_trace,
                ?faulty_trace,
                "Trace does not match good replica"
            );

            count += 1;
            if count > 10 {
                return InconsistentBlockReason::TracesContents;
            }
        }
        InconsistentBlockReason::Unknown
    }
}

#[cfg(test)]
pub mod tests {
    use alloy_primitives::{Address, Bytes, FixedBytes, Log, LogData, U8};
    use alloy_rlp::Encodable;
    use monad_archive::{
        kvstore::{memory::MemoryStorage, WritePolicy},
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
        let (parent, _, _) = create_test_block_data(block_num - 1, 1, None);
        let (block, receipts, traces) =
            create_test_block_data(block_num, 1, Some(parent.header.hash_slow()));

        // Valid case should return Ok
        assert!(verify_block(block_num, &block, &receipts, &traces, Some(&parent.header)).is_ok());

        // Invalid block number
        let wrong_block_num = 456;
        match verify_block(
            wrong_block_num,
            &block,
            &receipts,
            &traces,
            Some(&parent.header),
        ) {
            Err(FaultKind::InvalidBlockNumber { expected, actual }) => {
                assert_eq!(expected, wrong_block_num);
                assert_eq!(actual, block_num);
            }
            _ => panic!("Expected InvalidBlockNumber fault"),
        }

        // Receipt count mismatch
        let fewer_receipts = create_test_receipts(2); // Only 2 receipts for 3 transactions
        match verify_block(
            block_num,
            &block,
            &fewer_receipts,
            &traces,
            Some(&parent.header),
        ) {
            Err(FaultKind::ReceiptCountMismatch {
                tx_count,
                receipt_count,
            }) => {
                assert_eq!(tx_count, 3);
                assert_eq!(receipt_count, 2);
            }
            _ => panic!("Expected ReceiptCountMismatch fault"),
        }

        // Invalid parent
        match verify_block(block_num, &block, &receipts, &traces, Some(&block.header)) {
            Err(FaultKind::InconsistentBlock(InconsistentBlockReason::InvalidParentHash)) => {}
            out => panic!(
                "Expected InconsistentBlock(InvalidParentHash) fault, got {:?}",
                out
            ),
        }
    }

    #[test]
    fn test_find_consensus() {
        let block_num = 123;
        let mut faults_by_replica = HashMap::new();
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Create 2 different valid blocks
        let (block1, receipts1, traces1) = create_test_block_data(block_num, 1, None);
        let (block3, receipts3, traces3) = create_test_block_data(block_num, 3, None);

        // Create a map with 3 replicas, two agreeing, one different
        let mut valid_replicas = HashMap::new();
        valid_replicas.insert("replica1".into(), (&block1, &receipts1, &traces1));
        valid_replicas.insert("replica2".into(), (&block1, &receipts1, &traces1)); // Same as replica1
        valid_replicas.insert("replica3".into(), (&block3, &receipts3, &traces3)); // Different

        find_consensus(
            block_num,
            &valid_replicas,
            &mut faults_by_replica,
            &mut good_blocks,
        );

        // Should select replica1 as the good replica (they're identical, but replica1 is chosen since it's first)
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));
        let selected_replica = good_blocks.block_num_to_replica.get(&block_num).unwrap();
        assert_eq!(selected_replica, "replica1");

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

        // Create three different valid blocks
        let (block1, receipts1, traces1) = create_test_block_data(block_num, 1, None);
        let (block2, receipts2, traces2) = create_test_block_data(block_num, 1, None);
        let (block3, receipts3, traces3) = create_test_block_data(block_num, 3, None);

        valid_replicas.clear();
        valid_replicas.insert("replica1".into(), (&block1, &receipts1, &traces1));
        valid_replicas.insert("replica2".into(), (&block2, &receipts2, &traces2));
        valid_replicas.insert("replica3".into(), (&block3, &receipts3, &traces3));

        find_consensus(
            block_num,
            &valid_replicas,
            &mut faults_by_replica,
            &mut good_blocks,
        );

        // Should select one replica as the good one
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));

        // The other two should be marked as inconsistent
        assert_eq!(faults_by_replica.len(), 1);
    }

    #[test]
    fn test_process_single_block() {
        let block_num = 123;
        let mut faults_by_replica = HashMap::new();
        let mut good_blocks = GoodBlocks {
            block_num_to_replica: HashMap::new(),
        };

        // Create test data for each replica
        let mut blocks = create_test_block_data_range(block_num, [1, 2]);

        // Create replica data with one valid, one invalid, and one missing
        let mut replica_data = HashMap::new();
        replica_data.insert("replica3".to_string(), None); // Missing data
        replica_data.insert("replica2".to_string(), blocks.remove(&(block_num + 1)));
        replica_data.insert("replica1".to_string(), blocks.remove(&block_num));

        process_single_block(
            block_num,
            &replica_data,
            HashMap::new(),
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

    fn create_test_receipts(count: usize) -> BlockReceipts {
        // Use the existing mock_rx utility
        (0..count).map(|i| mock_rx(i, 1000 + i as u64)).collect()
    }

    fn create_test_traces(count: usize) -> BlockTraces {
        let call_frame = CallFrame {
            typ: CallKind::Call,
            flags: U64::from(1),
            from: Address::default(),
            to: None,
            value: U256::from(1),
            gas: U64::from(1),
            gas_used: U64::from(1),
            input: Bytes::from(vec![1, 2, 3]),
            output: Bytes::from(vec![4, 5, 6]),
            status: U8::from(1),
            depth: U64::from(1),
            logs: Some(vec![
                CallFrameLog {
                    log: Log {
                        address: Address::default(),
                        data: LogData::new(
                            vec![FixedBytes::<32>::from([1u8; 32])],
                            Bytes::from(vec![7, 8, 9]),
                        )
                        .unwrap(),
                    },
                    position: U64::from(1),
                },
                CallFrameLog {
                    log: Log {
                        address: Address::default(),
                        data: LogData::new(
                            vec![
                                FixedBytes::<32>::from([1u8; 32]),
                                FixedBytes::<32>::from([2u8; 32]),
                                FixedBytes::<32>::from([3u8; 32]),
                                FixedBytes::<32>::from([4u8; 32]),
                            ],
                            Bytes::from(vec![10, 11, 12]),
                        )
                        .unwrap(),
                    },
                    position: U64::from(1),
                },
            ]),
        };
        let mut buf = Vec::new();
        vec![vec![call_frame; 2]; 2].encode(&mut &mut buf);

        vec![buf; count]
    }

    pub(crate) fn create_test_block_data_with_len(
        block_num: u64,
        header_variant: u8,
        parent_hash: Option<FixedBytes<32>>,
        receipts_len: usize,
        traces_len: usize,
    ) -> (Block, BlockReceipts, BlockTraces) {
        // Use the existing mock_block utility with 3 transactions
        let mut block = mock_block(block_num, vec![mock_tx(1), mock_tx(2), mock_tx(3)]);
        if let Some(parent_hash) = parent_hash {
            block.header.parent_hash = parent_hash;
        }
        let txs = block
            .body
            .transactions
            .iter()
            .map(|tx| tx.tx.clone())
            .collect::<Vec<_>>();
        let tx_root = calculate_transaction_root(&txs);
        block.header.transactions_root = tx_root;

        // let mut block = create_test_block(block_num, parent_hash);
        // Make the block slightly different based on variant
        block.header.extra_data = Bytes::from(vec![header_variant]);

        let receipts = create_test_receipts(receipts_len);

        // Calculate transactions_root
        {
            let txs = block
                .body
                .transactions
                .iter()
                .map(|tx| tx.tx.clone())
                .collect::<Vec<_>>();
            block.header.transactions_root = calculate_transaction_root(&txs);
        }
        // Calculate receipts_root
        {
            let receipts = receipts
                .iter()
                .map(|r| r.receipt.clone())
                .collect::<Vec<_>>();
            block.header.receipts_root = calculate_receipt_root(&receipts);
        }

        let traces = create_test_traces(traces_len);

        (block, receipts, traces)
    }

    pub(crate) fn create_test_block_data(
        block_num: u64,
        header_variant: u8,
        parent_hash: Option<FixedBytes<32>>,
    ) -> (Block, BlockReceipts, BlockTraces) {
        create_test_block_data_with_len(block_num, header_variant, parent_hash, 3, 3)
    }

    pub(crate) fn create_test_block_data_range(
        block_start: u64,
        header_variants: impl IntoIterator<Item = u8>,
    ) -> HashMap<u64, (Block, BlockReceipts, BlockTraces)> {
        let mut out = HashMap::new();
        let mut prev_hash = None;
        for (i, header_variant) in header_variants.into_iter().enumerate() {
            let block_num = block_start + i as u64;
            let data = create_test_block_data_with_len(block_num, header_variant, prev_hash, 3, 3);
            prev_hash = Some(data.0.header.hash_slow());
            out.insert(block_num, data);
        }
        out
    }

    #[test]
    fn test_process_blocks_with_mixed_data() {
        let block_num = 100;
        let mut data_by_block_num = HashMap::new();

        // Create three blocks with data

        // All replicas have identical data for block 100
        {
            let mut replica_data = HashMap::new();
            let (block, receipts, traces) = create_test_block_data(block_num, 1, None);
            replica_data.insert(
                "replica1".to_string(),
                Some((block.clone(), receipts.clone(), traces.clone())),
            );
            replica_data.insert(
                "replica2".to_string(),
                Some((block.clone(), receipts.clone(), traces.clone())),
            );
            replica_data.insert("replica3".to_string(), Some((block, receipts, traces)));
            data_by_block_num.insert(block_num, replica_data);
        }

        let phash = |replica: &str,
                     block_num: u64,
                     table: &HashMap<
            u64,
            HashMap<String, Option<(Block, BlockReceipts, BlockTraces)>>,
        >| {
            let data = table.get(&block_num).unwrap();
            let replica_data = data.get(replica).unwrap();
            replica_data.as_ref().unwrap().0.header.hash_slow()
        };

        // Two replicas agree, one is different for block 101
        {
            let mut replica_data = HashMap::new();
            let parent_hash = phash("replica1", block_num, &data_by_block_num);

            let data1 = create_test_block_data(block_num + 1, 1, Some(parent_hash));
            let data2 = create_test_block_data(block_num + 1, 2, Some(parent_hash));
            replica_data.insert("replica1".to_string(), Some(data1.clone()));
            replica_data.insert("replica2".to_string(), Some(data1));
            replica_data.insert("replica3".to_string(), Some(data2));
            data_by_block_num.insert(block_num + 1, replica_data);
        }

        // One replica has data, one is missing, one has invalid block number for block 102
        {
            let mut replica_data = HashMap::new();
            let parent_hash_3 = phash("replica3", block_num + 1, &data_by_block_num);
            let (mut block, receipts, traces) =
                create_test_block_data(block_num + 2, 1, Some(parent_hash_3));

            let parent_hash_1 = phash("replica1", block_num + 1, &data_by_block_num);
            let (valid_block, valid_receipts, valid_traces) =
                create_test_block_data(block_num + 2, 1, Some(parent_hash_1));

            // Make block's number invalid
            block.header.number = block_num + 100; // Wrong block number

            replica_data.insert(
                "replica1".to_string(),
                Some((valid_block, valid_receipts, valid_traces)),
            );
            replica_data.insert("replica2".to_string(), None); // Missing
            replica_data.insert("replica3".to_string(), Some((block, receipts, traces)));
            data_by_block_num.insert(block_num + 2, replica_data);
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
        let (block, _, _) = create_test_block_data(block_num, 1, None);

        // Test with mismatched receipt and trace counts
        let receipts = create_test_receipts(3); // 3 transactions
        let traces = create_test_traces(2); // Only 2 traces

        // Check invalid trace counts cause error
        assert!(verify_block(block_num, &block, &receipts, &traces, None).is_err());

        // Test with empty receipts
        let empty_receipts = create_test_receipts(0);
        match verify_block(block_num, &block, &empty_receipts, &traces, None) {
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

    pub(crate) fn setup_test_model(replicas: usize) -> CheckerModel {
        let store: KVStoreErased = MemoryStorage::new("test-store").into();

        let mut block_data_readers = HashMap::new();

        // Create memory-based readers for testing
        for i in 1..=replicas {
            let reader_store: KVStoreErased = MemoryStorage::new(format!("reader-{}", i)).into();
            let reader = BlockDataArchive::new(reader_store);
            block_data_readers.insert(format!("replica{}", i), reader);
        }

        CheckerModel {
            store,
            block_data_readers: Arc::new(block_data_readers),
        }
    }

    async fn populate_test_replicas(
        model: &CheckerModel,
        block_range: RangeInclusive<u64>,
        skip_3: bool,
    ) {
        // Add test blocks to all replicas, with some variations

        use std::iter::repeat_n;
        let mut blocks = create_test_block_data_range(
            *block_range.start(),
            repeat_n(1, (*block_range.end() - *block_range.start() + 1) as usize),
        );

        for block_num in block_range {
            // For replica1 and replica2, add identical data (consensus)
            let (block1, receipts1, traces1) = blocks.remove(&block_num).unwrap();

            if let Some(archiver) = model.block_data_readers.get("replica1") {
                archiver
                    .archive_block(block1.clone(), WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_receipts(receipts1.clone(), block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_traces(traces1.clone(), block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .update_latest(block_num, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }

            if let Some(archiver) = model.block_data_readers.get("replica2") {
                archiver
                    .archive_block(block1.clone(), WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_receipts(receipts1.clone(), block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_traces(traces1.clone(), block_num, WritePolicy::NoClobber)
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
                    create_test_block_data(block_num, 2, None)
                } else {
                    // Otherwise identical to others
                    (block1, receipts1, traces1)
                };

                if let Some(archiver) = model.block_data_readers.get("replica3") {
                    archiver
                        .archive_block(block3, WritePolicy::NoClobber)
                        .await
                        .unwrap();
                    archiver
                        .archive_receipts(receipts3, block_num, WritePolicy::NoClobber)
                        .await
                        .unwrap();
                    archiver
                        .archive_traces(traces3, block_num, WritePolicy::NoClobber)
                        .await
                        .unwrap();
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
        let model = setup_test_model(3);

        // Populate replicas with test data for blocks 100-110
        let start_block = 100;
        let end_block = 110;
        populate_test_replicas(&model, start_block..=end_block, false).await;

        // Process the batch
        let next_block = process_block_batch(&model, start_block, end_block, 20, &Metrics::none())
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
                            FaultKind::InconsistentBlock(
                                InconsistentBlockReason::InvalidParentHash
                            )
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
        let model = setup_test_model(3);

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

                // Fetch the data for replica1 and archive it
                let (block, receipts, traces) = model
                    .fetch_block_data_for_replica(block_num, "replica1")
                    .await
                    .unwrap();

                archiver
                    .archive_block(block, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_traces(traces, block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
            }
            archiver
                .update_latest(end_block, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        // Process the batch
        let next_block = process_block_batch(&model, start_block, end_block, 20, &Metrics::none())
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
        let model = setup_test_model(3);

        // Populate replicas with test data for blocks 300-310
        let start_block = 300;
        let end_block = 310;

        populate_test_replicas(&model, start_block..=end_block, false).await;

        let invalid_block_num = 305;
        if let Some(archiver) = model.block_data_readers.get("replica3") {
            let (mut block, _, _) = model
                .fetch_block_data_for_replica(invalid_block_num, "replica1")
                .await
                .unwrap();

            // 1) Insert into block table
            let block_key = archiver.block_key(invalid_block_num);

            // 2) Change the block number to an invalid one
            block.header.number = 9999;

            // 3) Encode into storage repr
            let encoded_block = encode_block(block).unwrap();

            // 4) Put the block directly into the block table
            archiver
                .store
                .put(&block_key, encoded_block, WritePolicy::AllowOverwrite)
                .await
                .unwrap();
        };

        // Process the batch
        let next_block = process_block_batch(&model, start_block, end_block, 20, &Metrics::none())
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

    // NOTE: the "replica carries legacy data with no checksum at rest" scenario is not
    // exercised here -- `MemoryStorage::metadata()` always stores the checksum alongside
    // the bytes and cannot model it. That fallback is covered by the kvstore backend tests.

    /// Builds a `CheckerModel` whose three replica readers are backed by the supplied
    /// `MemoryStorage` handles, so a caller can retain clones and inspect per-store
    /// `get_count()`s. The checker's own result store is a separate `MemoryStorage`.
    fn model_with_stores(s1: MemoryStorage, s2: MemoryStorage, s3: MemoryStorage) -> CheckerModel {
        let mut readers = HashMap::new();
        let s1: KVStoreErased = s1.into();
        let s2: KVStoreErased = s2.into();
        let s3: KVStoreErased = s3.into();
        readers.insert("replica1".to_string(), BlockDataArchive::new(s1));
        readers.insert("replica2".to_string(), BlockDataArchive::new(s2));
        readers.insert("replica3".to_string(), BlockDataArchive::new(s3));
        let store: KVStoreErased = MemoryStorage::new("checker-store").into();
        CheckerModel {
            store,
            block_data_readers: Arc::new(readers),
        }
    }

    /// Archives the given block data to a replica reader using `NoClobber` and marks it
    /// uploaded, mirroring `populate_test_replicas`.
    async fn archive_to_replica(
        model: &CheckerModel,
        replica: &str,
        block_num: u64,
        data: &(Block, BlockReceipts, BlockTraces),
    ) {
        let archiver = model.block_data_readers.get(replica).unwrap();
        let (block, receipts, traces) = data;
        archiver
            .archive_block(block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        archiver
            .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        archiver
            .archive_traces(traces.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        archiver
            .update_latest(block_num, LatestKind::Uploaded)
            .await
            .unwrap();
    }

    /// Test 1: the checksum fast path downloads exactly ONE replica's body when all
    /// present replicas agree. This is the core bandwidth-saving guarantee: the canonical
    /// (lex-smallest) replica is fully read (3 GETs: block + receipts + traces) and the
    /// other N-1 replicas' bodies are NOT downloaded at all.
    #[tokio::test]
    async fn test_fast_path_downloads_only_one_body() {
        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        // Single-block batch (start == end) so `prev_headers` is empty: no parent check,
        // hence no parent noise. Identical data on all three replicas.
        let block_num = 500;
        let data = create_test_block_data(block_num, 1, None);
        archive_to_replica(&model, "replica1", block_num, &data).await;
        archive_to_replica(&model, "replica2", block_num, &data).await;
        archive_to_replica(&model, "replica3", block_num, &data).await;

        // Archiving uses `put`, never `get`, so the counters are already 0; reset
        // defensively to make the post-condition unambiguous.
        s1.reset_get_count();
        s2.reset_get_count();
        s3.reset_get_count();

        let next_block = process_block_batch(&model, block_num, block_num, 20, &Metrics::none())
            .await
            .unwrap();
        assert_eq!(next_block, block_num + 1);

        let good_blocks = model.get_good_blocks(block_num).await.unwrap();
        assert_eq!(
            good_blocks.block_num_to_replica.get(&block_num),
            Some(&"replica1".to_string()),
            "lex-smallest present replica should be the canonical good block"
        );

        for replica in ["replica1", "replica2", "replica3"] {
            let faults = model.get_faults_chunk(replica, block_num).await.unwrap();
            assert!(
                faults.is_empty(),
                "expected no faults for {replica}, got {faults:?}"
            );
        }

        // The bandwidth-saving proof: only the canonical replica's body was downloaded
        // (3 GETs = block + receipts + traces), and the other two replicas' bodies were
        // NOT downloaded at all.
        assert_eq!(
            s1.get_count(),
            3,
            "canonical replica1 body (block+receipts+traces) should be downloaded exactly once"
        );
        assert_eq!(
            s2.get_count(),
            0,
            "replica2 body must NOT be downloaded on the fast path"
        );
        assert_eq!(
            s3.get_count(),
            0,
            "replica3 body must NOT be downloaded on the fast path"
        );
    }

    /// Test 2: when present replicas disagree on checksums the fast path reports
    /// `Disagreement` and the caller falls back to the full-body path, reproducing the
    /// legacy behavior (outlier faulted, agreeing pair recorded good).
    #[tokio::test]
    async fn test_fast_path_disagreement_falls_back() {
        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        // Single-block batch: replica1 & replica2 identical, replica3 mutated (different
        // header `variant` byte -> different block checksum).
        let block_num = 600;
        let agreed = create_test_block_data(block_num, 1, None);
        let outlier = create_test_block_data(block_num, 2, None);
        archive_to_replica(&model, "replica1", block_num, &agreed).await;
        archive_to_replica(&model, "replica2", block_num, &agreed).await;
        archive_to_replica(&model, "replica3", block_num, &outlier).await;

        // Direct outcome assertion: present replicas disagree -> Disagreement.
        let replicas = ["replica1", "replica2", "replica3"];
        let checksums_by_block =
            fetch_block_data_checksums(&model, block_num..=block_num, &replicas, 20).await;
        let outcome = try_check_block_via_checksums(
            &model,
            block_num,
            checksums_by_block.get(&block_num),
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, FastPathOutcome::Disagreement),
            "expected Disagreement, got a different outcome"
        );

        // End-to-end via the fallback: outlier faulted, agreeing pair recorded good.
        let next_block = process_block_batch(&model, block_num, block_num, 20, &Metrics::none())
            .await
            .unwrap();
        assert_eq!(next_block, block_num + 1);

        let r3_faults = model.get_faults_chunk("replica3", block_num).await.unwrap();
        assert!(
            r3_faults
                .iter()
                .any(|f| f.block_num == block_num
                    && matches!(f.fault, FaultKind::InconsistentBlock(_))),
            "expected InconsistentBlock fault for replica3, got {r3_faults:?}"
        );

        let good_blocks = model.get_good_blocks(block_num).await.unwrap();
        let good = good_blocks.block_num_to_replica.get(&block_num);
        assert!(
            good == Some(&"replica1".to_string()) || good == Some(&"replica2".to_string()),
            "expected the agreeing pair to be recorded good, got {good:?}"
        );
    }

    /// Test 3: a replica missing the block is reported `MissingBlock`, while the present
    /// pair is verified and one of them recorded good.
    #[tokio::test]
    async fn test_fast_path_missing_replica() {
        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        // replica3 is absent for this block.
        let block_num = 700;
        let data = create_test_block_data(block_num, 1, None);
        archive_to_replica(&model, "replica1", block_num, &data).await;
        archive_to_replica(&model, "replica2", block_num, &data).await;

        // Direct outcome assertion: present pair Verified, replica3 missing.
        let replicas = ["replica1", "replica2", "replica3"];
        let checksums_by_block =
            fetch_block_data_checksums(&model, block_num..=block_num, &replicas, 20).await;
        let outcome = try_check_block_via_checksums(
            &model,
            block_num,
            checksums_by_block.get(&block_num),
            &HashMap::new(),
        )
        .await
        .unwrap();
        match outcome {
            FastPathOutcome::Verified {
                mut good_replicas,
                parent_faulted_replicas,
                mut missing_replicas,
                header,
            } => {
                good_replicas.sort();
                assert_eq!(good_replicas, vec!["replica1", "replica2"]);
                assert!(parent_faulted_replicas.is_empty());
                missing_replicas.sort();
                assert_eq!(missing_replicas, vec!["replica3"]);
                assert!(header.is_some());
            }
            _ => panic!("expected Verified with one missing replica"),
        }

        // End-to-end: replica3 -> MissingBlock; a good block recorded for the present pair.
        let next_block = process_block_batch(&model, block_num, block_num, 20, &Metrics::none())
            .await
            .unwrap();
        assert_eq!(next_block, block_num + 1);

        let r3_faults = model.get_faults_chunk("replica3", block_num).await.unwrap();
        assert!(
            r3_faults
                .iter()
                .any(|f| f.block_num == block_num && matches!(f.fault, FaultKind::MissingBlock)),
            "expected MissingBlock fault for replica3, got {r3_faults:?}"
        );
        let good_blocks = model.get_good_blocks(block_num).await.unwrap();
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));
    }

    /// All replicas agree on identical bytes that fail intra-block verification (a
    /// corrupted `transactions_root`): every present replica gets the fault, no good block.
    #[tokio::test]
    async fn test_fast_path_all_agree_but_verify_fails() {
        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        // Single-block batch: corrupt the tx root, then archive the SAME bytes to all 3.
        let block_num = 800;
        let (mut block, receipts, traces) = create_test_block_data(block_num, 1, None);
        block.header.transactions_root = FixedBytes::<32>::from([0xAB; 32]);
        let data = (block, receipts, traces);
        archive_to_replica(&model, "replica1", block_num, &data).await;
        archive_to_replica(&model, "replica2", block_num, &data).await;
        archive_to_replica(&model, "replica3", block_num, &data).await;

        // Direct outcome assertion: VerifyFailed with InvalidTransactionRoot for all 3.
        let replicas = ["replica1", "replica2", "replica3"];
        let checksums_by_block =
            fetch_block_data_checksums(&model, block_num..=block_num, &replicas, 20).await;
        let outcome = try_check_block_via_checksums(
            &model,
            block_num,
            checksums_by_block.get(&block_num),
            &HashMap::new(),
        )
        .await
        .unwrap();
        match outcome {
            FastPathOutcome::VerifyFailed {
                fault,
                mut fault_replicas,
                missing_replicas,
                ..
            } => {
                assert_eq!(
                    fault,
                    FaultKind::InconsistentBlock(InconsistentBlockReason::InvalidTransactionRoot)
                );
                fault_replicas.sort();
                assert_eq!(fault_replicas, vec!["replica1", "replica2", "replica3"]);
                assert!(missing_replicas.is_empty());
            }
            _ => panic!("expected VerifyFailed for all present replicas"),
        }

        // End-to-end: all three replicas carry the fault; no good block recorded.
        let next_block = process_block_batch(&model, block_num, block_num, 20, &Metrics::none())
            .await
            .unwrap();
        assert_eq!(next_block, block_num + 1);

        for replica in ["replica1", "replica2", "replica3"] {
            let faults = model.get_faults_chunk(replica, block_num).await.unwrap();
            assert!(
                faults.iter().any(|f| f.block_num == block_num
                    && matches!(
                        f.fault,
                        FaultKind::InconsistentBlock(
                            InconsistentBlockReason::InvalidTransactionRoot
                        )
                    )),
                "expected InvalidTransactionRoot fault for {replica}, got {faults:?}"
            );
        }
        let good_blocks = model.get_good_blocks(block_num).await.unwrap();
        assert!(
            !good_blocks.block_num_to_replica.contains_key(&block_num),
            "no good block should be recorded when the shared body fails verification"
        );
    }

    /// Test 5: the fast path still performs the per-replica parent-link check across
    /// blocks. All replicas agree on the bytes of both block N-1 and block N, but block
    /// N's `parent_hash` does not match block N-1's hash, so every present replica is
    /// faulted with `InvalidParentHash` at block N (preserving cross-block behavior).
    #[tokio::test]
    async fn test_fast_path_invalid_parent_hash_preserved() {
        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        // Two consecutive blocks. Built independently (not chained), so block N's default
        // zero parent_hash will NOT match block N-1's real hash -> parent check fires.
        let prev_num = 900;
        let block_num = 901;
        let prev_data = create_test_block_data(prev_num, 1, None);
        let cur_data = create_test_block_data(block_num, 1, None);
        // Sanity: the parent link really is broken.
        assert_ne!(
            cur_data.0.header.parent_hash,
            prev_data.0.header.hash_slow(),
            "test setup must produce a broken parent link"
        );

        for replica in ["replica1", "replica2", "replica3"] {
            archive_to_replica(&model, replica, prev_num, &prev_data).await;
            archive_to_replica(&model, replica, block_num, &cur_data).await;
        }

        let next_block = process_block_batch(&model, prev_num, block_num, 20, &Metrics::none())
            .await
            .unwrap();
        assert_eq!(next_block, block_num + 1);

        // Every present replica is faulted with InvalidParentHash at block N.
        for replica in ["replica1", "replica2", "replica3"] {
            let faults = model.get_faults_chunk(replica, prev_num).await.unwrap();
            assert!(
                faults.iter().any(|f| f.block_num == block_num
                    && matches!(
                        f.fault,
                        FaultKind::InconsistentBlock(
                            InconsistentBlockReason::InvalidParentHash
                        )
                    )),
                "expected InvalidParentHash fault at block {block_num} for {replica}, got {faults:?}"
            );
        }

        // Block N-1 (the batch's first block) has no parent and is recorded good.
        let good_blocks = model.get_good_blocks(prev_num).await.unwrap();
        assert!(good_blocks.block_num_to_replica.contains_key(&prev_num));
    }

    #[test]
    fn test_fast_path_metric_mapping() {
        // `MetricNames` has no `Debug`, so compare with `==` rather than `assert_eq!`.
        let header = create_test_block_data(1, 1, None).0.header;

        assert!(
            FastPathOutcome::Verified {
                good_replicas: vec![],
                parent_faulted_replicas: vec![],
                missing_replicas: vec![],
                header: None,
            }
            .fast_path_metric()
                == MetricNames::ARCHIVE_CHECKER_FAST_PATH_HITS
        );
        assert!(
            FastPathOutcome::VerifyFailed {
                fault: FaultKind::InconsistentBlock(
                    InconsistentBlockReason::InvalidTransactionRoot
                ),
                fault_replicas: vec![],
                missing_replicas: vec![],
                header,
            }
            .fast_path_metric()
                == MetricNames::ARCHIVE_CHECKER_FAST_PATH_HITS
        );
        assert!(
            FastPathOutcome::Disagreement.fast_path_metric()
                == MetricNames::ARCHIVE_CHECKER_FAST_PATH_MISSES
        );
    }

    /// Regression test for the resilience fix: a single replica's backend read error during
    /// the checksum prefetch must NOT abort the batch. The erroring replica is recorded
    /// `MissingBlock` (mirroring the old full-body path's error swallowing) while the other
    /// replicas are still checked and the batch completes successfully.
    #[tokio::test]
    async fn test_fast_path_replica_error_is_non_fatal() {
        use std::sync::atomic::Ordering;

        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        let block_num = 600;
        let data = create_test_block_data(block_num, 1, None);
        archive_to_replica(&model, "replica1", block_num, &data).await;
        archive_to_replica(&model, "replica2", block_num, &data).await;
        archive_to_replica(&model, "replica3", block_num, &data).await;

        // After populating, make replica3's backend fail every read. The batch must still
        // complete rather than propagating the error out of the checker worker.
        s3.should_fail.store(true, Ordering::SeqCst);

        let next_block = process_block_batch(&model, block_num, block_num, 20, &Metrics::none())
            .await
            .expect("a single replica's backend error must not fail the batch");
        assert_eq!(next_block, block_num + 1);

        // replica3 is recorded missing (its checksum read errored -> treated as absent).
        let r3_faults = model.get_faults_chunk("replica3", block_num).await.unwrap();
        assert!(
            r3_faults
                .iter()
                .any(|f| f.block_num == block_num && matches!(f.fault, FaultKind::MissingBlock)),
            "expected MissingBlock for the erroring replica, got {r3_faults:?}"
        );

        // replica1 and replica2 were still checked: a good block is recorded and they carry
        // no faults.
        let good_blocks = model.get_good_blocks(block_num).await.unwrap();
        assert!(good_blocks.block_num_to_replica.contains_key(&block_num));
        for replica in ["replica1", "replica2"] {
            let faults = model.get_faults_chunk(replica, block_num).await.unwrap();
            assert!(
                faults.is_empty(),
                "expected no faults for {replica}, got {faults:?}"
            );
        }
    }

    /// Locks in the documented fault-reason divergence: when all replicas share identical
    /// intra-invalid bytes and the parent is broken, the fast path reports the intra reason
    /// (`InvalidTransactionRoot`), not `InvalidParentHash`. If changed, update the NOTE.
    #[tokio::test]
    async fn test_fast_path_intra_invalid_with_broken_parent() {
        let s1 = MemoryStorage::new("replica1");
        let s2 = MemoryStorage::new("replica2");
        let s3 = MemoryStorage::new("replica3");
        let model = model_with_stores(s1.clone(), s2.clone(), s3.clone());

        let prev_num = 700;
        let block_num = 701;
        let prev_data = create_test_block_data(prev_num, 1, None);
        // Build block N with a broken parent link (independent block), then corrupt its
        // transactions_root so intra-block verification fails on the tx root.
        let mut cur_data = create_test_block_data(block_num, 1, None);
        cur_data.0.header.transactions_root = FixedBytes::<32>::from([0xAB; 32]);
        assert_ne!(
            cur_data.0.header.parent_hash,
            prev_data.0.header.hash_slow(),
            "test setup must produce a broken parent link"
        );

        for replica in ["replica1", "replica2", "replica3"] {
            archive_to_replica(&model, replica, prev_num, &prev_data).await;
            archive_to_replica(&model, replica, block_num, &cur_data).await;
        }

        process_block_batch(&model, prev_num, block_num, 20, &Metrics::none())
            .await
            .unwrap();

        // Every present replica gets the intra-block reason (InvalidTransactionRoot), NOT
        // InvalidParentHash, at block N -- the documented trade-off.
        for replica in ["replica1", "replica2", "replica3"] {
            let faults = model.get_faults_chunk(replica, prev_num).await.unwrap();
            let n_fault = faults.iter().find(|f| f.block_num == block_num);
            assert!(
                matches!(
                    n_fault.map(|f| &f.fault),
                    Some(FaultKind::InconsistentBlock(
                        InconsistentBlockReason::InvalidTransactionRoot
                    ))
                ),
                "expected InvalidTransactionRoot (the documented intra reason) at block {block_num} for {replica}, got {n_fault:?}"
            );
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_checker_with_mongo() {
        use monad_archive::{kvstore::mongo::MongoDbStorage, test_utils::TestMongoContainer};

        // Start MongoDB container
        let container = TestMongoContainer::new().await.unwrap();

        // Create MemoryStorage for checker state
        let checker_store: KVStoreErased = MemoryStorage::new("checker-state").into();

        // Create mixed storage for replicas: 1 MongoDB, 2 MemoryStorage
        let mut block_data_readers = HashMap::new();

        // Replica1: MongoDB storage
        let mongo_store =
            MongoDbStorage::new(&container.uri, "replica_db", "blocks", Metrics::none())
                .await
                .unwrap();

        let mongo_reader = BlockDataArchive::new(mongo_store);
        block_data_readers.insert("replica1".to_string(), mongo_reader);

        // Replica2 and Replica3: MemoryStorage
        for i in 2..=3 {
            let memory_store: KVStoreErased = MemoryStorage::new(format!("replica{}", i)).into();
            let memory_reader = BlockDataArchive::new(memory_store);
            block_data_readers.insert(format!("replica{}", i), memory_reader);
        }

        let model = CheckerModel {
            store: checker_store,
            block_data_readers: Arc::new(block_data_readers),
        };

        // Test data setup
        let start_block = 100;
        let end_block = 105;

        // Populate test data in all replicas
        let mut parent_hash1 = None;
        let mut parent_hash3 = None;
        for block_num in start_block..=end_block {
            // For replica1 (MongoDB) and replica2 (Memory), add identical data
            let (block1, receipts1, traces1) = create_test_block_data(block_num, 1, parent_hash1);
            parent_hash1 = Some(block1.header.hash_slow());

            for replica_name in ["replica1", "replica2"] {
                if let Some(archiver) = model.block_data_readers.get(replica_name) {
                    archiver
                        .archive_block(block1.clone(), WritePolicy::NoClobber)
                        .await
                        .unwrap();
                    archiver
                        .archive_receipts(receipts1.clone(), block_num, WritePolicy::NoClobber)
                        .await
                        .unwrap();
                    archiver
                        .archive_traces(traces1.clone(), block_num, WritePolicy::NoClobber)
                        .await
                        .unwrap();
                    archiver
                        .update_latest(block_num, LatestKind::Uploaded)
                        .await
                        .unwrap();
                }
            }

            // For replica3 (Memory), add different data for every other block
            if let Some(archiver) = model.block_data_readers.get("replica3") {
                let (block3, receipts3, traces3) = if block_num % 2 == 0 {
                    create_test_block_data(block_num, 2, parent_hash3) // Different data
                } else {
                    (block1, receipts1, traces1) // Same data
                };
                parent_hash3 = Some(block3.header.hash_slow());

                archiver
                    .archive_block(block3, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_receipts(receipts3, block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .archive_traces(traces3, block_num, WritePolicy::NoClobber)
                    .await
                    .unwrap();
                archiver
                    .update_latest(block_num, LatestKind::Uploaded)
                    .await
                    .unwrap();
            }
        }

        // Process blocks with checker
        let next_block = process_block_batch(&model, start_block, end_block, 20, &Metrics::none())
            .await
            .unwrap();

        assert_eq!(next_block, end_block + 1);

        // Verify results stored in checker's MemoryStorage
        // Check that faults were recorded for replica3 on even blocks
        let faults = model
            .get_faults_chunk("replica3", start_block)
            .await
            .unwrap();

        let expected_fault_count = (end_block - start_block).div_ceil(2) as usize;
        assert!(faults.len() >= expected_fault_count);

        // Verify good blocks were stored
        let good_blocks = model.get_good_blocks(start_block).await.unwrap();
        assert_eq!(
            good_blocks.block_num_to_replica.len(),
            (end_block - start_block + 1) as usize
        );

        // Verify latest checked was updated
        for replica_name in ["replica1", "replica2", "replica3"] {
            let latest_checked = model
                .get_latest_checked_for_replica(replica_name)
                .await
                .unwrap();
            assert_eq!(latest_checked, end_block);
        }
    }
}
