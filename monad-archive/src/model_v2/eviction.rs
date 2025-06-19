use futures::TryStreamExt;
use mongodb::bson::doc;

use crate::{
    model::block_data_archive::BlockDataArchive,
    model_v2::{LatestDoc, S3Ref, TxDoc},
    prelude::*,
};

const MAX_BLOCKS_TO_EVICT: u64 = if cfg!(test) { 10 } else { 10_000 };

pub fn eviction_worker(model_v2: ModelV2, max_storage_size: i64, metrics: Metrics) {
    tokio::spawn(async move {
        loop {
            info!("Checking db size and evicting traces if needed");

            if let Err(err) = evict_traces_if_needed(&model_v2, max_storage_size, &metrics).await {
                error!("Failed to evict traces: {err:?}");
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(MAX_BLOCKS_TO_EVICT / 3)).await;
        }
    });
}

pub async fn evict_traces_if_needed(
    model_v2: &ModelV2,
    max_storage_size: i64,
    metrics: &Metrics,
) -> Result<()> {
    let db = model_v2.client.database(&model_v2.replica_name);
    let state = db.run_command(doc! { "dbStats": 1 }).await?;

    let storage_size = state
        .get("dataSize")
        .wrap_err("dataSize not found")?
        .as_f64()
        .wrap_err("dataSize is not a f64")?
        .round() as i64;
    metrics.periodic_gauge_with_attrs(MetricNames::DATA_SIZE, storage_size as u64, vec![]);
    info!(storage_size, max_storage_size, "Db data");

    if storage_size > max_storage_size {
        let latest_block = model_v2
            .headers
            .clone_with_type::<LatestDoc>()
            .find_one(LatestDoc::trace_eviction_query())
            .await?
            .unwrap_or_else(|| LatestDoc::trace_eviction(-1))
            .block_number;

        metrics.periodic_gauge_with_attrs(
            MetricNames::LATEST_BLOCK_FOR_TRACE_EVICTION,
            latest_block as u64,
            vec![],
        );

        let range = latest_block + 1..=latest_block + MAX_BLOCKS_TO_EVICT as i64;
        info!(
            start_block = range.start(),
            end_block = range.end(),
            "Evicting traces..."
        );

        futures::stream::iter(range.clone())
            .map(|block_num| evict_traces_for_block(model_v2, block_num as u64))
            .buffer_unordered(model_v2.concurrency)
            .try_collect::<Vec<_>>()
            .await
            .wrap_err("failed to evict traces")?;

        // Update the trace eviction marker
        model_v2
            .headers
            .clone_with_type::<LatestDoc>()
            .replace_one(
                LatestDoc::trace_eviction_query(),
                LatestDoc::trace_eviction(*range.end()),
            )
            .upsert(true)
            .await?;

        info!(
            start_block = range.start(),
            end_block = range.end(),
            "Evicted traces"
        );

        metrics.periodic_gauge_with_attrs(
            MetricNames::LATEST_BLOCK_FOR_TRACE_EVICTION,
            latest_block as u64,
            vec![],
        );
    }

    Ok(())
}

pub async fn evict_traces_for_block(model_v2: &ModelV2, block_num: u64) -> Result<()> {
    // First, fetch ALL documents for the block
    let all_txs = model_v2
        .txs
        .find(doc! {
            "block_number": block_num as i64,
        })
        .await?
        .try_collect::<Vec<TxDoc>>()
        .await?;
    let header = model_v2.get_header(block_num).await?;

    // Filter to only those with inline traces
    let txs_with_traces: Vec<TxDoc> = all_txs
        .into_iter()
        .filter(|tx| tx.trace.is_some() && tx.trace_ref.is_none())
        .collect();

    if txs_with_traces.is_empty() {
        // No inline traces to evict
        return Ok(());
    }

    // Collect all traces for the block
    let mut traces = HashMap::with_capacity(txs_with_traces.len());

    for tx_doc in txs_with_traces {
        if let Some(trace_binary) = tx_doc.trace {
            traces.insert(tx_doc._id, trace_binary.bytes);
        }
    }

    // Check if this data already exists in S3
    // let s3 = model_v2.s3_client.clone();
    let bdr = BlockDataArchive::new(model_v2.s3_client.clone());
    let s3_key = bdr.traces_key(block_num);

    // Try to get existing data from S3
    let (existing_data, offsets) = bdr.get_traces_with_offsets(block_num).await?;

    for (s3_trace, tx_hash) in existing_data.into_iter().zip(header.tx_hashes.iter()) {
        let trace = traces
            .get(tx_hash)
            .wrap_err("tx hash not found in traces")?;
        if trace != &s3_trace {
            bail!(
                "S3 trace data differs from MongoDB data for block: {:?}, tx_hash={:?}",
                block_num,
                tx_hash
            );
        }
    }

    // Update MongoDB documents to replace inline traces with S3 references
    futures::stream::iter(header.tx_hashes.into_iter().zip(offsets))
        .map(|(tx_hash, trace_range)| {
            let key = s3_key.clone();
            async move {
                model_v2
                    .txs
                    .update_one(
                        doc! { "_id": tx_hash },
                        doc! {
                            // Unset the trace field
                            "$unset": {
                                "trace": "",
                            },
                            // Set the trace_ref field
                            "$set": {
                                "trace_ref": S3Ref {
                                    key,
                                    start: trace_range.start as i32,
                                    end: (trace_range.end - 1) as i32,  // Convert from exclusive to inclusive
                                }
                            }
                        },
                    )
                    .await
                    .wrap_err_with(|| {
                        format!("Failed to update trace for block {block_num} for eviction")
                    })
            }
        })
        .buffer_unordered(model_v2.concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(())
}

// TODO: evict tx bodies

// TODO: evict receipts

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metrics::Metrics,
        model::BlockArchiver,
        model_v2::tests::setup,
        test_utils::{mock_block, mock_rx, mock_tx},
    };

    // This test requires actual S3 connectivity to work properly.
    // It's ignored by default and should only be run with proper S3 setup.
    #[ignore]
    #[tokio::test]
    async fn test_eviction() -> Result<()> {
        let (_container, model) = setup().await?;
        let v1_model = BlockDataArchive::new(model.s3_client.clone());

        // Test archiving a block with traces
        let block_num = 300;
        let txs = vec![mock_tx(30), mock_tx(31)];
        let block = mock_block(block_num, txs.clone());
        let receipts = vec![mock_rx(100, 1000), mock_rx(200, 2000)];
        let traces = vec![vec![10; 50], vec![20; 50]];

        // Archive the block
        model
            .archive_block_data(block.clone(), receipts.clone(), traces.clone())
            .await?;

        let retrieved_traces = model.get_block_traces(block_num).await?;
        assert_eq!(retrieved_traces, traces);

        // Archive the block to v1
        v1_model
            .archive_block_data(block.clone(), receipts.clone(), traces.clone())
            .await?;

        // Verify traces are inline before eviction
        let tx_docs_before: Vec<TxDoc> = model
            .txs
            .find(doc! { "block_number": block_num as i64 })
            .await?
            .try_collect()
            .await?;

        assert_eq!(tx_docs_before.len(), 2);
        assert!(tx_docs_before[0].trace.is_some());
        assert!(tx_docs_before[0].trace_ref.is_none());

        // Run eviction for this block
        evict_traces_for_block(&model, block_num).await?;

        // Verify traces are now in S3 with references
        let tx_docs_after: Vec<TxDoc> = model
            .txs
            .find(doc! { "block_number": block_num as i64 })
            .await?
            .try_collect()
            .await?;

        assert_eq!(tx_docs_after.len(), 2);
        assert!(tx_docs_after.iter().all(|tx| tx.trace.is_none()));
        assert!(tx_docs_after.iter().all(|tx| tx.trace_ref.is_some()));

        // Verify we can still read traces through references
        let retrieved_traces = model.get_block_traces(block_num).await?;
        assert_eq!(retrieved_traces, traces);

        // Verify we can read individual traces
        let trace = model.get_trace(txs[0].tx.tx_hash()).await?;
        assert_eq!(trace.0, traces[0]);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_eviction_with_size_threshold() -> Result<()> {
        let (_container, model) = setup().await?;
        let v1_model = BlockDataArchive::new(model.s3_client.clone());
        let metrics = Metrics::none();

        // Helper to get actual storage size from MongoDB
        let get_data_size = || {
            let db = model.client.database(&model.replica_name);
            async move {
                let stats = db.run_command(doc! { "dbStats": 1 }).await?;
                eyre::Ok(
                    stats
                        .get("dataSize")
                        .wrap_err("dataSize not found")?
                        .as_f64()
                        .wrap_err("dataSize is not a f64")?
                        .round() as i64,
                )
            }
        };

        // Get initial size
        let initial_size = get_data_size().await?;
        eprintln!("Initial storage size: {initial_size:?}");

        // Archive blocks until we have at least 2MB of data
        let mut block_num = 0u64;
        loop {
            let txs: Vec<_> = (0..50).map(|i| mock_tx(block_num * 100 + i)).collect();
            let block = mock_block(block_num, txs.clone());
            let receipts: Vec<_> = (0..50)
                .map(|i| mock_rx(100 + i, 1000 * (i as u128 + 1)))
                .collect();
            let traces: Vec<_> = (0..50).map(|i| vec![i as u8; 1000]).collect();

            // Archive to both V1 and V2
            model
                .archive_block_data(block.clone(), receipts.clone(), traces.clone())
                .await?;
            v1_model.archive_block_data(block, receipts, traces).await?;

            let current_size = get_data_size().await?;
            eprintln!("After block {block_num}: data size = {current_size:?}");

            block_num += 1;

            // Stop when we have at least 2MB
            if current_size - initial_size >= 2_000_000 {
                break;
            }
        }

        let size_after_2mb = get_data_size().await?;
        eprintln!("Data size after ~2MB of data: {}", size_after_2mb);

        // First eviction check with 5MB limit - should NOT evict anything
        evict_traces_if_needed(&model, 5_000_000, &metrics).await?;

        // Verify no eviction happened
        let tx_docs: Vec<TxDoc> = model.txs.find(doc! {}).await?.try_collect().await?;

        assert!(
            tx_docs.iter().all(|tx| tx.trace.is_some()),
            "No traces should be evicted yet"
        );
        assert!(
            tx_docs.iter().all(|tx| tx.trace_ref.is_none()),
            "No trace refs should exist yet"
        );

        // Continue archiving blocks until we exceed 5MB
        let _blocks_before_5mb = block_num - 1;
        loop {
            let txs: Vec<_> = (0..50).map(|i| mock_tx(block_num * 100 + i)).collect();
            let block = mock_block(block_num, txs.clone());
            let receipts: Vec<_> = (0..50)
                .map(|i| mock_rx(100 + i, 1000 * (i as u128 + 1)))
                .collect();
            let traces: Vec<_> = (0..50).map(|i| vec![i as u8; 1000]).collect();

            // Archive to both V1 and V2
            model
                .archive_block_data(block.clone(), receipts.clone(), traces.clone())
                .await?;
            v1_model.archive_block_data(block, receipts, traces).await?;

            let current_size = get_data_size().await?;
            eprintln!("After block {}: data size = {}", block_num, current_size);

            block_num += 1;

            // Stop when we exceed 5MB
            if current_size - initial_size > 5_000_000 {
                break;
            }
        }

        let total_blocks = block_num - 1;
        let size_after_5mb = get_data_size().await?;
        eprintln!("Data size after exceeding 5MB: {}", size_after_5mb);
        eprintln!("Total blocks archived: {}", total_blocks);

        // Second eviction check with 5MB limit - SHOULD evict traces
        evict_traces_if_needed(&model, initial_size + 5_000_000, &metrics).await?;

        // Check which blocks got evicted
        let evicted_blocks = model
            .headers
            .clone_with_type::<LatestDoc>()
            .find_one(LatestDoc::trace_eviction_query())
            .await?
            .map(|doc| doc.block_number)
            .unwrap_or(0);

        eprintln!("Last block evicted: {evicted_blocks:?}");

        // Verify eviction happened for the expected blocks
        if evicted_blocks > 0 {
            for block in 0..=evicted_blocks {
                let tx_docs: Vec<TxDoc> = model
                    .txs
                    .find(doc! { "block_number": block })
                    .await?
                    .try_collect()
                    .await?;

                if !tx_docs.is_empty() {
                    assert!(
                        tx_docs.iter().all(|tx| tx.trace.is_none()),
                        "Block {} should have traces evicted",
                        block
                    );
                    assert!(
                        tx_docs.iter().all(|tx| tx.trace_ref.is_some()),
                        "Block {} should have trace refs",
                        block
                    );
                }
            }
        }

        // Verify remaining blocks still have inline traces
        for block in (evicted_blocks + 1)..=total_blocks as i64 {
            let tx_docs: Vec<TxDoc> = model
                .txs
                .find(doc! { "block_number": block })
                .await?
                .try_collect()
                .await?;

            if !tx_docs.is_empty() {
                assert!(
                    tx_docs.iter().all(|tx| tx.trace.is_some()),
                    "Block {} should still have inline traces",
                    block
                );
                assert!(
                    tx_docs.iter().all(|tx| tx.trace_ref.is_none()),
                    "Block {} should not have trace refs",
                    block
                );
            }
        }

        // Verify we can still read all traces correctly
        for block in 1..=total_blocks {
            let traces = model.get_block_traces(block).await?;
            assert_eq!(traces.len(), 50, "Block {} should have 50 traces", block);

            // Verify trace content
            for (i, trace) in traces.iter().enumerate() {
                assert_eq!(trace.len(), 1000);
                assert_eq!(trace[0], i as u8);
            }
        }

        // Check final storage size
        let final_size = get_data_size().await?;
        eprintln!("Final data size: {}", final_size);

        // Storage should be reduced after eviction
        assert!(
            final_size < size_after_5mb,
            "Data size should be reduced after eviction: {} < {}",
            final_size,
            size_after_5mb
        );

        Ok(())
    }
}
