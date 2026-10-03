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

use std::vec::IntoIter;

use alloy_primitives::hex::ToHexExt;
use clap::Parser;
use eyre::Context;
use monad_archive::{
    archive_reader::redact_mongo_url,
    cli::ArchiveArgs,
    kvstore::{mongo::new_client, WritePolicy},
    prelude::*,
};
use mongodb::{
    bson::{doc, Bson, Document},
    options::{CollectionOptions, WriteConcern},
    Collection,
};
use tracing::Level;

mod cli;

/// MongoDB storage for JSON documents stored as native BSON documents.
#[derive(Clone)]
pub struct MongoJsonDocumentStore {
    collection: Collection<Document>,
    name: String,
}

impl MongoJsonDocumentStore {
    pub async fn new(
        connection_string: &str,
        database: &str,
        collection_name: &str,
    ) -> eyre::Result<Self> {
        info!(
            "Initializing MongoDB JSON document store: {}/{} collection: {}",
            redact_mongo_url(connection_string),
            database,
            collection_name
        );

        let client = new_client(connection_string).await?;
        let db = client.database(database);

        let collection_exists = db
            .list_collection_names()
            .await?
            .contains(&collection_name.to_string());

        if !collection_exists {
            info!("Collection '{}' not found, creating...", collection_name);
            db.create_collection(collection_name).await?;
            info!("Collection '{}' created successfully", collection_name);
        }

        let collection: Collection<Document> = db.collection_with_options(
            collection_name,
            CollectionOptions::builder()
                .write_concern(Some(WriteConcern::builder().journal(Some(true)).build()))
                .build(),
        );

        Ok(Self {
            collection,
            name: format!("mongodb://{database}/{collection_name}"),
        })
    }

    /// Store a JSON value as a native BSON document.
    pub async fn put_json_document(
        &self,
        key: &str,
        document: serde_json::Value,
        policy: WritePolicy,
    ) -> eyre::Result<()> {
        // Convert serde_json::Value to BSON
        let bson_value: Bson = document
            .try_into()
            .wrap_err("Failed to convert JSON to BSON")?;

        let Bson::Document(mut bson_doc) = bson_value else {
            return Err(eyre::eyre!("JSON value must be an object"));
        };

        // Set the _id field to the key
        bson_doc.insert("_id", key);

        match policy {
            WritePolicy::NoClobber => {
                match self.collection.insert_one(&bson_doc).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // Check if it's a duplicate key error
                        if let mongodb::error::ErrorKind::Write(
                            mongodb::error::WriteFailure::WriteError(write_error),
                        ) = e.kind.as_ref()
                        {
                            if write_error.code == 11000 {
                                // Duplicate key - skip silently for NoClobber
                                info!(key, "Document already exists, skipping (NoClobber policy)");
                                return Ok(());
                            }
                        }
                        Err(e).wrap_err("Failed to insert document")
                    }
                }
            }
            WritePolicy::AllowOverwrite => {
                self.collection
                    .replace_one(doc! { "_id": key }, &bson_doc)
                    .upsert(true)
                    .await
                    .wrap_err("Failed to upsert document")?;
                Ok(())
            }
        }
    }

    /// Get a document by key, returning a specific field value as string.
    pub async fn get_string(&self, key: &str, field: &str) -> eyre::Result<Option<String>> {
        let result = self
            .collection
            .find_one(doc! { "_id": key })
            .await
            .wrap_err("Failed to get document")?;

        match result {
            Some(doc) => match doc.get(field) {
                Some(Bson::String(s)) => Ok(Some(s.clone())),
                Some(Bson::Int64(n)) => Ok(Some(n.to_string())),
                Some(Bson::Int32(n)) => Ok(Some(n.to_string())),
                Some(other) => Err(eyre::eyre!(
                    "Field '{}' has unexpected type: {:?}",
                    field,
                    other
                )),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Store a simple key-value pair (for metadata like "latest" block).
    pub async fn put_metadata(
        &self,
        key: &str,
        field: &str,
        value: impl Into<Bson>,
    ) -> eyre::Result<()> {
        let doc = doc! {
            "_id": key,
            field: value.into(),
        };

        self.collection
            .replace_one(doc! { "_id": key }, &doc)
            .upsert(true)
            .await
            .wrap_err("Failed to upsert metadata")?;

        Ok(())
    }
}

fn block_to_json(
    block: &Block,
    receipts: &BlockReceipts,
    traces: &BlockTraces,
) -> serde_json::Value {
    let header = &block.header;
    let body = &block.body;

    // Convert traces from raw bytes to hex strings for JSON serialization
    let traces_hex: Vec<String> = traces
        .iter()
        .map(|trace| trace.encode_hex_with_prefix())
        .collect();

    serde_json::json!({
        "header": {
            "parentHash": header.parent_hash,
            "ommersHash": header.ommers_hash,
            "beneficiary": header.beneficiary,
            "stateRoot": header.state_root,
            "transactionsRoot": header.transactions_root,
            "receiptsRoot": header.receipts_root,
            "logsBloom": header.logs_bloom,
            "difficulty": header.difficulty,
            "number": header.number,
            "gasLimit": header.gas_limit,
            "gasUsed": header.gas_used,
            "timestamp": header.timestamp,
            "extraData": header.extra_data,
            "mixHash": header.mix_hash,
            "nonce": header.nonce,
            "baseFeePerGas": header.base_fee_per_gas,
            "withdrawalsRoot": header.withdrawals_root,
            "blobGasUsed": header.blob_gas_used,
            "excessBlobGas": header.excess_blob_gas,
            "parentBeaconBlockRoot": header.parent_beacon_block_root,
            "requestsHash": header.requests_hash,
        },
        "body": {
            "transactions": body.transactions,
            "ommers": body.ommers,
            "withdrawals": body.withdrawals,
        },
        "receipts": receipts,
        "traces": traces_hex,
    })
}

async fn process_block(
    reader: &BlockDataReaderErased,
    current_block: u64,
    writer: &MongoJsonDocumentStore,
) -> Result<()> {
    // Fetch block, receipts, and traces in parallel (like monad-archive-checker)
    let (block, receipts, traces) = try_join!(
        reader.get_block_by_number(current_block),
        reader.get_block_receipts(current_block),
        reader.get_block_traces(current_block),
    )
    .wrap_err_with(|| format!("Failed to fetch block data for block {current_block}"))?;

    let json_doc = block_to_json(&block, &receipts, &traces);

    let key = current_block.to_string();

    writer
        .put_json_document(&key, json_doc, WritePolicy::AllowOverwrite)
        .await
        .wrap_err_with(|| format!("Failed to write block {current_block} to database"))?;

    info!("Wrote block {}", current_block,);
    Ok(())
}

async fn build_json_archive(
    sink: &ArchiveArgs,
    collection: &str,
) -> Result<MongoJsonDocumentStore> {
    let json_store = match sink {
        ArchiveArgs::MongoDb(args) => {
            MongoJsonDocumentStore::new(&args.url, &args.db, collection).await?
        }
        ArchiveArgs::Aws(_args) => panic!("Aws unsupported"),
        ArchiveArgs::Fs(_args) => panic!("Filesystem unsupported"),
    };

    Ok(json_store)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    // Handle SetStartBlock separately since it doesn't need shared args
    if let cli::Mode::SetStartBlock {
        block,
        archive_sink,
        collection,
    } = args.mode
    {
        let store = build_json_archive(&archive_sink, &collection).await?;
        set_latest_block(&store, block).await?;
        println!("Set latest marker: key=\"latest\", block={block}");
        return Ok(());
    }

    let shared_args = args.mode.shared();
    let max_retries = shared_args.max_retries;

    let reader = shared_args
        .block_data_source
        .build(&Metrics::none())
        .await?;
    let json_archiver =
        build_json_archive(&shared_args.archive_sink, &shared_args.collection).await?;

    match args.mode {
        cli::Mode::SetStartBlock { .. } => unreachable!(),
        cli::Mode::WriteRange(ref write_range_args) => {
            tokio::spawn(write_range(
                shared_args.concurrency,
                reader,
                json_archiver,
                max_retries,
                write_range_args.start_block,
                write_range_args.stop_block,
            ))
            .await?;
        }
        cli::Mode::Stream(ref stream_args) => {
            loop {
                let Ok(mut start) = get_latest_block(&json_archiver)
                    .await
                    .inspect_err(|e| error!("Failed to get latest block: {e:?}"))
                else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };
                if start != 0 {
                    start += 1; // We already processed this block, so start from the next one
                }

                let Ok(Some(mut stop)) = reader
                    .get_latest(LatestKind::Uploaded)
                    .await
                    .inspect_err(|e| error!("Failed to get latest block: {e:?}"))
                else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };

                if start >= stop {
                    info!(
                        "No new blocks to process, sleeping for {} seconds",
                        stream_args.sleep_secs
                    );
                    tokio::time::sleep(Duration::from_secs_f64(stream_args.sleep_secs)).await;
                    continue;
                }
                stop = stop.min(start + stream_args.max_blocks_per_iter);
                let last_block = match tokio::spawn(write_range(
                    shared_args.concurrency,
                    reader.clone(),
                    json_archiver.clone(),
                    max_retries,
                    start,
                    stop,
                ))
                .await
                {
                    Ok(last_block) => last_block,
                    Err(e) => {
                        error!("Task panicked: {e:?}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };

                if let Err(e) = set_latest_block(&json_archiver, last_block).await {
                    error!("Failed to set latest block: {e:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
        }
    }

    Ok(())
}

async fn get_latest_block(store: &MongoJsonDocumentStore) -> Result<u64> {
    let latest = store.get_string("latest", "value").await?;
    match latest {
        Some(value) => value
            .parse::<u64>()
            .wrap_err("Failed to parse latest block"),
        None => {
            store.put_metadata("latest", "value", 0_i64).await?;
            Ok(0)
        }
    }
}

async fn set_latest_block(store: &MongoJsonDocumentStore, block: u64) -> Result<()> {
    store.put_metadata("latest", "value", block as i64).await?;
    Ok(())
}

async fn write_range(
    concurrency: usize,
    reader: BlockDataReaderErased,
    json_archiver: MongoJsonDocumentStore,
    max_retries: u32,
    start_block: u64,
    stop_block: u64,
) -> u64 {
    let mut failed_blocks: Vec<u64> = Vec::new();

    for attempt in 0..=max_retries {
        let blocks_to_process: TwoIters<RangeInclusive<u64>, IntoIter<u64>> = if attempt == 0 {
            // First attempt: process all blocks
            TwoIters::A(start_block..=stop_block)
        } else {
            // Retry: only process failed blocks
            info!(
                "Retry attempt {} for {} failed blocks",
                attempt,
                failed_blocks.len()
            );
            let iter = failed_blocks.into_iter();
            failed_blocks = Vec::new();
            TwoIters::B(iter)
        };

        futures::stream::iter(blocks_to_process)
            .map(|current_block| {
                let reader = reader.clone();
                let json_archiver = json_archiver.clone();

                async move {
                    tokio::spawn(async move {
                        process_block(&reader, current_block, &json_archiver)
                            .await
                            .map_err(|e| (current_block, e))
                    })
                    .await
                    .map_err(|e| (current_block, e))
                }
            })
            // Prefer buffered over unordered to lay down blocks in order to allow exeecution
            // to begin processing before all blocks are laid down.
            .buffered(concurrency)
            // Collect failed blocks for retry
            .for_each(|result| {
                match result {
                    Ok(Ok(())) => {
                        // Success - no retry needed
                    }
                    Ok(Err((block_num, e))) => {
                        error!("Failed to process block {}: {:?}", block_num, e);
                        failed_blocks.push(block_num);
                    }
                    Err((block_num, e)) => {
                        error!("Task panicked for block {}: {:?}", block_num, e);
                        failed_blocks.push(block_num);
                    }
                }
                futures::future::ready(())
            })
            .await;

        if failed_blocks.is_empty() {
            break;
        }
    }

    if !failed_blocks.is_empty() {
        let min_failed = *failed_blocks.iter().min().unwrap();
        error!(
            "Failed to process {} blocks after {} retries, earliest failure: {}",
            failed_blocks.len(),
            max_retries,
            min_failed
        );
        // Return the block before the first failure so we don't skip any blocks
        return min_failed.saturating_sub(1);
    }

    stop_block
}

enum TwoIters<A, B> {
    A(A),
    B(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for TwoIters<A, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::A(a) => a.next(),
            Self::B(b) => b.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_archive::test_utils::{mock_block, mock_rx, mock_tx};

    use super::*;

    fn create_test_block(
        block_number: u64,
        tx_count: usize,
    ) -> (Block, BlockReceipts, BlockTraces) {
        let block = mock_block(
            block_number,
            (0..tx_count).map(|i| mock_tx(i as u64)).collect(),
        );

        let receipts: BlockReceipts = (0..tx_count)
            .map(|i| mock_rx(10, (i + 1) as u128 * 21000))
            .collect();

        let traces: BlockTraces = (0..tx_count).map(|i| vec![i as u8, 1, 2, 3]).collect();

        (block, receipts, traces)
    }

    #[test]
    fn test_block_to_json_has_required_fields() {
        let (block, receipts, traces) = create_test_block(42, 2);
        let json = block_to_json(&block, &receipts, &traces);

        assert!(json.is_object(), "Result should be a JSON object");
        let obj = json.as_object().unwrap();

        assert!(obj.contains_key("header"), "Should have header field");
        assert!(obj.contains_key("body"), "Should have body field");
        assert!(obj.contains_key("receipts"), "Should have receipts field");
        assert!(obj.contains_key("traces"), "Should have traces field");
    }

    #[test]
    fn test_block_to_json_header_fields() {
        let (block, receipts, traces) = create_test_block(123, 1);
        let json = block_to_json(&block, &receipts, &traces);

        let header = json.get("header").unwrap().as_object().unwrap();

        assert_eq!(header.get("number").unwrap(), 123);
        assert_eq!(header.get("timestamp").unwrap(), 1234567);
        assert!(header.contains_key("parentHash"));
        assert!(header.contains_key("stateRoot"));
        assert!(header.contains_key("transactionsRoot"));
        assert!(header.contains_key("receiptsRoot"));
        assert!(header.contains_key("gasLimit"));
        assert!(header.contains_key("gasUsed"));
        assert!(header.contains_key("baseFeePerGas"));
    }

    #[test]
    fn test_block_to_json_body_fields() {
        let (block, receipts, traces) = create_test_block(1, 3);
        let json = block_to_json(&block, &receipts, &traces);

        let body = json.get("body").unwrap().as_object().unwrap();

        assert!(body.contains_key("transactions"));
        assert!(body.contains_key("ommers"));
        assert!(body.contains_key("withdrawals"));

        let transactions = body.get("transactions").unwrap().as_array().unwrap();
        assert_eq!(transactions.len(), 3);
    }

    #[test]
    fn test_block_to_json_receipts() {
        let (block, receipts, traces) = create_test_block(1, 2);
        let json = block_to_json(&block, &receipts, &traces);

        let json_receipts = json.get("receipts").unwrap().as_array().unwrap();
        assert_eq!(json_receipts.len(), 2);
    }

    #[test]
    fn test_block_to_json_traces_as_hex() {
        let (block, receipts, traces) = create_test_block(1, 2);
        let json = block_to_json(&block, &receipts, &traces);

        let json_traces = json.get("traces").unwrap().as_array().unwrap();
        assert_eq!(json_traces.len(), 2);

        // Traces should be hex-encoded strings
        let first_trace = json_traces[0].as_str().unwrap();
        assert!(
            first_trace.starts_with("0x"),
            "Traces should be hex with 0x prefix"
        );

        // First trace is vec![0, 1, 2, 3] -> "0x00010203"
        assert_eq!(first_trace, "0x00010203");

        // Second trace is vec![1, 1, 2, 3] -> "0x01010203"
        let second_trace = json_traces[1].as_str().unwrap();
        assert_eq!(second_trace, "0x01010203");
    }

    #[test]
    fn test_block_to_json_empty_block() {
        let (block, receipts, traces) = create_test_block(0, 0);
        let json = block_to_json(&block, &receipts, &traces);

        let body = json.get("body").unwrap().as_object().unwrap();
        let transactions = body.get("transactions").unwrap().as_array().unwrap();
        assert_eq!(transactions.len(), 0);

        let json_receipts = json.get("receipts").unwrap().as_array().unwrap();
        assert_eq!(json_receipts.len(), 0);

        let json_traces = json.get("traces").unwrap().as_array().unwrap();
        assert_eq!(json_traces.len(), 0);
    }

    #[test]
    fn test_block_to_json_preserves_block_number() {
        for block_num in [0, 1, 100, 999_999, 1_000_000] {
            let (block, receipts, traces) = create_test_block(block_num, 1);
            let json = block_to_json(&block, &receipts, &traces);

            let header = json.get("header").unwrap().as_object().unwrap();
            assert_eq!(header.get("number").unwrap(), block_num);
        }
    }
}
