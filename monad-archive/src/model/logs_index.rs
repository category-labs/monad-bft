use alloy_primitives::{hex::ToHexExt, Address, FixedBytes, Log};
use alloy_rpc_types::Topic;
use futures::{FutureExt, Stream};
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    Collection, IndexModel,
};

use crate::{
    kvstore::{mongo::KeyValueDocument, retry},
    prelude::*,
};

/// Enriches TxIndexData KV with fields `logs` and `block_number` to support efficient eth_getLogs filter queries
/// `logs` is an array containing address and topic prefixes for all the logs emitted in the tx
/// This index is similar to a bloom filter: it returns a superset of the correct matches. Prefix lengths were chosen
/// to provide a very small likelihood of false positives.
#[derive(Clone)]
pub struct LogsIndexArchiver {
    index_reader: IndexReaderImpl,
    collection: Collection<KeyValueDocument>,
    concurrency: usize,
}

impl LogsIndexArchiver {
    pub async fn from_tx_index_archiver(
        tx_index_reader: &IndexReaderImpl,
        concurrency: usize,
    ) -> Option<Self> {
        let KVReaderErased::MongoDbStorage(mongo_db_storage) = &tx_index_reader.index_store else {
            return None;
        };
        let collection = mongo_db_storage.collection.clone();

        let keys = doc! {
            "block_number": 1,
            "logs.address": 1,
            "logs.topic_0": 1,
            "logs.topic_1": 1,
            "logs.topic_2": 1,
            "logs.topic_3": 1,
        };
        collection
            .create_index(IndexModel::builder().keys(keys).build())
            .await
            .inspect_err(|e| {
                error!("Failed to create index: {e:?}");
            })
            .ok()?;

        Some(Self {
            collection,
            concurrency,
            index_reader: tx_index_reader.clone(),
        })
    }

    pub async fn index_block(&self, block: &Block, receipts: &BlockReceipts) -> Result<()> {
        let txs = TxLogsIndexDocument::from_receipts(block, receipts);
        if txs.is_empty() {
            return Ok(());
        }

        futures::stream::iter(txs.iter())
            .map(|tx| {
                retry(|| {
                    let collection = self.collection.clone();
                    let tx = tx.clone();
                    async move {
                        collection
                            .update_one(
                                doc! { "_id": tx._id },
                                doc! {
                                    "$set": {
                                        "logs": tx.logs,
                                        "block_number": tx.block_number as i64,
                                    }
                                },
                            )
                            .await
                            .wrap_err("Failed to insert logs")
                            .inspect_err(|e| {
                                warn!("Failed to insert logs: {e:?}. Retrying...");
                            })
                            .map(|_| ())
                    }
                })
            })
            .buffer_unordered(self.concurrency)
            .try_collect::<()>()
            .boxed()
            .await
            .wrap_err("Failed to insert logs after retries")
    }

    /// Query the logs index for a given range of blocks and filter criteria
    /// Note: query_logs returns a superset of the txs containing matching logs
    /// This means another pass of filtering must be done by client
    pub async fn query_logs<'a>(
        &'a self,
        from: u64,
        to: u64,
        addresses: impl IntoIterator<Item = &Address> + 'a + Send,
        topics: &'a [Topic],
    ) -> Result<impl Stream<Item = Result<TxIndexedData>> + 'a + Send> {
        let mut query = doc! {
            "block_number": { "$gte": from as i64, "$lte": to as i64 },
        };

        let address_prefixes = addresses
            .into_iter()
            .map(|a| fb_to_bson(a))
            .collect::<Vec<_>>();
        let num_addresses = address_prefixes.len();
        if !address_prefixes.is_empty() {
            query.insert("logs.address", doc! { "$in": address_prefixes });
        }

        let convert_topic = |t: &Topic| t.iter().map(fb_to_bson).collect::<Vec<_>>();
        for (i, topic) in topics.iter().enumerate() {
            if topic.is_empty() {
                continue;
            }
            query.insert(
                format!("logs.topic_{}", i),
                doc! { "$in": convert_topic(topic) },
            );
        }

        // Not efficient, but prefer for better debug logging.
        // Can be removed after this code has been battle tested
        let err_msg = format!("Failed to query logs: {query:?}");
        debug!(
            "Querying logs from {from} to {to}, with {num_addresses} addresses and {} topics",
            topics.len()
        );
        debug!("Logs mongo query: {query:?}");

        // Query for the kv document
        let cursor = self
            .collection
            .find(query)
            // do not return index fields, only kv
            .projection(doc! { "_id": true, "value": true })
            .await
            .wrap_err_with(|| err_msg)?;

        // Decode kv document and potentially resolve references to produce a TxIndexData
        Ok(cursor
            .then(|doc| {
                let reader = self.index_reader.clone();
                let collection = self.collection.clone();
                async move {
                    let doc = doc?;
                    let (_id, bytes) = doc.resolve(&collection).await?;
                    reader
                        .resolve_from_bytes(&bytes)
                        .await
                        .wrap_err("Failed to decode TxIndexedData when querying logs")
                }
            })
            .boxed())
    }
}

#[derive(Clone)]
pub struct TxLogsIndexDocument {
    pub block_number: u64,
    /// TxHash as lowercase hex without leading `0x`
    pub _id: String,
    pub logs: Vec<Document>,
}

impl TxLogsIndexDocument {
    pub fn from_receipts(block: &Block, receipts: &BlockReceipts) -> Vec<Self> {
        let block_number = block.header.number;

        receipts
            .iter()
            .zip(&block.body.transactions)
            .map(|(receipt, tx)| {
                let tx_hash_hex = tx.tx.tx_hash().encode_hex();
                TxLogsIndexDocument {
                    _id: tx_hash_hex,
                    block_number,
                    logs: logs_to_doc(receipt.receipt.logs()),
                }
            })
            .collect()
    }
}

fn logs_to_doc(logs: &[Log]) -> Vec<Document> {
    logs.iter()
        .map(|log| {
            let mut doc = doc! {
                "address": fb_to_bson(&log.address),
            };

            for (i, topic) in log.topics().iter().enumerate() {
                doc.insert(format!("topic_{}", i), fb_to_bson(topic));
            }

            doc
        })
        .collect()
}

fn fb_to_bson<const N: usize>(fb: &FixedBytes<N>) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: fb.as_slice()[0..4].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr};

    use alloy_rpc_types::FilterSet;
    use serde::Serialize;

    use super::*;
    use crate::{
        cli::BlockDataReaderArgs,
        kvstore::mongo::{mongo_tests::TestMongoContainer, MongoDbStorage},
    };

    async fn setup() -> Result<(TestMongoContainer, LogsIndexArchiver, TxIndexArchiver)> {
        let container = TestMongoContainer::new().await?;

        let mongo_storage = MongoDbStorage::new_index_store(
            &format!("mongodb://localhost:{}", container.port),
            "archive-db",
            Some(100_000),
        )
        .await?;
        let indexer = TxIndexArchiver::new(
            mongo_storage.clone(),
            BlockDataArchive::new(mongo_storage),
            1_000_000,
        );

        let logs_index_archiver = LogsIndexArchiver::from_tx_index_archiver(&indexer, 1)
            .await
            .expect("Failed to create logs index archiver");

        Ok((container, logs_index_archiver, indexer))
    }

    #[tokio::test]
    #[ignore]
    async fn test_basic_operations() {
        let (_container, logs_index_archiver, tx_indexer) = setup().await.unwrap();

        let bdr = BlockDataReaderArgs::from_str("aws testnet-ltu-032-0 50")
            .unwrap()
            .build(&Metrics::none())
            .await
            .unwrap();

        let block_number = 5_000_000;

        let (receipts, block, traces) = try_join!(
            bdr.get_block_receipts(block_number),
            bdr.get_block_by_number(block_number),
            bdr.get_block_traces(block_number),
        )
        .unwrap();

        let mut logs = Vec::new();
        for log in receipts
            .iter()
            .flat_map(|r| r.receipt.logs().iter().take(1))
            .take(10)
            .cloned()
        {
            println!("{}", serde_json::to_string(&log).unwrap());
            logs.push(log);
        }
        let log = &logs[5];

        tx_indexer
            .index_block(block.clone(), traces.clone(), receipts.clone(), None)
            .await
            .unwrap();

        logs_index_archiver
            .index_block(&block, &receipts)
            .await
            .unwrap();

        let x = logs_index_archiver
            .query_logs(block_number, block_number, &[], &[])
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.tx.tx.tx_hash().encode_hex())
            .collect::<Vec<String>>();

        let y = logs_index_archiver
            .collection
            .find(doc! { "block_number": block_number as i64 })
            .projection(doc! { "_id": true, "value": true })
            .await
            .unwrap()
            .try_collect::<Vec<KeyValueDocument>>()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v._id)
            .collect::<Vec<String>>();

        print_n(&x, 5);
        print_n(&y, 5);

        println!(
            "Compare mongo and with index, no topic filter. {} {}",
            x.len(),
            y.len()
        );
        assert_eq!(x.len(), y.len());
        assert_eq!(x, y, "Empty topic filter query returned different results");

        let mut x = logs_index_archiver
            .query_logs(
                block_number,
                block_number,
                &[log.address],
                &[FilterSet::from_iter([log.topics()[0]])],
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.tx.tx.tx_hash().encode_hex())
            .collect::<Vec<String>>();

        let y = logs_index_archiver
            .collection
            .find(doc! {
                "block_number": block_number as i64,
                "logs.address": fb_to_bson(&log.address),
                "logs.topic_0": fb_to_bson(&log.topics()[0])
            })
            .projection(doc! { "_id": true, "value": true })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v._id)
            .collect::<Vec<String>>();

        println!(
            "Compare direct mongo and with index, topic filter. {} {}",
            x.len(),
            y.len()
        );
        print_n(&x, 5);
        print_n(&y, 5);

        assert_eq!(x.len(), y.len());
        assert_eq!(x, y, "Empty topic filter query returned different results");

        let mut no_mongo = TxLogsIndexDocument::from_receipts(&block, &receipts)
            .into_iter()
            .filter(|l| {
                l.logs.iter().any(|doc| {
                    let topic_0 = doc.get("topic_0").unwrap();
                    let address = doc.get("address").unwrap();
                    *topic_0 == fb_to_bson(&log.topics()[0]) && *address == fb_to_bson(&log.address)
                })
            })
            .map(|l| l._id)
            .collect::<Vec<String>>();

        print_n(&no_mongo, 5);

        no_mongo.sort();
        x.sort();

        dbg!(&x, &no_mongo);

        for (i, (x, y)) in x.iter().zip(no_mongo.iter()).enumerate() {
            assert_eq!(
                x, y,
                "Index returned different results than the local index, {}",
                i
            );
        }

        let x_set = HashSet::<String>::from_iter(x);
        let no_mongo_set = HashSet::<String>::from_iter(no_mongo);
        assert_eq!(
            x_set.len(),
            no_mongo_set.len(),
            "Len of local index and mongo index are different"
        );
        assert_eq!(
            x_set, no_mongo_set,
            "MongoDB returned different results than the local index"
        );

        assert_eq!(
            &x_set, &no_mongo_set,
            "MongoDB returned different results than the local index"
        );
    }

    fn print_n<S: Serialize>(v: &[S], n: usize) {
        println!(
            "{}",
            serde_json::to_string(&v[0..(v.len().min(n))]).unwrap()
        );
    }
}
