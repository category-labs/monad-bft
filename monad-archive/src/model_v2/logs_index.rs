use alloy_primitives::{Address, FixedBytes, Log};
use alloy_rpc_types::Topic;
use futures::Stream;
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    Collection, IndexModel,
};

use crate::{
    model_v2::{resolve_inline_or_ref, resolve_trace_inline_or_ref, ModelV2, TxDoc},
    prelude::*,
};

/// Create indexes for eth_getLogs queries if they don't exist
pub(super) async fn ensure_logs_indexes(txs: &Collection<TxDoc>) -> Result<()> {
    txs.create_index(
        IndexModel::builder()
            .keys(doc! {
                "block_number": 1,
                "logs.address": 1,
                "logs.topic_0": 1,
                "logs.topic_1": 1,
                "logs.topic_2": 1,
                "logs.topic_3": 1,
            })
            .build(),
    )
    .await
    .wrap_err("Failed to create logs indexes")?;
    Ok(())
}

impl ModelV2 {
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
            "logs": { "$exists": true, "$ne": [] },
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

        debug!(
            "Querying logs from {from} to {to}, with {num_addresses} addresses and {} topics",
            topics.len()
        );
        debug!("Logs mongo query: {query:?}");

        // Query for the tx documents
        let cursor = self
            .txs
            .find(query)
            .await
            .wrap_err("Failed to query logs")?;

        // Convert to TxIndexedData stream
        Ok(cursor
            .then(move |doc| {
                let model = self.clone();
                async move {
                    let tx_doc: TxDoc = doc?;

                    // Fetch all the data needed for TxIndexedData
                    let (header, tx, trace, receipt) = futures::try_join!(
                        model.get_header(tx_doc.block_number as u64),
                        resolve_inline_or_ref::<TxEnvelopeWithSender>(
                            &model.s3_client,
                            tx_doc.body,
                            tx_doc.body_ref,
                        ),
                        resolve_trace_inline_or_ref(
                            &model.s3_client,
                            tx_doc.trace,
                            tx_doc.trace_ref
                        ),
                        resolve_inline_or_ref::<ReceiptWithLogIndex>(
                            &model.s3_client,
                            tx_doc.receipt,
                            tx_doc.receipt_ref,
                        ),
                    )?;

                    let header_subset = header.to_header_subset(tx_doc.tx_index, &tx_doc.gas_used);

                    Ok(TxIndexedData {
                        tx,
                        trace,
                        receipt,
                        header_subset,
                    })
                }
            })
            .boxed())
    }
}

pub(super) fn logs_to_doc(logs: &[Log]) -> Vec<Document> {
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
    use alloy_consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy_primitives::{Bloom, Log, LogData, B256};

    use super::*;
    use crate::{
        model_v2::tests::setup,
        test_utils::{mock_block, mock_tx},
    };

    fn create_receipt_with_logs(
        cumulative_gas: u128,
        logs: Vec<(Address, Vec<B256>)>,
    ) -> ReceiptWithLogIndex {
        let logs = logs
            .into_iter()
            .map(|(address, topics)| Log {
                address,
                data: LogData::new(topics, vec![42; 100].into()).unwrap(),
            })
            .collect();

        ReceiptWithLogIndex {
            receipt: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    logs,
                    status: alloy_consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: cumulative_gas,
                },
                Bloom::default(),
            )),
            starting_log_index: 0,
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_logs_indexing_comprehensive() -> Result<()> {
        let (_container, model) = setup().await?;

        // Define test addresses and topics
        let addr1 = Address::from([0x11; 20]);
        let addr2 = Address::from([0x22; 20]);
        let addr3 = Address::from([0x33; 20]);
        let addr4 = Address::from([0x44; 20]);

        let topic_a = B256::from([0xaa; 32]);
        let topic_b = B256::from([0xbb; 32]);
        let topic_c = B256::from([0xcc; 32]);
        let topic_d = B256::from([0xdd; 32]);

        // Block 100: 3 transactions with various log configurations
        let block1_num = 100;
        let block1_txs = vec![mock_tx(1), mock_tx(2), mock_tx(3)];
        let block1 = mock_block(block1_num, block1_txs);

        let block1_receipts = vec![
            // Tx1: Multiple logs with different addresses and topics
            create_receipt_with_logs(
                1000,
                vec![
                    (addr1, vec![topic_a]),
                    (addr1, vec![topic_b, topic_c]),
                    (addr2, vec![topic_a, topic_b]),
                ],
            ),
            // Tx2: Single log with multiple topics
            create_receipt_with_logs(
                2000,
                vec![(addr3, vec![topic_a, topic_b, topic_c, topic_d])],
            ),
            // Tx3: No logs (empty)
            create_receipt_with_logs(3000, vec![]),
        ];

        let block1_traces = vec![vec![1; 100], vec![2; 100], vec![3; 100]];

        // Block 101: 2 transactions
        let block2_num = 101;
        let block2_txs = vec![mock_tx(4), mock_tx(5)];
        let block2 = mock_block(block2_num, block2_txs);

        let block2_receipts = vec![
            // Tx4: Logs with overlapping topics from block1
            create_receipt_with_logs(
                4000,
                vec![(addr1, vec![topic_a]), (addr4, vec![topic_b, topic_c])],
            ),
            // Tx5: Different address/topic combination
            create_receipt_with_logs(5000, vec![(addr2, vec![topic_d])]),
        ];

        let block2_traces = vec![vec![4; 100], vec![5; 100]];

        // Archive and index both blocks
        model
            .archive_block_data(block1.clone(), block1_receipts.clone(), block1_traces)
            .await?;
        // model.index_block_logs(&block1, &block1_receipts).await?;

        model
            .archive_block_data(block2.clone(), block2_receipts.clone(), block2_traces)
            .await?;
        // model.index_block_logs(&block2, &block2_receipts).await?;

        // Test 1: Query by address only (addr1 appears in both blocks)
        let results: Vec<_> = model
            .query_logs(block1_num, block2_num, &[addr1], &[])
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 2); // Tx1 and Tx4

        // Test 2: Query by multiple addresses
        let results: Vec<_> = model
            .query_logs(block1_num, block2_num, &[addr1, addr2], &[])
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 3); // Tx1, Tx4, Tx5

        // Test 3: Query by topic only (topic_a)
        let results: Vec<_> = model
            .query_logs(
                block1_num,
                block2_num,
                &[],
                &[alloy_rpc_types::FilterSet::from(topic_a)],
            )
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 3); // Tx1, Tx2, Tx4

        // Test 4: Query by address AND topic
        let results: Vec<_> = model
            .query_logs(
                block1_num,
                block2_num,
                &[addr1],
                &[alloy_rpc_types::FilterSet::from(topic_a)],
            )
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 2); // Tx1 and Tx4 (both have addr1 with topic_a)

        // Test 5: Query with multiple topics (topic at position 0 and 1)
        let results: Vec<_> = model
            .query_logs(
                block1_num,
                block2_num,
                &[],
                &[
                    alloy_rpc_types::FilterSet::from(topic_a),
                    alloy_rpc_types::FilterSet::from(topic_b),
                ],
            )
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 2); // Tx1 and Tx2 (both have topic_a at [0] and topic_b at [1])

        // Test 6: Query specific block range
        let results: Vec<_> = model
            .query_logs(block2_num, block2_num, &[addr1], &[])
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 1); // Only Tx4 from block2

        // Test 7: Empty results (no matching address)
        let results: Vec<_> = model
            .query_logs(
                block1_num,
                block2_num,
                &[Address::from([0xff; 20])], // Non-existent address
                &[],
            )
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 0);

        // Test 8: Query with OR topics (multiple possibilities for same position)
        let results: Vec<_> = model
            .query_logs(
                block1_num,
                block2_num,
                &[],
                &[alloy_rpc_types::FilterSet::from(vec![topic_a, topic_b])], // topic_a OR topic_b at position 0
            )
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 3); // Tx1 (has topic_a and topic_b at position 0), Tx2 (has topic_a), Tx4 (has topic_a)

        // Test 9: Query all logs (no filters)
        let results: Vec<_> = model
            .query_logs(block1_num, block2_num, &[], &[])
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 4); // All txs with logs: Tx1, Tx2, Tx4, Tx5

        // Test 10: Complex multi-position topic filter
        let results: Vec<_> = model
            .query_logs(
                block1_num,
                block2_num,
                &[],
                &[
                    alloy_rpc_types::FilterSet::from(topic_a), // position 0
                    alloy_rpc_types::FilterSet::from(topic_b), // position 1
                    alloy_rpc_types::FilterSet::from(topic_c), // position 2
                    alloy_rpc_types::FilterSet::from(topic_d), // position 3
                ],
            )
            .await?
            .try_collect()
            .await?;
        assert_eq!(results.len(), 1); // Only Tx2 has all 4 topics in order

        Ok(())
    }
}
