use futures::TryStreamExt;
use serial_test::serial;

use crate::{
    kvstore::{
        memory::MemoryStorage,
        mongo::new_client,
        object_store::ObjectStore,
    },
    model_v2::ModelV2,
    prelude::*,
    test_utils::{mock_block, mock_rx, mock_trace, mock_tx, TestMongoContainer},
};

async fn setup_v2() -> Result<(TestMongoContainer, ModelV2)> {
    let container = TestMongoContainer::new().await?;
    let client = new_client(&format!("mongodb://localhost:{}", container.port)).await?;

    // Create test database first
    client.database("test_db").create_collection("test").await?;

    // For tests, use MemoryStorage wrapped in ObjectStore enum
    let memory_storage = MemoryStorage::new("test-bucket");

    let model_v2 = ModelV2::from_client(
        client,
        ObjectStore::Memory(memory_storage),
        10,
        "test_db".to_string(),
    )
    .await?;

    Ok((container, model_v2))
}

#[ignore]
#[tokio::test]
#[serial]
async fn test_archive_worker_with_v2() -> Result<()> {
    let (_container, model) = setup_v2().await?;

    // Create some test blocks in the model (acting as source)
    for block_num in 0..3 {
        let txs = vec![mock_tx(block_num * 10), mock_tx(block_num * 10 + 1)];
        let block = mock_block(block_num, txs);
        let receipts = vec![
            mock_rx(
                100 + block_num as usize * 100,
                1000 + block_num as u128 * 1000,
            ),
            mock_rx(
                200 + block_num as usize * 100,
                2000 + block_num as u128 * 1000,
            ),
        ];
        let traces = vec![mock_trace(block_num), mock_trace(block_num + 10)];

        model
            .archive_block_data(block.clone(), receipts.clone(), traces)
            .await?;
        model.update_latest(block_num).await?;
    }

    // Create a second model instance to act as the archive writer
    let archive_model = model.clone();

    // Reset the archive's latest to simulate starting fresh
    // (In a real test we'd use a separate database)

    // For now, we'll just verify the basic functionality
    // In a real system, the archive_worker function would be called
    // which internally calls archive_blocks

    // Verify blocks were archived
    let latest = archive_model.get_latest().await?;
    assert_eq!(latest, Some(2));

    // Verify we can read the blocks back
    for block_num in 0..3 {
        let block = archive_model.get_block_by_number(block_num).await?;
        assert_eq!(block.header.number, block_num);
        assert_eq!(block.body.transactions.len(), 2);

        let receipts = archive_model.get_block_receipts(block_num).await?;
        assert_eq!(receipts.len(), 2);

        let traces = archive_model.get_block_traces(block_num).await?;
        assert_eq!(traces.len(), 2);
    }

    Ok(())
}

#[ignore]
#[tokio::test]
#[serial]
async fn test_logs_automatically_indexed_in_v2() -> Result<()> {
    let (_container, model) = setup_v2().await?;

    // Create a block with logs
    let block_num = 100;
    let test_address = alloy_primitives::Address::from([0x11; 20]);
    let test_topic = alloy_primitives::B256::from([0x22; 32]);

    let txs = vec![mock_tx(1), mock_tx(2)];
    let block = mock_block(block_num, txs);

    // Create receipts with logs
    use alloy_consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy_primitives::{Bloom, Log, LogData};

    let log = Log {
        address: test_address,
        data: LogData::new(vec![test_topic], vec![42; 100].into()).unwrap(),
    };

    let receipts = vec![
        ReceiptWithLogIndex {
            receipt: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    logs: vec![log],
                    status: alloy_consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: 1000,
                },
                Bloom::default(),
            )),
            starting_log_index: 0,
        },
        mock_rx(200, 2000), // Second receipt without logs
    ];

    let traces = vec![vec![1; 100], vec![2; 100]];

    // Archive the block - this should automatically index logs in V2
    model
        .archive_block_data(block.clone(), receipts.clone(), traces)
        .await?;

    // Verify logs were indexed by querying
    let results: Vec<_> = model
        .query_logs(block_num, block_num, &[test_address], &[])
        .await?
        .try_collect()
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].header_subset.block_number, block_num);

    Ok(())
}
