use alloy_primitives::hex::ToHexExt;
use clap::Parser;
use futures::{stream, StreamExt};
use monad_archive::{
    fault::{get_timestamp, BlockCheckResult, Fault, FaultWriter},
    prelude::*,
};
use tokio::join;

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();

    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-index-checker",
        args.source.replica_name(),
        Duration::from_secs(15),
    )?;

    // Construct s3 and dynamodb connections
    let reader = args.source.build_archive_reader(&metrics).await?;

    let mut latest_checked = args.start_block.unwrap_or(0);

    let mut fault_writer = FaultWriter::new(&args.checker_path).await?;

    loop {
        sleep(Duration::from_millis(100)).await;
        let start = Instant::now();

        // get latest indexed and indexed from s3
        let latest_indexed = match reader.get_latest(LatestKind::Uploaded).await {
            Ok(number) => number.unwrap_or(0),
            Err(e) => {
                warn!("Error getting latest uploaded block: {e:?}");
                continue;
            }
        };

        if let Some(stop_block_override) = args.stop_block {
            if latest_checked >= stop_block_override {
                info!("Reached stop block override, stopping...");
                return Ok(());
            }
        }

        if latest_checked >= latest_indexed {
            info!(latest_checked, latest_indexed, "Nothing to process");
            continue;
        }

        // compute start and end block for this upload
        let start_block_num = if latest_checked == 0 {
            0
        } else {
            latest_checked + 1
        };
        let end_block_num = latest_indexed.min(start_block_num + args.max_blocks_per_iteration);

        info!(
            start_block_num,
            end_block_num, latest_indexed, "Spawning checker tasks for blocks"
        );

        if let Err(e) = handle_blocks(
            &reader,
            start_block_num,
            end_block_num,
            args.concurrent_blocks,
            &mut fault_writer,
            &metrics,
        )
        .await
        {
            error!("Error handling blocks: {e:?}");
            continue;
        } else {
            latest_checked = end_block_num;
        }

        let duration = start.elapsed();
        info!("Time spent = {:?}", duration);
    }
}

async fn handle_blocks(
    reader: &ArchiveReader,
    start_block_num: u64,
    end_block_num: u64,
    concurrency: usize,
    fault_writer: &mut FaultWriter,
    metrics: &Metrics,
) -> Result<()> {
    let faults: Vec<_> = stream::iter(start_block_num..=end_block_num)
        .map(|block_num| async move {
            let check_result = tokio::spawn(handle_block(reader.clone(), block_num)).await;

            match check_result {
                Ok(Ok(fault)) => fault,
                Ok(Err(e)) => {
                    error!("Encountered error handling block: {e:?}");
                    BlockCheckResult {
                        timestamp: get_timestamp(),
                        block_num,
                        faults: vec![Fault::ErrorChecking {
                            err: format!("{e:?}"),
                        }],
                    }
                }
                Err(e) => {
                    error!("Encountered error handling block: {e:?}");
                    BlockCheckResult {
                        timestamp: get_timestamp(),
                        block_num,
                        faults: vec![Fault::ErrorChecking {
                            err: format!("{e:?}"),
                        }],
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    for block_check in &faults {
        if !block_check.faults.is_empty() {
            metrics.counter("faults_blocks_with_faults", 1);
        }
        for fault in &block_check.faults {
            match fault {
                Fault::ErrorChecking { .. } => metrics.counter("faults_error_checking", 1),
                Fault::CorruptedBlock => metrics.counter("faults_corrupted_blocks", 1),
                Fault::MissingAllTxHash { num_txs } => {
                    metrics.counter("faults_blocks_missing_all_txhash", 1);
                    metrics.counter("faults_missing_txhash", *num_txs as u64);
                }
                Fault::MissingTxhash { .. } => metrics.counter("faults_missing_txhash", 1),
                Fault::IncorrectTxData { .. } => metrics.counter("faults_incorrect_tx_data", 1),

                // Other faults are not DynamoDB faults
                _ => (),
            }
        }
    }

    fault_writer.write_faults(&faults).await
}

async fn handle_block(reader: ArchiveReader, block_num: u64) -> Result<BlockCheckResult> {
    let (block, traces, receipts) = match get_block_data(&reader, block_num).await {
        Ok(x) => x,
        Err(check_result) => return Ok(check_result),
    };
    let num_txs = block.body.transactions.len();
    info!(num_txs, block_num, "Handling block");

    if block.body.transactions.is_empty() {
        return Ok(BlockCheckResult::valid(block_num));
    }

    let hashes = block
        .body
        .transactions
        .iter()
        .map(|tx| *tx.tx.tx_hash())
        .collect::<Vec<_>>();

    let gas_used_vec: Vec<_> = {
        let mut last = 0;
        receipts
            .iter()
            .map(|r| {
                let gas_used = r.receipt.cumulative_gas_used() - last;
                last = r.receipt.cumulative_gas_used();
                gas_used
            })
            .collect()
    };

    let block_hash = block.header.hash_slow();
    let block_timestamp = block.header.timestamp;
    let base_fee_per_gas = block.header.base_fee_per_gas;
    let expected = block
        .body
        .transactions
        .into_iter()
        .zip(traces.into_iter())
        .zip(receipts.into_iter())
        .enumerate()
        .map(|(idx, ((tx, trace), receipt))| TxIndexedData {
            header_subset: HeaderSubset {
                block_hash,
                block_number: block_num,
                block_timestamp,
                tx_index: idx as u64,
                gas_used: gas_used_vec[idx],
                base_fee_per_gas,
            },
            tx,
            receipt,
            trace,
        });

    let fetched = reader.get_tx_indexed_data_bulk(&hashes).await?;
    let mut faults = Vec::new();

    for expected in expected {
        let key = expected.tx.tx.tx_hash();
        let fetched = fetched.get(key);
        let Some(fetched) = fetched else {
            faults.push(Fault::MissingTxhash {
                txhash: key.encode_hex(),
            });
            continue;
        };
        if fetched.header_subset.block_number != block_num {
            warn!(
                fetched_block_num = fetched.header_subset.block_number,
                block_num, "Fetched block_num incorrect"
            );
            faults.push(Fault::IncorrectTxData {
                fetched: fetched.clone(),
                expected,
            });
            continue;
        }
        if fetched != &expected {
            warn!(?fetched, ?expected, "Fetched does not equal expected");
            faults.push(Fault::IncorrectTxData {
                fetched: fetched.clone(),
                expected,
            });
            continue;
        }
    }

    // reduce all txhash case
    if faults.len() == num_txs
        && faults
            .iter()
            .all(|f| matches!(f, Fault::MissingTxhash { .. }))
    {
        faults.clear();
        faults.push(Fault::MissingAllTxHash { num_txs });
    }

    Ok(BlockCheckResult::new(block_num, faults))
}

async fn get_block_data(
    reader: &ArchiveReader,
    block_num: u64,
) -> std::result::Result<(Block, BlockTraces, BlockReceipts), BlockCheckResult> {
    let (block, traces, receipts) = join!(
        reader.get_block_by_number(block_num),
        reader.get_block_traces(block_num),
        reader.get_block_receipts(block_num)
    );

    match (block, traces, receipts) {
        (Ok(b), Ok(traces), Ok(receipts)) => Ok((b, traces, receipts)),
        (block, traces, receipts) => {
            let mut check_result = BlockCheckResult::new(block_num, Vec::new());
            if let Err(e) = block {
                warn!("Error fetching block: {e:?}");
                check_result.faults.push(Fault::S3MissingBlock {
                    buckets: vec![reader.get_bucket().to_owned()],
                });
            }
            if let Err(e) = traces {
                warn!("Error fetching traces: {e:?}");
                check_result.faults.push(Fault::S3MissingTraces {
                    buckets: vec![reader.get_bucket().to_owned()],
                });
            }
            if let Err(e) = receipts {
                warn!("Error fetching receipts: {e:?}");
                check_result.faults.push(Fault::S3MissingReceipts {
                    buckets: vec![reader.get_bucket().to_owned()],
                });
            }
            Err(check_result)
        }
    }
}
