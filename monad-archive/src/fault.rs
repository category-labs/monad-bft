use std::{path::Path, sync::Arc};

use chrono::prelude::Local;
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, sync::Mutex};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockCheckResult {
    pub timestamp: String,
    pub block_num: u64,
    pub faults: Vec<Fault>,
}

impl BlockCheckResult {
    pub fn valid(block_num: u64) -> BlockCheckResult {
        BlockCheckResult {
            timestamp: get_timestamp(),
            block_num,
            faults: vec![],
        }
    }

    pub fn new(block_num: u64, faults: Vec<Fault>) -> BlockCheckResult {
        BlockCheckResult {
            timestamp: get_timestamp(),
            block_num,
            faults,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Fault {
    ErrorChecking {
        err: String,
    },

    // DynamoDB errors
    CorruptedBlock,
    MissingAllTxHash {
        num_txs: u64,
    },
    MissingTxhash {
        txhash: String,
    },
    WrongBlockNumber {
        txhash: String,
        wrong_block_num: u64,
    },

    // S3 errors
    S3MissingBlock {
        buckets: Vec<String>,
    },
    S3MissingReceipts {
        buckets: Vec<String>,
    },
    S3MissingTraces {
        buckets: Vec<String>,
    },
    // pairwise inconsistency
    S3InconsistentBlock {
        bucket1: String,
        bucket2: String,
    },
    S3InconsistentReceipts {
        bucket1: String,
        bucket2: String,
    },
    S3InconsistentTraces {
        bucket1: String,
        bucket2: String,
    },
}

#[derive(Clone)]
pub struct FaultWriter {
    file: Arc<Mutex<tokio::io::BufWriter<tokio::fs::File>>>,
}

impl FaultWriter {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            // .append(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .wrap_err_with(|| format!("Failed to create Fault Writer. Path {:?}", path.as_ref()))?;
        Ok(Self {
            file: Arc::new(Mutex::new(tokio::io::BufWriter::new(file))),
        })
    }

    pub async fn write_faults<'a>(
        &mut self,
        block_faults: impl IntoIterator<Item = &'a BlockCheckResult>,
    ) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);
        let mut file = self.file.lock().await;
        for fault in block_faults {
            serde_json::to_writer(&mut buf, fault)?;
            file.write_all(&buf).await?;
            file.write(b"\n").await?;
            buf.clear();
        }
        file.flush().await?;
        Ok(())
    }

    pub async fn write_fault(&mut self, block_fault: BlockCheckResult) -> Result<()> {
        // serde_json::to_writer(&mut self.file, &block_fault)?;
        let buf = serde_json::to_vec(&block_fault)?;
        let mut file = self.file.lock().await;
        file.write_all(&buf).await?;
        file.write(b"\n").await?;
        file.flush().await.map_err(Into::into)
    }
}

pub fn get_timestamp() -> String {
    let now = Local::now();
    now.format("%d/%m/%Y %H:%M:%S:%.3f").to_string()
}
