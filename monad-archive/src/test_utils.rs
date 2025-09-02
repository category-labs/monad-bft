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

use std::{process::Command, sync::atomic::AtomicU16};

pub use alloy_consensus::{Receipt, SignableTransaction, TxEip1559};
pub use alloy_primitives::{Bloom, Log, LogData, B256};
pub use alloy_signer::SignerSync;
pub use alloy_signer_local::PrivateKeySigner;
use mongodb::{options::ClientOptions, Client};

pub use crate::{kvstore::memory::MemoryStorage, prelude::*};

pub fn mock_tx(salt: u64) -> TxEnvelopeWithSender {
    let tx = TxEip1559 {
        nonce: salt,
        gas_limit: 456 + salt,
        max_fee_per_gas: 789,
        max_priority_fee_per_gas: 135,
        ..Default::default()
    };
    let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    let tx = tx.into_signed(sig);
    TxEnvelopeWithSender {
        tx: tx.into(),
        sender: signer.address(),
    }
}

pub fn mock_rx(receipt_len: usize, cumulative_gas: u128) -> ReceiptWithLogIndex {
    let receipt = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
        Receipt::<Log> {
            logs: vec![Log {
                address: Default::default(),
                data: LogData::new(
                    vec![],
                    std::iter::repeat_n(42, receipt_len)
                        .collect::<Vec<u8>>()
                        .into(),
                )
                .unwrap(),
            }],
            status: alloy_consensus::Eip658Value::Eip658(true),
            cumulative_gas_used: cumulative_gas,
        },
        Bloom::repeat_byte(b'a'),
    ));
    ReceiptWithLogIndex {
        receipt,
        starting_log_index: 0,
    }
}

pub fn mock_block(number: u64, transactions: Vec<TxEnvelopeWithSender>) -> Block {
    Block {
        header: Header {
            number,
            timestamp: 1234567,
            base_fee_per_gas: Some(100),
            ..Default::default()
        },
        body: BlockBody {
            transactions,
            ommers: vec![],
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
        },
    }
}

pub struct TestMinioContainer {
    pub port: u16,
    pub container_id: String,
}

impl TestMinioContainer {
    pub async fn new() -> Result<Self> {
        let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "-p",
                &format!("{port}:9000"),
                "minio/minio",
                "server",
                "/tmp/minio",
            ])
            .output()
            .wrap_err("Failed to start Minio container")?;

        let container_id = String::from_utf8(output.stdout)
            .wrap_err("Invalid container ID output")?
            .trim()
            .to_string();

        // Wait for Minio to be ready by attempting to connect to the port
        let addr = format!("127.0.0.1:{}", port);
        let mut ready = false;
        let max_attempts = 30;
        for _ in 0..max_attempts {
            match tokio::net::TcpStream::connect(&addr).await {
                Ok(_) => {
                    ready = true;
                    break;
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        if !ready {
            eyre::bail!("Minio container did not become ready in time");
        }

        Ok(Self { port, container_id })
    }
}

impl Drop for TestMinioContainer {
    fn drop(&mut self) {
        println!("Stopping Minio container: {}", self.container_id);
        Command::new("docker")
            .args(["stop", &self.container_id])
            .output()
            .expect("Failed to stop Minio container");
        Command::new("docker")
            .args(["rm", &self.container_id])
            .output()
            .expect("Failed to remove Minio container");
    }
}

pub struct TestMongoContainer {
    pub container_id: String,
    pub uri: String,
    pub port: u16,
    pub temp_dir: Option<tempfile::TempDir>,
}

static NEXT_PORT: AtomicU16 = AtomicU16::new(2812);

impl TestMongoContainer {
    pub async fn new() -> Result<Self> {
        let container_id = mongodb::bson::uuid::Uuid::new();
        let container_name = format!("mongo_test_{}", container_id);
        let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Create temporary directory for MongoDB data
        let temp_dir = tempfile::TempDir::new().wrap_err("Failed to create temp directory")?;
        let data_path = temp_dir.path().to_string_lossy();

        // Set permissions on temp directory to be writable by MongoDB
        Command::new("chmod")
            .args(["777", &data_path])
            .output()
            .wrap_err("Failed to set permissions on temp directory")?;

        // Start container with mounted volume
        // Use explicit mongod command to ensure proper startup
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "-p",
                &format!("{port}:27017"),
                "--name",
                &container_name,
                "-v",
                &format!("{}:/data/db", data_path),
                "mongo:latest",
                "mongod",
                "--dbpath",
                "/data/db",
                "--bind_ip_all",
            ])
            .output()
            .wrap_err("Failed to start MongoDB container")?;

        let container_id = String::from_utf8(output.stdout)
            .wrap_err("Invalid container ID output")?
            .trim()
            .to_string();

        println!(
            "Starting MongoDB container: {}, {}",
            container_name, container_id
        );

        let output = Command::new("docker")
            .args(["ps"])
            .output()
            .expect("Failed to list containers");

        println!("Containers: {}", String::from_utf8(output.stdout).unwrap());

        // Poll until MongoDB is ready
        let client_options = ClientOptions::parse(format!("mongodb://localhost:{port}"))
            .await
            .unwrap();
        let max_attempts = 30; // 30 * 200ms = 6 seconds max
        let mut attempt = 0;

        while attempt < max_attempts {
            match Client::with_options(client_options.clone()) {
                Ok(client) => {
                    // Try to actually connect and run a command
                    match client.list_database_names().await {
                        Ok(_) => {
                            return Ok(Self {
                                container_id,
                                uri: format!("mongodb://localhost:{port}"),
                                port,
                                temp_dir: Some(temp_dir),
                            })
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            attempt += 1;
                            continue;
                        }
                    }
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    attempt += 1;
                    continue;
                }
            }
        }

        bail!("MongoDB container failed to become ready")
    }
}

impl Drop for TestMongoContainer {
    fn drop(&mut self) {
        println!("Stopping MongoDB container: {}", self.container_id);
        Command::new("docker")
            .args(["stop", &self.container_id])
            .output()
            .expect("Failed to stop MongoDB container");
        Command::new("docker")
            .args(["rm", &self.container_id])
            .output()
            .expect("Failed to remove MongoDB container");
    }
}
