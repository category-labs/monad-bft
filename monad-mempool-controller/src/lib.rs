use std::{
    future::Future,
    mem,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use ethers::{
    types::{transaction::eip2718::TypedTransaction, H256},
    utils::{hash_message, rlp::Rlp},
};
use monad_mempool_checker::{Checker, CheckerConfig};
use monad_mempool_messenger::{Messenger, MessengerConfig, MessengerError};
use monad_mempool_txpool::{Pool, PoolConfig};
use monad_mempool_types::tx::{PriorityTx, PriorityTxBatch};
use thiserror::Error;
use tokio::sync::mpsc::{self, Sender};
use tracing::{event, Level};

const DEFAULT_TX_THRESHOLD: usize = 1000;
const DEFAULT_TIME_THRESHOLD_S: u64 = 1;
const DEFAULT_BUFFER_SIZE: usize = 100000;

#[derive(Error, Debug)]
pub enum ControllerError {
    #[error("start() not yet called on Controller")]
    NotStartedError,
    #[error("Error with Messenger: {0}")]
    MessengerError(MessengerError),
}

pub struct ControllerConfig {
    checker_config: CheckerConfig,
    messenger_config: MessengerConfig,
    pool_config: PoolConfig,
    wait_for_peers: u8,
    // The interval at which the controller will create a new batch.
    time_threshold: Duration,
    // The maximum number of transactions that can be in a batch.
    tx_threshold: usize,
    // The maximum number of transactions that can be pending at a time.
    buffer_size: usize,
    // Whether to make the mempool local.
    // When the mempool is local, the controller will not broadcast transactions
    // to peers, and will create proposals with full transactions instead of
    // just the transaction hashes.
    local_mempool: bool,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            checker_config: CheckerConfig::default(),
            messenger_config: MessengerConfig::default(),
            pool_config: PoolConfig::default(),
            time_threshold: Duration::from_secs(DEFAULT_TIME_THRESHOLD_S),
            wait_for_peers: 1,
            tx_threshold: DEFAULT_TX_THRESHOLD,
            buffer_size: DEFAULT_BUFFER_SIZE,
            local_mempool: false,
        }
    }
}

impl ControllerConfig {
    pub fn with_checker_config(mut self, checker_config: CheckerConfig) -> Self {
        self.checker_config = checker_config;
        self
    }

    pub fn with_messenger_config(mut self, messenger_config: MessengerConfig) -> Self {
        self.messenger_config = messenger_config;
        self
    }

    pub fn with_pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }

    pub fn with_wait_for_peers(mut self, wait_for_peers: u8) -> Self {
        self.wait_for_peers = wait_for_peers;
        self
    }

    pub fn with_time_threshold(mut self, time_threshold: Duration) -> Self {
        self.time_threshold = time_threshold;
        self
    }

    pub fn with_tx_threshold(mut self, tx_threshold: usize) -> Self {
        self.tx_threshold = tx_threshold;
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn with_local_mempool(mut self, local_mempool: bool) -> Self {
        self.local_mempool = local_mempool;
        self
    }
}

pub struct Controller {
    checker: Checker,
    messenger: Messenger,
    pool: Arc<Mutex<Pool>>,
    wait_for_peers: u8,
    time_threshold: Duration,
    tx_threshold: usize,
    buffer_size: usize,
    local_mempool: bool,
    pending_tx_batch: Arc<Mutex<Vec<PriorityTx>>>,
    tx_sender: Option<mpsc::Sender<String>>,
}

impl Controller {
    pub fn new(config: &ControllerConfig) -> Self {
        let checker = Checker::new(&config.checker_config);
        let messenger = Messenger::new(&config.messenger_config);
        let pool = Arc::new(Mutex::new(Pool::new(&config.pool_config)));

        Self {
            checker,
            messenger,
            pool,
            wait_for_peers: config.wait_for_peers,
            time_threshold: config.time_threshold,
            tx_threshold: config.tx_threshold,
            buffer_size: config.buffer_size,
            local_mempool: config.local_mempool,
            pending_tx_batch: Arc::new(Mutex::new(Vec::new())),
            tx_sender: None,
        }
    }

    pub fn get_sender(&mut self) -> Result<mpsc::Sender<String>, ControllerError> {
        self.tx_sender
            .as_ref()
            .cloned()
            .ok_or(ControllerError::NotStartedError)
    }

    /// Starts the controller module, including the messenger.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (tx_sender, tx_receiver) = mpsc::channel::<String>(self.buffer_size);
        self.tx_sender = Some(tx_sender);

        self.messenger.start(self.wait_for_peers).await?;
        event! {Level::INFO, "Connected to at least 1 other peer, continuing..."}

        let mut interval = tokio::time::interval(self.time_threshold);

        // Spawn task to receive incoming transactions from the server, add
        // them to the mempool, and broadcast them to peers.
        tokio::spawn(self.listen_server(tx_receiver));

        if !self.local_mempool {
            // Spawn task to broadcast transaction batches every time_threshold seconds.
            tokio::spawn({
                let pending_tx_batch = self.pending_tx_batch.clone();
                let messenger_sender = self.messenger.get_sender().unwrap();

                async move {
                    loop {
                        interval.tick().await;
                        if let Err(e) =
                            Self::broadcast_tx_batch(&pending_tx_batch, &messenger_sender).await
                        {
                            event!(Level::ERROR, "Error broadcasting tx batch: {}", e);
                        };
                    }
                }
            });

            // Spawn task to receive incoming transaction batches from the messenger.
            // This task takes ownership of the messenger.
            tokio::spawn(self.listen_messenger());
        }

        Ok(())
    }

    pub fn create_proposal(&self) -> Vec<ethers::types::Bytes> {
        self.pool
            .lock()
            .unwrap()
            .create_proposal(self.local_mempool)
    }

    fn listen_server(&self, mut tx_receiver: mpsc::Receiver<String>) -> impl Future<Output = ()> {
        let checker = self.checker.clone();
        let messenger_sender = self.messenger.get_sender().unwrap();
        let pool = self.pool.clone();
        let pending_tx_batch = self.pending_tx_batch.clone();
        let tx_threshold = self.tx_threshold;
        let local_mempool = self.local_mempool;

        async move {
            while let Some(tx_hex) = tx_receiver.recv().await {
                let priority_tx = match Self::decode_priority_tx(&tx_hex) {
                    Ok(tx) => tx,
                    Err(e) => {
                        event!(Level::ERROR, "Error decoding tx: {}", e);
                        continue;
                    }
                };

                if let Err(e) = checker.check_priority_tx(&priority_tx) {
                    event!(Level::ERROR, "Rejecting tx: {}", e);
                    continue;
                }

                if let Err(e) = pool.lock().unwrap().insert(priority_tx.clone()) {
                    event!(Level::ERROR, "Error inserting tx into pool: {}", e);
                    continue;
                }

                let new_len = {
                    let mut lock = pending_tx_batch.lock().unwrap();
                    lock.push(priority_tx);
                    lock.len()
                };
                if !local_mempool && new_len >= tx_threshold {
                    if let Err(e) =
                        Self::broadcast_tx_batch(&pending_tx_batch, &messenger_sender).await
                    {
                        event!(
                            Level::ERROR,
                            "Error submitting tx batch to messenger: {}",
                            e
                        );
                    };
                }
            }
            event!(Level::ERROR, "Received None from tx receiver channel");
        }
    }

    fn listen_messenger(&mut self) -> impl Future<Output = ()> {
        let mut messenger = std::mem::replace(
            &mut self.messenger,
            Messenger::new(&MessengerConfig::default()),
        );
        let checker = self.checker.clone();
        let pool = self.pool.clone();

        async move {
            loop {
                let tx_batch = messenger.receive().await;
                match tx_batch {
                    Ok(Some(tx_batch)) => {
                        let expected_batch_hash = Self::calculate_batch_hash(&tx_batch.txs);
                        if expected_batch_hash.as_bytes() != tx_batch.hash.as_slice() {
                            event!(Level::ERROR, "Rejecting batch due to hash mismatch",);
                            continue;
                        }

                        event!(
                            Level::INFO,
                            "Received transaction batch of size {}",
                            tx_batch.numtx
                        );

                        let mut pool_guard = pool.lock().unwrap();
                        for priority_tx in tx_batch.txs {
                            if let Err(e) = checker.check_priority_tx(&priority_tx) {
                                event!(Level::ERROR, "Rejecting tx: {}", e);
                                continue;
                            }
                            if let Err(e) = pool_guard.insert(priority_tx) {
                                event!(Level::ERROR, "Could not insert tx into pool: {}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        event!(Level::ERROR, "Received None from messenger channel");
                        break;
                    }
                    Err(e) => {
                        event!(Level::ERROR, "Error receiving from messenger: {}", e);
                        break;
                    }
                }
            }
        }
    }

    /// Creates a new PriorityTxBatch and broadcasts it to peers via the messenger.
    async fn broadcast_tx_batch(
        pending_tx_batch: &Arc<Mutex<Vec<PriorityTx>>>,
        messenger_sender: &Sender<PriorityTxBatch>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Add a guard here to prevent the lock from being held across await
        let batch: PriorityTxBatch = {
            let mut pending_tx_batch = pending_tx_batch.lock().unwrap();
            if pending_tx_batch.len() == 0 {
                return Ok(());
            }

            let batch_hash = Self::calculate_batch_hash(&pending_tx_batch);
            PriorityTxBatch {
                hash: batch_hash.as_bytes().into(),
                numtx: pending_tx_batch.len() as u32,
                txs: mem::take(&mut pending_tx_batch),
                time: Some(SystemTime::now().into()),
            }
        };

        event!(Level::INFO, ?batch.hash, "Broadcasting batch of size {}", batch.numtx);
        messenger_sender.send(batch).await?;

        Ok(())
    }

    fn calculate_batch_hash(txs: &[PriorityTx]) -> H256 {
        let tx_hashes: Vec<u8> = txs.iter().flat_map(|tx| tx.hash.clone()).collect();
        hash_message(tx_hashes)
    }

    /// Decodes a hex-encoded transaction into a PriorityTx.
    fn decode_priority_tx(tx_hex: &String) -> Result<PriorityTx, Box<dyn std::error::Error>> {
        let tx_bytes: Vec<u8> = hex::decode(tx_hex)?;

        let rlp = Rlp::new(&tx_bytes);
        let (tx, sig) = TypedTransaction::decode_signed(&rlp)?;
        event!(Level::INFO, ?tx, ?sig, "Decoded transaction");

        let hash = tx.hash(&sig).as_bytes().to_vec();

        Ok(PriorityTx {
            hash,
            priority: Self::determine_priority(&tx),
            rlpdata: tx_bytes,
        })
    }

    fn determine_priority(_: &TypedTransaction) -> i64 {
        // TODO: Implement priority calculation
        0
    }
}
