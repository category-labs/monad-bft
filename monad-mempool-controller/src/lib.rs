use std::{
    future::Future,
    mem,
    ops::DerefMut,
    sync::Arc,
    task::Poll,
    time::{Duration, SystemTime},
};

use ethers::{
    types::{transaction::eip2718::TypedTransaction, H256},
    utils::{
        hash_message,
        rlp::{decode_list, encode_list, Rlp},
    },
};
use futures::{FutureExt, Stream};
use monad_mempool_checker::{Checker, CheckerConfig};
use monad_mempool_messenger::{Messenger, MessengerConfig, MessengerError};
use monad_mempool_txpool::{Pool, PoolConfig};
use monad_mempool_types::tx::{PriorityTx, PriorityTxBatch};
use thiserror::Error;
use tokio::{
    sync::{mpsc, Mutex},
    task::{JoinError, JoinHandle},
};
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
}

pub struct Controller {
    pool: Arc<Mutex<Pool>>,
    tx_sender: mpsc::Sender<String>,

    broadcast_handle: JoinHandle<()>,
    listen_messenger_handle: JoinHandle<()>,
    listen_server_handle: JoinHandle<()>,
}

impl Controller {
    pub async fn new(config: &ControllerConfig) -> Result<Self, MessengerError> {
        let checker = Checker::new(&config.checker_config);
        let mut messenger = Messenger::new(&config.messenger_config);
        let pool = Arc::new(Mutex::new(Pool::new(&config.pool_config)));

        let (tx_sender, tx_receiver) = mpsc::channel::<String>(config.buffer_size);

        messenger.start(config.wait_for_peers).await?;
        event! {Level::INFO, "Connected to at least 1 other peer, continuing..."}

        let pending_tx_batch = Arc::new(Mutex::new(Vec::new()));

        // Spawn task to receive incoming transactions from the server, add them to the mempool, and broadcast them to peers.
        let listen_server_handle = tokio::spawn(Self::listen_server(
            config,
            tx_receiver,
            checker.clone(),
            messenger.get_sender().unwrap(),
            pool.clone(),
            pending_tx_batch.clone(),
        ));

        // Spawn task to broadcast transaction batches every time_threshold seconds.
        let broadcast_handle = tokio::spawn(Self::listen_broadcast(
            config,
            pending_tx_batch,
            messenger.get_sender().unwrap(),
        ));

        // Spawn task to receive incoming transaction batches from the messenger, taking ownership of the messenger.
        let listen_messenger_handle =
            tokio::spawn(Self::listen_messenger(checker, messenger, pool.clone()));

        Ok(Self {
            pool,
            tx_sender,

            broadcast_handle,
            listen_messenger_handle,
            listen_server_handle,
        })
    }

    fn listen_server(
        config: &ControllerConfig,
        mut tx_receiver: mpsc::Receiver<String>,
        checker: Checker,
        messenger_sender: mpsc::Sender<PriorityTxBatch>,
        pool: Arc<Mutex<Pool>>,
        pending_tx_batch: Arc<Mutex<Vec<PriorityTx>>>,
    ) -> impl Future<Output = ()> {
        let tx_threshold = config.tx_threshold;

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

                if let Err(e) = pool.lock().await.insert(priority_tx.clone()) {
                    event!(Level::ERROR, "Error inserting tx into pool: {}", e);
                    continue;
                }

                let new_len = {
                    let mut lock = pending_tx_batch.lock().await;
                    lock.push(priority_tx);
                    lock.len()
                };

                if new_len >= tx_threshold {
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

    fn listen_broadcast(
        config: &ControllerConfig,
        pending_tx_batch: Arc<Mutex<Vec<PriorityTx>>>,
        messenger_sender: mpsc::Sender<PriorityTxBatch>,
    ) -> impl Future<Output = ()> {
        let mut interval = tokio::time::interval(config.time_threshold);

        async move {
            loop {
                interval.tick().await;

                if let Err(e) = Self::broadcast_tx_batch(&pending_tx_batch, &messenger_sender).await
                {
                    event!(Level::ERROR, "Error broadcasting tx batch: {}", e);
                };
            }
        }
    }

    async fn listen_messenger(checker: Checker, mut messenger: Messenger, pool: Arc<Mutex<Pool>>) {
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

                    let mut pool_guard = pool.lock().await;

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

    /// Creates a new PriorityTxBatch and broadcasts it to peers via the messenger.
    async fn broadcast_tx_batch(
        pending_tx_batch: &Arc<Mutex<Vec<PriorityTx>>>,
        messenger_sender: &mpsc::Sender<PriorityTxBatch>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Add a guard here to prevent the lock from being held across await
        let batch: PriorityTxBatch = {
            let mut pending_tx_batch = pending_tx_batch.lock().await;

            if pending_tx_batch.is_empty() {
                return Ok(());
            }

            let batch_hash = Self::calculate_batch_hash(&pending_tx_batch);

            PriorityTxBatch {
                hash: batch_hash.as_bytes().into(),
                numtx: pending_tx_batch.len().try_into()?,
                txs: mem::take(&mut pending_tx_batch),
                time: Some(SystemTime::now().into()),
            }
        };

        event!(Level::INFO, ?batch.hash, "Broadcasting batch of size {}", batch.numtx);

        messenger_sender.send(batch).await?;

        Ok(())
    }

    pub async fn create_proposal(&self) -> Vec<u8> {
        let proposal = self.pool.lock().await.create_proposal();

        encode_list::<Vec<u8>, _>(
            proposal
                .into_iter()
                .map(|b| b.0.into())
                .collect::<Vec<Vec<u8>>>()
                .as_slice(),
        )
        .to_vec()
    }

    pub async fn fetch_full_txs(&self, txs: Vec<u8>) -> Option<Vec<u8>> {
        let txs = decode_list::<Vec<u8>>(&txs);

        let full_txs = self.pool.lock().await.fetch_full_txs(
            txs.into_iter()
                .map(ethers::types::Bytes::from)
                .collect::<Vec<_>>(),
        );

        full_txs.map(|full_txs| {
            encode_list::<Vec<u8>, _>(
                full_txs
                    .into_iter()
                    .map(|b| b.0.into())
                    .collect::<Vec<Vec<u8>>>()
                    .as_slice(),
            )
            .to_vec()
        })
    }

    pub fn get_sender(&self) -> mpsc::Sender<String> {
        self.tx_sender.clone()
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

impl Drop for Controller {
    fn drop(&mut self) {
        self.broadcast_handle.abort();
        self.listen_messenger_handle.abort();
        self.listen_server_handle.abort();
    }
}

impl Stream for Controller {
    type Item = JoinError;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Poll::Ready(result) = this.broadcast_handle.poll_unpin(cx) {
            return Poll::Ready(result.err());
        }

        if let Poll::Ready(result) = this.listen_messenger_handle.poll_unpin(cx) {
            return Poll::Ready(result.err());
        }

        if let Poll::Ready(result) = this.listen_server_handle.poll_unpin(cx) {
            return Poll::Ready(result.err());
        }

        Poll::Pending
    }
}
