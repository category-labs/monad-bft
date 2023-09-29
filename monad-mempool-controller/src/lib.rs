use std::{
    future::Future,
    mem,
    ops::DerefMut,
    path::PathBuf,
    sync::Arc,
    task::Poll,
    time::{Duration, SystemTime},
};

use ethers::{
    types::H256,
    utils::{
        hash_message,
        rlp::{decode_list, encode_list},
    },
};
use futures::{FutureExt, Stream, StreamExt};
use monad_mempool_checker::{Checker, CheckerConfig};
use monad_mempool_ipc::{generate_uds_path, MempoolTxIpcReceiver};
use monad_mempool_messenger::{Messenger, MessengerConfig, MessengerError};
use monad_mempool_txpool::{Pool, PoolConfig};
use monad_mempool_types::tx::{EthTx, EthTxBatch};
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
    mempool_ipc_path: PathBuf,
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
            mempool_ipc_path: generate_uds_path().into(),
            time_threshold: Duration::from_secs(DEFAULT_TIME_THRESHOLD_S),
            wait_for_peers: 0,
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

    pub fn with_mempool_ipc_path(mut self, mempool_ipc_path: PathBuf) -> Self {
        self.mempool_ipc_path = mempool_ipc_path;
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

    broadcast_handle: JoinHandle<()>,
    listen_ipc_handle: JoinHandle<()>,
    listen_messenger_handle: JoinHandle<()>,
}

impl Controller {
    pub async fn new(config: &ControllerConfig) -> Result<Self, MessengerError> {
        let messenger = Messenger::new(&config.messenger_config, config.wait_for_peers).await?;

        let checker = Checker::new(&config.checker_config);
        let pool = Arc::new(Mutex::new(Pool::new(&config.pool_config)));
        let pending_tx_batch = Arc::new(Mutex::new(Vec::new()));

        // Spawn task to broadcast transaction batches every time_threshold seconds.
        let broadcast_handle = tokio::spawn(Self::listen_broadcast(
            config,
            pending_tx_batch.clone(),
            messenger.get_sender(),
        ));

        let tx_receiver = MempoolTxIpcReceiver::new(config.mempool_ipc_path.as_path())
            .map_err(MessengerError::IpcReceiverBindError)?;

        // Spawn task to receive incoming transactions over ipc (from rpc server), add them to the mempool, and broadcast them to peers.
        let listen_ipc_handle = tokio::spawn(Self::listen_ipc(
            config,
            tx_receiver,
            checker.clone(),
            messenger.get_sender(),
            pool.clone(),
            pending_tx_batch,
        ));

        // Spawn task to receive incoming transaction batches from the messenger, taking ownership of the messenger.
        let listen_messenger_handle =
            tokio::spawn(Self::listen_messenger(checker, messenger, pool.clone()));

        Ok(Self {
            pool,

            broadcast_handle,
            listen_ipc_handle,
            listen_messenger_handle,
        })
    }

    fn listen_ipc(
        config: &ControllerConfig,
        mut tx_receiver: MempoolTxIpcReceiver,
        checker: Checker,
        messenger_sender: mpsc::Sender<EthTxBatch>,
        pool: Arc<Mutex<Pool>>,
        pending_tx_batch: Arc<Mutex<Vec<EthTx>>>,
    ) -> impl Future<Output = ()> {
        let tx_threshold = config.tx_threshold;

        async move {
            while let Some(Ok(eth_tx)) = tx_receiver.next().await {
                if let Err(e) = checker.check_eth_tx(&eth_tx) {
                    event!(Level::ERROR, "Rejecting tx: {}", e);
                    continue;
                }

                if let Err(e) = pool.lock().await.insert(eth_tx.clone()) {
                    event!(Level::ERROR, "Error inserting tx into pool: {}", e);
                    continue;
                }

                let new_len = {
                    let mut lock = pending_tx_batch.lock().await;
                    lock.push(eth_tx);
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
        pending_tx_batch: Arc<Mutex<Vec<EthTx>>>,
        messenger_sender: mpsc::Sender<EthTxBatch>,
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
        while let Some(tx_batch) = messenger.recv().await {
            let expected_batch_hash = Self::calculate_batch_hash(&tx_batch.txs);

            if expected_batch_hash.as_bytes() != tx_batch.hash.as_slice() {
                event!(Level::ERROR, "Rejecting batch due to hash mismatch");
                continue;
            }

            event!(
                Level::INFO,
                "Received transaction batch of size {}",
                tx_batch.numtx
            );

            let mut pool_guard = pool.lock().await;

            for eth_tx in tx_batch.txs {
                if let Err(e) = checker.check_eth_tx(&eth_tx) {
                    event!(Level::ERROR, "Rejecting tx: {}", e);
                    continue;
                }

                if let Err(e) = pool_guard.insert(eth_tx) {
                    event!(Level::ERROR, "Could not insert tx into pool: {}", e);
                }
            }
        }

        event!(Level::ERROR, "Received None from messenger channel");
    }

    /// Creates a new EthTxBatch and broadcasts it to peers via the messenger.
    async fn broadcast_tx_batch(
        pending_tx_batch: &Arc<Mutex<Vec<EthTx>>>,
        messenger_sender: &mpsc::Sender<EthTxBatch>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Add a guard here to prevent the lock from being held across await
        let batch: EthTxBatch = {
            let mut pending_tx_batch = pending_tx_batch.lock().await;

            if pending_tx_batch.is_empty() {
                return Ok(());
            }

            let batch_hash = Self::calculate_batch_hash(&pending_tx_batch);

            EthTxBatch {
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

    pub async fn create_proposal(&self, tx_limit: usize, pending_txs: Vec<Vec<u8>>) -> Vec<u8> {
        let pending_txs = pending_txs
            .into_iter()
            .map(|txs| decode_list::<Vec<u8>>(&txs))
            .fold(Vec::default(), |mut acc, txs| {
                acc.extend(txs);
                acc
            });

        let proposal = self
            .pool
            .lock()
            .await
            .create_proposal(tx_limit, pending_txs);

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

    pub async fn drain_txs(&self, drain_txs: Vec<u8>) {
        let drain_txs = decode_list::<Vec<u8>>(&drain_txs);

        self.pool.lock().await.remove_tx_hashes(
            drain_txs
                .into_iter()
                .map(ethers::types::Bytes::from)
                .collect::<Vec<_>>(),
        )
    }

    fn calculate_batch_hash(txs: &[EthTx]) -> H256 {
        let tx_hashes: Vec<u8> = txs.iter().flat_map(|tx| tx.hash.clone()).collect();

        hash_message(tx_hashes)
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.broadcast_handle.abort();
        self.listen_ipc_handle.abort();
        self.listen_messenger_handle.abort();
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

        if let Poll::Ready(result) = this.listen_ipc_handle.poll_unpin(cx) {
            return Poll::Ready(result.err());
        }

        if let Poll::Ready(result) = this.listen_messenger_handle.poll_unpin(cx) {
            return Poll::Ready(result.err());
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use std::{path::PathBuf, time::Duration};

    use ethers::{
        signers::LocalWallet,
        types::{transaction::eip2718::TypedTransaction, Address, TransactionRequest},
    };
    use futures::SinkExt;
    use monad_mempool_ipc::MempoolTxIpcSender;
    use monad_mempool_types::tx::EthTx;
    use oorandom::Rand32;
    use tempfile::NamedTempFile;

    use crate::{Controller, ControllerConfig};

    const NUM_CONTROLLER: u16 = 4;
    const NUM_TX: u16 = 10;

    #[tokio::test(flavor = "multi_thread")]
    /// Starts NUM_CONTROLLER controllers, sends NUM_TX messages to one controller
    /// and checks that all controllers create the same proposal.
    async fn test_multi_controller() {
        let mut controllers: Vec<(Controller, PathBuf)> = vec![];

        for _ in 0..NUM_CONTROLLER {
            let mempool_ipc_tempfile = NamedTempFile::new().unwrap();

            let mempool_ipc_temppath = mempool_ipc_tempfile.path().to_path_buf();

            mempool_ipc_tempfile.close().unwrap();

            controllers.push((
                Controller::new(
                    &ControllerConfig::default()
                        .with_wait_for_peers(0)
                        .with_mempool_ipc_path(mempool_ipc_temppath.clone()),
                )
                .await
                .unwrap(),
                mempool_ipc_temppath,
            ));
        }

        let (sender_controller, sender_mempool_ipc_file) = controllers.get_mut(0).unwrap();
        let mut sender = MempoolTxIpcSender::new(sender_mempool_ipc_file)
            .await
            .unwrap();

        let hex_txs = create_eth_txs(0, NUM_TX);
        for hex_tx in hex_txs {
            sender.send(hex_tx).await.unwrap();
        }

        // Allow time for controllers to receive the messages
        tokio::time::sleep(Duration::from_secs(2)).await;

        let sender_proposal = sender_controller
            .create_proposal(NUM_TX.into(), vec![])
            .await;

        for (controller, _) in &controllers {
            let proposal = controller.create_proposal(NUM_TX.into(), vec![]).await;
            assert_eq!(sender_proposal, proposal);
        }
    }

    const LOCAL_TEST_KEY: &str = "046507669b0b9d460fe9d48bb34642d85da927c566312ea36ac96403f0789b69";

    fn create_eth_txs(seed: u64, count: u16) -> Vec<EthTx> {
        let wallet = LOCAL_TEST_KEY.parse::<LocalWallet>().unwrap();

        create_txs(seed, count)
            .into_iter()
            .map(|tx| {
                let signature = wallet.sign_transaction_sync(&tx).unwrap();

                EthTx {
                    hash: tx.hash(&signature).as_bytes().to_vec(),
                    // priority: 0,
                    rlpdata: tx.rlp_signed(&signature).to_vec(),
                }
            })
            .collect()
    }

    fn create_txs(seed: u64, count: u16) -> Vec<TypedTransaction> {
        let mut rng = Rand32::new(seed);

        (0..count)
            .map(|_| {
                TransactionRequest::new()
                    .to("0xc582768697b4a6798f286a03A2A774c8743163BB"
                        .parse::<Address>()
                        .unwrap())
                    .gas(21337)
                    .gas_price(42)
                    .value(rng.rand_u32())
                    .nonce(0)
                    .into()
            })
            .collect()
    }
}

#[cfg(all(test, tokio_unstable))]
mod test {
    use tokio::runtime::Handle;

    use crate::{Controller, ControllerConfig};

    #[tokio::test]
    async fn test_tokio_thread_destruction() {
        let metrics = Handle::current().metrics();

        let start_tasks = metrics.active_tasks_count();

        let mempool_ipc_tempfile = NamedTempFile::new().unwrap();

        let mempool_ipc_temppath = mempool_ipc_tempfile.path().to_path_buf();

        mempool_ipc_tempfile.close().unwrap();

        let controller = Controller::new(
            &ControllerConfig::default()
                .with_wait_for_peers(0)
                .with_mempool_ipc_path(mempool_ipc_temppath.into()),
        )
        .await
        .unwrap();

        assert_ne!(start_tasks, metrics.active_tasks_count());

        drop(controller);

        tokio::task::yield_now().await;

        let end_tasks = metrics.active_tasks_count();

        assert_eq!(start_tasks, end_tasks);
    }
}
