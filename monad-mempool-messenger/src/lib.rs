mod gossipsub;

use libp2p::identity::Keypair;
use monad_mempool_types::tx::PriorityTxBatch;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::event;

use crate::gossipsub::GOSSIP_SUB_DEFAULT_BUFFER_SIZE;

#[derive(Error, Debug)]
pub enum MessengerError {
    #[error("start() not yet called on Messenger")]
    NotStartedError,

    #[error("error starting gossipsub")]
    GossipSubStartError,
}

pub struct MessengerConfig {
    local_key: Keypair,
    port: u16,
}

impl Default for MessengerConfig {
    fn default() -> Self {
        Self {
            local_key: Keypair::generate_ed25519(),
            port: 0,
        }
    }
}

impl MessengerConfig {
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_local_key(mut self, local_key: Keypair) -> Self {
        self.local_key = local_key;
        self
    }
}

pub struct Messenger {
    sender: mpsc::Sender<PriorityTxBatch>,
    receiver: mpsc::Receiver<PriorityTxBatch>,

    gossipsub_listen_handle: JoinHandle<()>,
}

impl Messenger {
    pub async fn new(config: &MessengerConfig, wait_for_peers: u8) -> Result<Self, MessengerError> {
        let (gossipsub_listen_handle, sender, receiver, mut connected_rx) =
            gossipsub::start_gossipsub(
                config.local_key.clone(),
                config.port,
                GOSSIP_SUB_DEFAULT_BUFFER_SIZE,
            )
            .map_err(|_| MessengerError::GossipSubStartError)?;

        event! {tracing::Level::INFO, "Messenger started, waiting for {} peer(s)...", wait_for_peers};

        for _ in 0..wait_for_peers {
            connected_rx.recv().await;
        }

        event! {tracing::Level::INFO, "Messenger connected to {} peer(s)" , wait_for_peers};

        Ok(Self {
            sender,
            receiver,

            gossipsub_listen_handle,
        })
    }

    pub fn get_sender(&self) -> mpsc::Sender<PriorityTxBatch> {
        self.sender.clone()
    }

    pub async fn recv(&mut self) -> Option<PriorityTxBatch> {
        self.receiver.recv().await
    }
}

impl Drop for Messenger {
    fn drop(&mut self) {
        self.gossipsub_listen_handle.abort();
    }
}

#[cfg(test)]
mod test {
    use monad_mempool_types::tx::{PriorityTx, PriorityTxBatch};
    use tokio::time::{timeout, Duration};

    use super::{Messenger, MessengerConfig};

    const TIMEOUT_SEC: u64 = 5;

    #[tokio::test]
    async fn test_messenger() {
        let mut receiver1 = Messenger::new(&MessengerConfig::default(), 0)
            .await
            .unwrap();
        let mut receiver2 = Messenger::new(&MessengerConfig::default(), 0)
            .await
            .unwrap();
        let head = Messenger::new(&MessengerConfig::default(), 2)
            .await
            .unwrap();

        let batches = (0..1)
            .map(|i| {
                let tx = PriorityTx {
                    hash: format!("hash{i}").as_bytes().to_vec(),
                    priority: i as i64,
                    rlpdata: format!("rlpdata{i}").as_bytes().to_vec(),
                };
                PriorityTxBatch {
                    hash: format!("batchhash{i}").as_bytes().to_vec(),
                    txs: vec![tx],
                    time: Some(std::time::SystemTime::now().into()),
                    numtx: 1,
                }
            })
            .collect::<Vec<_>>();

        let sender = head.get_sender();
        for i in &batches {
            sender.send(i.clone()).await.unwrap();
        }

        for i in &batches {
            assert_eq!(
                timeout(Duration::from_secs(TIMEOUT_SEC), receiver1.recv())
                    .await
                    .unwrap(),
                Some(i.clone())
            );
            assert_eq!(
                timeout(Duration::from_secs(TIMEOUT_SEC), receiver2.recv())
                    .await
                    .unwrap(),
                Some(i.clone())
            );
        }
    }
}
