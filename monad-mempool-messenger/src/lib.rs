mod gossipsub;

use gossipsub::GossipSub;
use libp2p::identity::Keypair;
use monad_mempool_types::tx::PriorityTxBatch;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::event;

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
    gossipsub: GossipSub<64>,
    sender: Option<mpsc::Sender<PriorityTxBatch>>,
    receiver: Option<mpsc::Receiver<PriorityTxBatch>>,
}

impl Messenger {
    pub fn new(config: &MessengerConfig) -> Self {
        let gossipsub = GossipSub::new(config.local_key.clone(), config.port);

        Self {
            gossipsub,
            sender: None,
            receiver: None,
        }
    }

    pub fn get_sender(&self) -> Result<mpsc::Sender<PriorityTxBatch>, MessengerError> {
        self.sender
            .as_ref()
            .ok_or(MessengerError::NotStartedError)
            .map(|s| s.clone())
    }

    pub async fn receive(&mut self) -> Result<Option<PriorityTxBatch>, MessengerError> {
        let rx = self
            .receiver
            .as_mut()
            .ok_or(MessengerError::NotStartedError)?;

        Ok(rx.recv().await)
    }

    pub async fn start(&mut self, wait_for_peers: u8) -> Result<(), MessengerError> {
        let (sender, receiver, mut connected_rx) = self
            .gossipsub
            .start::<PriorityTxBatch>()
            .map_err(|_| MessengerError::GossipSubStartError)?;
        self.sender = Some(sender);
        self.receiver = Some(receiver);

        event! {tracing::Level::INFO, "Messenger started, waiting for {} peer(s)...", wait_for_peers};
        for _ in 0..wait_for_peers {
            connected_rx.recv().await;
        }
        event! {tracing::Level::INFO, "Connected to {} peer(s)" , wait_for_peers};

        Ok(())
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
        let mut head = Messenger::new(&MessengerConfig::default());
        let mut receiver1 = Messenger::new(&MessengerConfig::default());
        let mut receiver2 = Messenger::new(&MessengerConfig::default());

        receiver1.start(0).await.unwrap();
        receiver2.start(0).await.unwrap();
        head.start(2).await.unwrap();

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

        let sender = head.get_sender().unwrap();
        for i in &batches {
            sender.send(i.clone()).await.unwrap();
        }

        for i in &batches {
            assert_eq!(
                timeout(Duration::from_secs(TIMEOUT_SEC), receiver1.receive())
                    .await
                    .unwrap()
                    .unwrap(),
                Some(i.clone())
            );
            assert_eq!(
                timeout(Duration::from_secs(TIMEOUT_SEC), receiver2.receive())
                    .await
                    .unwrap()
                    .unwrap(),
                Some(i.clone())
            );
        }
    }
}
