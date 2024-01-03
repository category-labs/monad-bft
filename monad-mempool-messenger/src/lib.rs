mod gossipsub;

use std::net::Ipv4Addr;

use libp2p::identity::Keypair;
use monad_mempool_proto::tx::UnverifiedEthTxBatch;
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

    #[error(transparent)]
    IpcReceiverBindError(std::io::Error),
}

#[derive(Clone)]
pub struct MessengerConfig {
    local_key: Keypair,
    address: Ipv4Addr,
    port: u16,
    bootstrap_peers: Vec<(Ipv4Addr, u16)>,
}

impl Default for MessengerConfig {
    fn default() -> Self {
        Self {
            local_key: Keypair::generate_ed25519(),
            address: Ipv4Addr::new(0, 0, 0, 0),
            port: 0,
            bootstrap_peers: Vec::new(),
        }
    }
}

impl MessengerConfig {
    pub fn with_local_key(mut self, local_key: Keypair) -> Self {
        self.local_key = local_key;
        self
    }

    pub fn with_address(mut self, address: Ipv4Addr) -> Self {
        self.address = address;
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_bootstrap_peers(mut self, bootstrap_peers: Vec<(Ipv4Addr, u16)>) -> Self {
        self.bootstrap_peers = bootstrap_peers;
        self
    }
}

pub struct Messenger {
    sender: mpsc::Sender<UnverifiedEthTxBatch>,
    receiver: mpsc::Receiver<UnverifiedEthTxBatch>,

    gossipsub_listen_handle: JoinHandle<()>,
}

impl Messenger {
    pub async fn new(config: MessengerConfig, wait_for_peers: u8) -> Result<Self, MessengerError> {
        let (gossipsub_listen_handle, sender, receiver, mut connected_rx) =
            gossipsub::start_gossipsub(
                config.local_key,
                config.address,
                config.port,
                config.bootstrap_peers,
                GOSSIP_SUB_DEFAULT_BUFFER_SIZE,
            )
            .map_err(|_| MessengerError::GossipSubStartError)?;

        if wait_for_peers != 0 {
            event! {tracing::Level::INFO, "Messenger started, waiting for {} peer(s)...", wait_for_peers};

            for _ in 0..wait_for_peers {
                connected_rx.recv().await;
            }
        }

        event! {tracing::Level::INFO, "Messenger connected to {} peer(s)" , wait_for_peers};

        Ok(Self {
            sender,
            receiver,

            gossipsub_listen_handle,
        })
    }

    pub fn get_sender(&self) -> mpsc::Sender<UnverifiedEthTxBatch> {
        self.sender.clone()
    }

    pub async fn recv(&mut self) -> Option<UnverifiedEthTxBatch> {
        self.receiver.recv().await
    }
}

impl Drop for Messenger {
    fn drop(&mut self) {
        self.gossipsub_listen_handle.abort();
    }
}
