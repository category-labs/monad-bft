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

use std::{marker::PhantomData, net::SocketAddr, ops::DerefMut, path::PathBuf, task::Poll};

use futures::Stream;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::Executor;
use monad_executor_glue::{
    ConfigEvent, ConfigReloadCommand, ConfigUpdate, KnownPeersUpdate, MonadEvent, PeerEntry,
};
use monad_node_config::{NodeBootstrapPeerConfig, NodeConfig};
use monad_types::{ExecutionProtocol, NodeId};
use monad_validator::signature_collection::SignatureCollection;
use tokio::{
    net::lookup_host,
    sync::mpsc::{error::TrySendError, Receiver, Sender},
};
use tracing::{error, warn};
pub struct MockConfigLoader<ST, SCT, EPT> {
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> Default for MockConfigLoader<ST, SCT, EPT> {
    fn default() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}

impl<ST, SCT, EPT> Stream for MockConfigLoader<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl<ST, SCT, EPT> Executor for MockConfigLoader<ST, SCT, EPT>
where
    SCT: SignatureCollection,
{
    type Command = ConfigReloadCommand;

    fn exec(&mut self, _commands: Vec<Self::Command>) {}

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain {
        Default::default()
    }
}

pub struct ConfigLoader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    config_path: PathBuf,

    request_tx: Sender<(
        Vec<NodeBootstrapPeerConfig<ST>>,
        Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    )>,
    response_tx: Sender<ConfigEvent<ST, SCT>>,
    response_rx: Receiver<ConfigEvent<ST, SCT>>,

    _phantom: PhantomData<EPT>,
}

impl<ST, SCT, EPT> ConfigLoader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(config_path: PathBuf) -> Self {
        let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(10);
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(10);

        let bootstrap_peers_refresh_tx = response_tx.clone();
        tokio::spawn(async move {
            while let Some((bootstrap_peers, pinned_nodes)) = request_rx.recv().await {
                let known_peers = Self::resolve_domains(bootstrap_peers).await;
                let config_event = ConfigEvent::KnownPeersUpdate(KnownPeersUpdate {
                    known_peers,
                    pinned_nodes,
                });

                match bootstrap_peers_refresh_tx.try_send(config_event) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        warn!("config update response channel closed");
                    }
                    Err(TrySendError::Full(_)) => {
                        warn!("config update response channel full");
                    }
                };
            }

            error!("config request sending side closed");
        });

        Self {
            config_path,

            request_tx,
            response_tx,
            response_rx,

            _phantom: PhantomData,
        }
    }

    fn extract_config_update(&mut self, node_config: NodeConfig<ST>) -> ConfigEvent<ST, SCT> {
        let dedicated_full_nodes: Vec<_> = node_config
            .fullnode_dedicated
            .identities
            .iter()
            .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
            .collect();

        let prioritized_full_nodes: Vec<_> = node_config
            .fullnode_raptorcast
            .full_nodes_prioritized
            .identities
            .iter()
            .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
            .collect();

        let blocksync_override_peers = node_config
            .blocksync_override
            .peers
            .into_iter()
            .map(|p| NodeId::new(p.secp256k1_pubkey))
            .collect();

        let pinned_full_nodes = dedicated_full_nodes
            .iter()
            .cloned()
            .chain(prioritized_full_nodes.iter().cloned())
            .collect();

        match self
            .request_tx
            .try_send((node_config.bootstrap.peers, pinned_full_nodes))
        {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {
                warn!("config request channel closed");
            }
            Err(TrySendError::Full(_)) => {
                warn!("config request channel full");
            }
        };

        ConfigEvent::ConfigUpdate(ConfigUpdate {
            dedicated_full_nodes,
            prioritized_full_nodes,
            prioritized_upstream: node_config.fullnode_raptorcast.prioritized_upstream,
            blocksync_override_peers,
        })
    }

    fn reload(&mut self) {
        let config_event = match std::fs::read_to_string(&self.config_path) {
            Ok(config_string) => match toml::from_str::<NodeConfig<ST>>(&config_string) {
                Ok(node_config) => self.extract_config_update(node_config),
                Err(err) => ConfigEvent::LoadError(err.to_string()),
            },
            Err(err) => ConfigEvent::LoadError(err.to_string()),
        };
        match self.response_tx.try_send(config_event) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {
                warn!("config update response channel closed");
            }
            Err(TrySendError::Full(_)) => {
                warn!("config update response channel full");
            }
        }
    }

    async fn resolve_domains(
        bootstrap_peers: Vec<NodeBootstrapPeerConfig<ST>>,
    ) -> Vec<PeerEntry<ST>> {
        let mut peer_entries = Vec::new();
        for peer in bootstrap_peers {
            let addr = match resolve_domain_v4(&peer.address).await {
                Ok(Some(SocketAddr::V4(addr))) => addr,
                _ => {
                    warn!("config loader: cannot resolve: {:?}", &peer.address);
                    continue;
                }
            };
            peer_entries.push(PeerEntry {
                pubkey: peer.secp256k1_pubkey,
                addr,
                signature: peer.name_record_sig,
                record_seq_num: peer.record_seq_num,
            });
        }
        peer_entries
    }
}

impl<ST, SCT, EPT> Stream for ConfigLoader<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        this.response_rx.poll_recv(cx).map(|maybe_event| {
            maybe_event.map(|config_event| MonadEvent::ConfigEvent(config_event))
        })
    }
}

impl<ST, SCT, EPT> Executor for ConfigLoader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = ConfigReloadCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ConfigReloadCommand::ReloadConfig => {
                    self.reload();
                }
            }
        }
    }

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain {
        Default::default()
    }
}

async fn resolve_domain_v4(domain: &String) -> Result<Option<SocketAddr>, std::io::Error> {
    let dns_response = lookup_host(domain).await?;

    for entry in dns_response {
        match entry {
            SocketAddr::V4(_) => return Ok(Some(entry)),
            SocketAddr::V6(_) => continue,
        }
    }

    Ok(None)
}
