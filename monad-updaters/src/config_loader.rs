use std::{
    collections::BTreeMap, marker::PhantomData, net::SocketAddr, ops::DerefMut, path::PathBuf,
    task::Poll, time::Duration,
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{
    ConfigEvent, ConfigReloadCommand, ConfigUpdate, KnownPeersUpdate, MonadEvent,
};
use monad_node_config::{NodeBootstrapPeerConfig, NodeConfig};
use monad_types::{DropTimer, ExecutionProtocol, NodeId};
use tokio::{
    net::lookup_host,
    sync::mpsc::{error::TrySendError, Receiver, Sender},
};
use tracing::{debug, info, warn};
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
    SCT: SignatureCollection,
{
    config_path: PathBuf,

    request_tx: Sender<Vec<NodeBootstrapPeerConfig<SCT::NodeIdPubKey>>>,
    response_tx: Sender<ConfigEvent<SCT>>,
    response_rx: Receiver<ConfigEvent<SCT>>,

    _phantom: PhantomData<(ST, EPT)>,
}

impl<ST, SCT, EPT> ConfigLoader<ST, SCT, EPT>
where
    SCT: SignatureCollection,
{
    pub fn new(
        config_path: PathBuf,
        bootstrap_peers: Vec<NodeBootstrapPeerConfig<SCT::NodeIdPubKey>>,
        last_resolved: Vec<(NodeId<SCT::NodeIdPubKey>, SocketAddr)>,
    ) -> Self {
        let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(10);
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(10);
        let mut peers_table = PeersTable::new(bootstrap_peers, last_resolved);

        let dns_refresh_tx = response_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // first tick completes immediately
            interval.tick().await;

            let try_send_update =
                |config_event: ConfigEvent<SCT>| match dns_refresh_tx.try_send(config_event) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        warn!("config update response channel closed");
                    }
                    Err(TrySendError::Full(_)) => {
                        warn!("config update response channel full");
                    }
                };

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Some(update) = peers_table.refresh_dns().await {
                            try_send_update(update);
                        }
                    }
                    maybe_req = request_rx.recv() => {
                        interval.reset();
                        let Some(req): Option<Vec<NodeBootstrapPeerConfig<SCT::NodeIdPubKey>>> = maybe_req else {
                            panic!("config request sending side closed, exiting..");
                        };
                        let new_peers = req.into_iter().map(|peer| (NodeId::new(peer.secp256k1_pubkey), peer.address)).collect::<Vec<_>>();
                        peers_table.set_peers(new_peers);
                        if let Some(update) = peers_table.refresh_dns().await {
                            try_send_update(update);
                        }
                    }
                }
            }
        });

        Self {
            config_path,

            request_tx,
            response_tx,
            response_rx,

            _phantom: PhantomData,
        }
    }

    fn extract_config_update(
        &mut self,
        node_config: NodeConfig<SCT::NodeIdPubKey>,
    ) -> ConfigEvent<SCT> {
        let full_nodes = node_config
            .fullnode
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

        match self.request_tx.try_send(node_config.bootstrap.peers) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {
                warn!("config request channel closed");
            }
            Err(TrySendError::Full(_)) => {
                warn!("config request channel full");
            }
        };

        ConfigEvent::ConfigUpdate(ConfigUpdate {
            full_nodes,
            blocksync_override_peers,
        })
    }

    fn reload(&mut self) {
        let config_event = match std::fs::read_to_string(&self.config_path) {
            Ok(config_string) => {
                match toml::from_str::<NodeConfig<SCT::NodeIdPubKey>>(&config_string) {
                    Ok(node_config) => self.extract_config_update(node_config),
                    Err(err) => ConfigEvent::LoadError(err.to_string()),
                }
            }
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
    SCT: SignatureCollection,
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

struct PeersTable<SCT: SignatureCollection>(
    BTreeMap<NodeId<SCT::NodeIdPubKey>, (String, Option<SocketAddr>)>,
);

impl<SCT: SignatureCollection> PeersTable<SCT> {
    fn new(
        bootstrap_peers: Vec<NodeBootstrapPeerConfig<SCT::NodeIdPubKey>>,
        last_resolved: Vec<(NodeId<SCT::NodeIdPubKey>, SocketAddr)>,
    ) -> Self {
        assert_eq!(bootstrap_peers.len(), last_resolved.len());
        let resolved_map: BTreeMap<NodeId<SCT::NodeIdPubKey>, SocketAddr> =
            last_resolved.into_iter().collect();
        let mut peers_table = BTreeMap::new();
        for peer in bootstrap_peers {
            let node_id = NodeId::new(peer.secp256k1_pubkey);
            let addr = resolved_map
                .get(&node_id)
                .expect("all peers are resolved on startup");
            peers_table.insert(node_id, (peer.address, Some(*addr)));
        }
        Self(peers_table)
    }

    async fn resolve_domain(domain: &String) -> Result<Option<SocketAddr>, std::io::Error> {
        let mut dns_response = lookup_host(domain).await?;
        Ok(dns_response.next())
    }

    async fn resolve_domains<P: PubKey>(
        peers: Vec<(NodeId<P>, &String)>,
    ) -> Vec<(NodeId<P>, Option<SocketAddr>)> {
        let mut resolved_peers = Vec::with_capacity(peers.len());
        for (node_id, address) in peers {
            let maybe_addr = match Self::resolve_domain(address).await {
                Ok(Some(addr)) => Some(addr),
                Ok(None) => {
                    info!(?node_id, domain=?address, "No DNS record");
                    None
                }
                Err(e) => {
                    info!(?node_id, domain=?address, "Failed to resolve: {}", e);
                    None
                }
            };
            resolved_peers.push((node_id, maybe_addr));
        }
        resolved_peers
    }

    async fn refresh_dns(&mut self) -> Option<ConfigEvent<SCT>> {
        info!("refreshing dns record");
        let _drop_timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, "refreshing dns record done")
        });
        // refresh dns for peer table
        let mut should_update = false;
        let peers = self
            .0
            .iter()
            .map(|(node_id, (hostname, _))| (*node_id, hostname))
            .collect::<Vec<_>>();
        let resolved = Self::resolve_domains(peers).await;
        for (node_id, maybe_addr) in resolved {
            // retain old record if hostname fails to resolve
            if let Some(new_addr) = maybe_addr {
                let (_, maybe_last_resolved) = self
                    .0
                    .get_mut(&node_id)
                    .expect("peers table is immutable during lookup");
                if maybe_last_resolved.is_none()
                    || maybe_last_resolved.is_some_and(|last_resolved| last_resolved != new_addr)
                {
                    info!(?node_id, old_addr=?maybe_last_resolved, ?new_addr, "update dns record");
                    should_update = true;
                    *maybe_last_resolved = Some(new_addr);
                }
            }
        }
        if should_update {
            let response = self
                .0
                .iter()
                .filter_map(|(&node_id, (_, maybe_addr))| maybe_addr.map(|addr| (node_id, addr)))
                .collect::<Vec<_>>();

            return Some(ConfigEvent::KnownPeersUpdate(KnownPeersUpdate {
                known_peers: response,
            }));
        }
        None
    }

    fn set_peers(&mut self, new_peers: Vec<(NodeId<SCT::NodeIdPubKey>, String)>) {
        for (node_id, hostname) in new_peers {
            self.0
                .entry(node_id)
                .and_modify(|(old_hostname, maybe_resolved)| {
                    if old_hostname != &hostname {
                        debug!(?node_id, ?old_hostname, new_hostname=?hostname, "hostname changed");
                        *old_hostname = hostname.clone();
                        *maybe_resolved = None;
                    }
                })
                .or_insert((hostname, None));
        }
    }
}
