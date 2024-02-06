use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::DerefMut,
    task::{Poll, Waker},
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::Gossip;
use monad_types::{Deserializable, NodeId, Serializable};
use tokio::sync::mpsc::error::TrySendError;

use crate::{endpoint::SyncEndpoint, quinn_config::QuinnConfig};

/// Service is an implementation of a RouterCommand updater that's backed by Quic
/// It can be parameterized by a Gossip algorithm
pub struct Service<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey>,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
{
    me: NodeId<G::NodeIdPubKey>,

    poll_budget: u64,
    waker: Option<Waker>,

    endpoint: SyncEndpoint<QC, G, M, OM>,
    rx: tokio::sync::mpsc::Receiver<M::Event>,
}

/// Configuration for Service
pub struct ServiceConfig<QC: QuinnConfig> {
    /// The NodeId of self
    pub me: NodeId<QC::NodeIdPubKey>,

    /// The UDP address to bind the quic endpoint to
    pub server_address: SocketAddr,
    /// Quinn configuration
    pub quinn_config: QC,

    /// Lookup table for addresses of peers
    ///
    /// Currently, the entire validator set must be present here, because peer discovery is not
    /// supported
    pub known_addresses: HashMap<NodeId<QC::NodeIdPubKey>, SocketAddr>,
}

impl<QC, G, M, OM> Service<QC, G, M, OM>
where
    G: Gossip + Send + 'static,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey> + Send + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,
{
    pub fn new(config: ServiceConfig<QC>, gossip: G) -> Self {
        let me = config.me;
        let endpoint = SyncEndpoint::new(config, gossip);

        // TODO size this appropriately?
        const EVENT_BUFFER_SIZE: usize = 1_000;
        let (tx, rx) = tokio::sync::mpsc::channel(EVENT_BUFFER_SIZE);
        tokio::task::spawn({
            let mut endpoint = endpoint.clone();

            async move {
                while let Some(event) = endpoint.next().await {
                    let result = tx.try_send(event);
                    match result {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => todo!(
                            "consensus event loop not consuming fast enough, buf_size={}",
                            EVENT_BUFFER_SIZE
                        ),
                        Err(TrySendError::Closed(_)) => break,
                    }
                }
            }
        });
        Self {
            me,
            poll_budget: 0,
            waker: None,

            endpoint,
            rx,
        }
    }
    pub fn me(&self) -> NodeId<QC::NodeIdPubKey> {
        self.me
    }
}

impl<QC, G, M, OM> Executor for Service<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    OM: Into<M> + Clone,
{
    type Command = RouterCommand<G::NodeIdPubKey, OM>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            RouterCommand::Publish { .. } => false,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.endpoint.exec(commands);
    }
}

impl<QC, G, M, OM> Stream for Service<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    OM: Into<M> + Clone,

    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut _router_poll_span = tracing::info_span!("router_poll_span").entered();

        let this = self.deref_mut();
        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        this.poll_budget += 1;
        if this.poll_budget >= 16 {
            this.poll_budget = 0;
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
            return Poll::Pending;
        }

        this.rx.poll_recv(cx)
    }
}
