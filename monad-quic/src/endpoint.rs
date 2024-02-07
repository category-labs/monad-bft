use std::{
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::{
    future::BoxFuture,
    stream::{SelectAll, StreamExt},
    FutureExt, Stream,
};
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::{ConnectionManager, ConnectionManagerEvent, Gossip, GossipEvent};
use monad_types::{Deserializable, NodeId, Serializable};
use quinn::Connecting;
use quinn_proto::ClientConfig;

use crate::{
    connection::{Connection, ConnectionEvent, ConnectionId, ConnectionWriter},
    QuinnConfig, ServiceConfig,
};

const ZERO_INSTANT: SystemTime = UNIX_EPOCH;

pub(crate) struct SyncEndpoint<QC, G, M, OM>(Arc<Mutex<Endpoint<QC, G, M, OM>>>)
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey>,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>;

impl<G, QC, M, OM> Clone for SyncEndpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey>,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<QC, G, M, OM> SyncEndpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey>,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
{
    pub fn new(config: ServiceConfig<QC>, gossip: G) -> Self {
        Self(Arc::new(Mutex::new(Endpoint::new(config, gossip))))
    }
}

impl<QC, G, M, OM> SyncEndpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,
{
    pub fn exec(&mut self, commands: Vec<RouterCommand<G::NodeIdPubKey, OM>>) {
        self.0.lock().unwrap().exec(commands)
    }
}

pub(crate) struct Endpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey>,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
{
    /// Time since ZERO_INSTANT (duration since epoch start)
    current_time: Duration,

    /// The NodeId of self
    me: NodeId<G::NodeIdPubKey>,
    /// known_addresses is used for knowing what address to dial to connect to a given PeerId.
    ///
    /// The dialed peer's certificate MUST be validated to ensure that it matches the expected
    /// PeerId.
    ///
    /// This might be replaced in the future once we support peer discovery
    known_addresses: HashMap<NodeId<G::NodeIdPubKey>, SocketAddr>,

    /// The gossip implementation
    gossip: ConnectionManager<G>,

    /// The main entrypoint into Quinn's API
    endpoint: quinn::Endpoint,
    /// Configuration generator used for Quinn initialization and connections
    quinn_config: QC,

    /// Future that yields on the next inbound connection attempt
    accept: BoxFuture<'static, Connecting>,

    /// Future that yields whenever the gossip implementation wants to be woken up
    gossip_timeout: Pin<Box<tokio::time::Sleep>>,
    waker: Option<Waker>,

    connections: SelectAll<Connection<G::NodeIdPubKey>>,

    /// Each currently open canonical connection
    canonical_connections: HashMap<ConnectionId, CanonicalConnection<G::NodeIdPubKey>>,
    /// Latest tie-broken connection for any given NodeId
    ///
    /// If the value is None, then the canonical connection is not yet ready
    /// This can happen if there's a pending outbound connection
    node_connections: HashMap<NodeId<G::NodeIdPubKey>, Option<ConnectionId>>,

    _pd: PhantomData<(M, OM)>,
}

struct CanonicalConnection<PT: PubKey> {
    node_id: NodeId<PT>,
    writer: ConnectionWriter,
}

impl<QC, G, M, OM> Endpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey>,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
{
    pub fn new(config: ServiceConfig<QC>, gossip: G) -> Self {
        let mut server_config = quinn::ServerConfig::with_crypto(config.quinn_config.server());
        server_config.transport_config(config.quinn_config.transport());
        let endpoint = quinn::Endpoint::server(server_config, config.server_address)
            .unwrap_or_else(|_| {
                panic!(
                    "Endpoint initialization shouldn't fail: {:?}",
                    config.server_address
                )
            });

        let accept = {
            let endpoint = endpoint.clone();
            #[allow(clippy::async_yields_async)]
            async move { endpoint.accept().await.expect("endpoint is never closed") }.boxed()
        };

        Self {
            current_time: ZERO_INSTANT.elapsed().unwrap(),

            me: config.me,
            known_addresses: config.known_addresses,

            gossip: ConnectionManager::new(gossip),

            endpoint,
            quinn_config: config.quinn_config,

            accept,

            gossip_timeout: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waker: None,

            connections: SelectAll::new(),
            canonical_connections: Default::default(),
            node_connections: Default::default(),

            _pd: PhantomData,
        }
    }

    fn update_current_time(&mut self) {
        // enforce monotonicity of SystemTime
        self.current_time = self.current_time.max(ZERO_INSTANT.elapsed().unwrap());
    }
}

impl<QC, G, M, OM> Endpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,
{
    fn exec(&mut self, commands: Vec<RouterCommand<G::NodeIdPubKey, OM>>) {
        self.update_current_time();

        for command in commands {
            match command {
                RouterCommand::Publish { target, message } => {
                    let message = {
                        let mut _ser_span = tracing::info_span!("serialize_span").entered();
                        message.serialize()
                    };
                    let mut _publish_span =
                        tracing::info_span!("publish_span", message_len = message.len()).entered();
                    self.gossip.send(self.current_time, target, message);

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }

    fn connecting(&mut self, connection: Connection<G::NodeIdPubKey>) {
        self.connections.push(connection);
    }

    /// Populates self.outbound_messages for a new established connection
    fn connected(&mut self, node_id: NodeId<G::NodeIdPubKey>, writer: ConnectionWriter) {
        let connection_id = writer.connection_id();

        if let Some(old_connection_id) = self.node_connections.get(&node_id).copied().flatten() {
            if self.me < node_id {
                // disconnect new connection
                self.disconnected(&connection_id);
                return;
            } else {
                // disconnect old connection
                self.disconnected(&old_connection_id)
            }
        }

        self.gossip.connected(self.current_time, node_id);
        self.canonical_connections
            .insert(connection_id, CanonicalConnection { node_id, writer });
        self.node_connections.insert(node_id, Some(connection_id));
    }

    /// Cleans up self.outbound_messages for a connection that's shutting down
    /// Will cause quinn::Connection::close() to be called if it still exists
    fn disconnected(&mut self, connection_id: &ConnectionId) {
        if let Some(connection) = self.canonical_connections.remove(connection_id) {
            self.gossip
                .disconnected(self.current_time, connection.node_id);
            self.node_connections.remove(&connection.node_id);
        }
    }

    fn handle_gossip_event(
        &mut self,
        gossip_event: ConnectionManagerEvent<G::NodeIdPubKey>,
    ) -> Option<M::Event> {
        match gossip_event {
            ConnectionManagerEvent::RequestConnect(to) => {
                if let Entry::Vacant(e) = self.node_connections.entry(to) {
                    let known_address = match self.known_addresses.get(&to) {
                        Some(address) => *address,
                        None => todo!("Peer discovery unsupported, address unknown for: {:?}", to),
                    };
                    let client_config = {
                        let mut c = ClientConfig::new(self.quinn_config.client());
                        c.transport_config(self.quinn_config.transport());
                        c
                    };
                    let connection = match self.endpoint.connect_with(
                        client_config,
                        known_address,
                        "MONAD", // server_name doesn't matter because we're verifying the peer_id that signed the certificate
                    ) {
                        Ok(connecting) => Connection::outbound::<QC>(connecting, to),
                        Err(err) => {
                            todo!("Unexpected connection error: {:?}", err)
                        }
                    };
                    e.insert(None);
                    self.connecting(connection);
                } else {
                    // (connection|connection_attempt) already exists
                }
                None
            }
            ConnectionManagerEvent::GossipEvent(GossipEvent::Send(to, gossip_message)) => {
                let connection_id = self
                    .node_connections
                    .get(&to)
                    .copied()
                    .flatten()
                    .expect("must only emit Send once connected");
                let connection = self.canonical_connections.get_mut(&connection_id).expect(
                    "invariant broken: node_connections connection_id not in canonical_connections",
                );
                let mut gossip_message: Vec<_> = gossip_message.into_inner().into_iter().collect();
                let result = connection
                    .writer
                    .write_chunks(&mut gossip_message)
                    .now_or_never()?;
                match result {
                    Ok(num_chunks) if num_chunks == gossip_message.len() => {}
                    Ok(_) => {
                        todo!("wrote partial chunks, replace with try_write_chunks");
                    }
                    Err(failure) => {
                        tracing::warn!("disconnecting, connection failure: {:?}", failure);
                        // this implies that the connection died
                        self.disconnected(&connection_id)
                    }
                }
                None
            }
            ConnectionManagerEvent::GossipEvent(GossipEvent::Emit(from, app_message)) => {
                let message = {
                    let mut _deser_span =
                        tracing::info_span!("deserialize_span", message_len = app_message.len())
                            .entered();
                    match M::deserialize(&app_message) {
                        Ok(m) => m,
                        Err(e) => todo!("err deserializing message: {:?}", e),
                    }
                };
                let event = {
                    let mut _message_to_event_span = tracing::info_span!(
                        "message_to_event_span",
                        message_len = app_message.len()
                    )
                    .entered();
                    message.event(from)
                };
                Some(event)
            }
        }
    }
}

impl<QC, G, M, OM> Stream for SyncEndpoint<QC, G, M, OM>
where
    G: Gossip,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    QC: QuinnConfig<NodeIdPubKey = G::NodeIdPubKey>,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.0.lock().unwrap();
        let mut _router_poll_span = tracing::info_span!("router_poll_span").entered();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }
        this.update_current_time();
        let current_time = this.current_time;

        loop {
            if let Poll::Ready(connecting) = this.accept.poll_unpin(cx) {
                let endpoint = this.endpoint.clone();
                this.accept = {
                    #[allow(clippy::async_yields_async)]
                    async move { endpoint.accept().await.expect("endpoint is never closed") }
                        .boxed()
                };

                this.connecting(Connection::inbound::<QC>(connecting));
                continue;
            }

            if let Some(timeout) = this.gossip.peek_tick() {
                // unit of timeout is duration since unix epoch
                let duration_until_timeout =
                    timeout.checked_sub(current_time).unwrap_or(Duration::ZERO);
                let current_instant = Instant::now();

                let deadline = current_instant + duration_until_timeout;
                if deadline > this.gossip_timeout.deadline().into_std() {
                    tokio::time::Sleep::reset(this.gossip_timeout.as_mut(), deadline.into());
                }
                if this.gossip_timeout.poll_unpin(cx).is_ready()
                    || duration_until_timeout == Duration::ZERO
                {
                    if let Some(gossip_event) = this.gossip.poll(current_time) {
                        if let Some(event) = this.handle_gossip_event(gossip_event) {
                            return Poll::Ready(Some(event));
                        }
                    }
                    // loop if don't return value, because need to re-poll timeout
                    continue;
                }
            }

            if !this.connections.is_empty() {
                if let Poll::Ready(connection_event) = this.connections.poll_next_unpin(cx) {
                    let connection_event = connection_event
                        .expect("connection_event stream should never be exhausted");
                    match connection_event {
                        ConnectionEvent::ConnectionFailure(maybe_expected_peer, failure) => {
                            tracing::warn!("connection failure, failure={:?}", failure);
                            if let Some(expected_peer) = maybe_expected_peer {
                                if let Some(None) = this.node_connections.get(&expected_peer) {
                                    this.node_connections.remove(&expected_peer);
                                } else {
                                    // don't need to do anything, because established conn is still
                                    // alive
                                }
                            }
                        }
                        ConnectionEvent::Connected(connection_id, node_id, writer) => {
                            assert_eq!(connection_id, writer.connection_id());
                            this.connected(node_id, writer);
                        }
                        ConnectionEvent::InboundMessage(from, gossip_messages) => {
                            if let Some(connection) = this.canonical_connections.get(&from) {
                                let node_id = connection.node_id;
                                for gossip_message in gossip_messages {
                                    this.gossip.handle_unframed_gossip_message(
                                        current_time,
                                        node_id,
                                        gossip_message,
                                    );
                                }
                            }
                        }
                        ConnectionEvent::Disconnected(connection_id, failure) => {
                            tracing::warn!(
                                "connection disconnected, id={:?}, failure={:?}",
                                connection_id,
                                failure
                            );
                            this.disconnected(&connection_id);
                        }
                    }
                    continue;
                }
            }

            return Poll::Pending;
        }
    }
}
