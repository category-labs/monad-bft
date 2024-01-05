use std::{
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    task::{Poll, Waker},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::{Gossip, GossipEvent};
use monad_types::{Deserializable, NodeId, Serializable};
use quinn::Connecting;
use quinn_proto::ClientConfig;
use tokio::sync::mpsc::error::TrySendError;

use crate::{
    connection::{Connection, ConnectionEvent, ConnectionId, ConnectionWriter},
    quinn_config::QuinnConfig,
};

/// Service is an implementation of a RouterCommand updater that's backed by Quic
/// It can be parameterized by a Gossip algorithm
pub struct Service<QC, G, M, OM>
where
    G: Gossip,
    M: Message,
{
    /// An arbitrary starting time - used for computing internally consistent relative times,
    /// which are in std::time::Duration, for the Gossip trait
    zero_instant: Instant,
    /// The NodeId of self
    me: NodeId,
    /// known_addresses is used for knowing what address to dial to connect to a given PeerId.
    ///
    /// The dialed peer's certificate MUST be validated to ensure that it matches the expected
    /// PeerId.
    ///
    /// This might be replaced in the future once we support peer discovery
    known_addresses: HashMap<NodeId, SocketAddr>,

    /// The gossip implementation
    gossip: G,

    /// The main entrypoint into Quinn's API
    endpoint: quinn::Endpoint,
    /// Configuration generator used for Quinn initialization and connections
    quinn_config: QC,

    /// Future that yields on the next inbound connection attempt
    accept: BoxFuture<'static, Connecting>,

    /// Receiver channel for all connection events
    connection_events: tokio::sync::mpsc::Receiver<ConnectionEvent>,
    // Sender channel for connection events (cloned for each new connection)
    connection_events_sender: tokio::sync::mpsc::Sender<ConnectionEvent>,

    /// Each currently open canonical connection
    canonical_connections: HashMap<ConnectionId, CanonicalConnection>,

    /// Latest tie-broken connection for any given NodeId
    ///
    /// If the value is None, then the canonical connection is not yet ready
    /// This can happen if there's a pending outbound connection
    node_connections: HashMap<NodeId, Option<ConnectionId>>,

    /// Future that yields whenever the gossip implementation wants to be woken up
    gossip_timeout: Pin<Box<tokio::time::Sleep>>,
    waker: Option<Waker>,

    _pd: PhantomData<(M, OM)>,
}

struct CanonicalConnection {
    node_id: NodeId,
    /// Channel that can be used for writing bytes on the connection
    writer: tokio::sync::mpsc::Sender<Bytes>,
}

/// Inbound event buffer size for ALL connections
const CONNECTION_EVENTS_BUFFER_SIZE: usize = 1_000;
/// Outbound message buffer size for EACH connection
const CONNECTION_OUTBOUND_MESSAGE_BUFFER_SIZE: usize = 100;

/// Configuration for Service
pub struct ServiceConfig<QC> {
    /// The NodeId of self
    pub me: NodeId,

    /// The UDP address to bind the quic endpoint to
    pub server_address: SocketAddr,
    /// Quinn configuration
    pub quinn_config: QC,

    /// Lookup table for addresses of peers
    ///
    /// Currently, the entire validator set must be present here, because peer discovery is not
    /// supported
    pub known_addresses: HashMap<NodeId, SocketAddr>,
}

impl<QC, G, M, OM> Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message,
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

        let (connection_events_sender, connection_events) =
            tokio::sync::mpsc::channel(CONNECTION_EVENTS_BUFFER_SIZE);

        Self {
            zero_instant: Instant::now(),
            me: config.me,
            known_addresses: config.known_addresses,

            gossip,

            endpoint,
            quinn_config: config.quinn_config,

            accept,
            connection_events,
            connection_events_sender,
            canonical_connections: HashMap::new(),
            node_connections: HashMap::new(),

            gossip_timeout: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waker: None,

            _pd: PhantomData,
        }
    }
}

impl<QC, G, M, OM> Executor for Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<Bytes> + Send + Sync + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    OM: Into<M> + Clone,
{
    type Command = RouterCommand<OM>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            RouterCommand::Publish { .. } => false,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let time = self.zero_instant.elapsed();

        for command in commands {
            match command {
                RouterCommand::Publish { target, message } => {
                    let message = {
                        let mut _ser_span = tracing::info_span!("serialize_span").entered();
                        message.serialize()
                    };
                    let mut _publish_span =
                        tracing::info_span!("publish_span", message_len = message.len()).entered();
                    self.gossip.send(time, target, message);

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }
}

impl<QC, G, M, OM> Stream for Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<Bytes> + Send + Sync + 'static,
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
        let time = this.zero_instant.elapsed();

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

            if let Poll::Ready(connection_event) = this.connection_events.poll_recv(cx) {
                let connection_event =
                    connection_event.expect("connection_event stream should never be exhausted");
                match connection_event {
                    ConnectionEvent::ConnectionFailure(maybe_expected_peer, failure) => {
                        tracing::warn!("connection failure, failure={:?}", failure);
                        if let Some(expected_peer) = maybe_expected_peer {
                            let removed = this.node_connections.remove(&expected_peer);
                            assert_eq!(removed, Some(None));
                        }
                    }
                    ConnectionEvent::Connected(connection_id, node_id, writer) => {
                        assert_eq!(connection_id, writer.connection_id());
                        this.connected(time, node_id, writer);
                    }
                    ConnectionEvent::InboundMessage(from, gossip_messages) => {
                        if let Some(connection) = this.canonical_connections.get(&from) {
                            for gossip_message in gossip_messages {
                                this.gossip.handle_gossip_message(
                                    time,
                                    connection.node_id,
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
                        this.disconnected(time, &connection_id);
                    }
                }
                continue;
            }

            if let Some(timeout) = this.gossip.peek_tick() {
                let deadline = this.zero_instant + timeout;
                if deadline > this.gossip_timeout.deadline().into_std() {
                    tokio::time::Sleep::reset(this.gossip_timeout.as_mut(), deadline.into());
                }
                if this.gossip_timeout.poll_unpin(cx).is_ready() || timeout <= time {
                    if let Some(gossip_event) = this.gossip.poll(time) {
                        if let Some(event) = this.handle_gossip_event(time, gossip_event) {
                            return Poll::Ready(Some(event));
                        }
                    }
                    // loop if don't return value, because need to re-poll timeout
                    continue;
                }
            }

            return Poll::Pending;
        }
    }
}

impl<QC, G, M, OM> Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<Bytes> + Send + Sync + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    OM: Into<M> + Clone,
{
    /// Wires events from a new pending connection to self.connection_events
    fn connecting(&mut self, mut connection: Connection) {
        let connection_events_sender = self.connection_events_sender.clone();
        tokio::spawn(async move {
            while let Some(event) = connection.next().await {
                match connection_events_sender.try_send(event) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => todo!("Inbound_events channel full! Consider raising CONNECTION_EVENTS_BUFFER_SIZE={}?", CONNECTION_EVENTS_BUFFER_SIZE),
                    Err(TrySendError::Closed(_)) => break,
                }
            }
        });
    }

    /// Populates self.outbound_messages for a new established connection
    fn connected(
        &mut self,
        time: Duration,
        node_id: NodeId,
        mut connection_writer: ConnectionWriter,
    ) {
        let connection_id = connection_writer.connection_id();
        let (connection_sender, mut connection_reader) =
            tokio::sync::mpsc::channel(CONNECTION_OUTBOUND_MESSAGE_BUFFER_SIZE);

        if let Some(old_connection_id) = self.node_connections.get(&node_id).copied().flatten() {
            if self.me < node_id {
                // disconnect new connection
                self.disconnected(time, &connection_id);
                return;
            } else {
                // disconnect old connection
                self.disconnected(time, &old_connection_id)
            }
        }

        self.gossip.connected(time, node_id);
        self.canonical_connections.insert(
            connection_id,
            CanonicalConnection {
                node_id,
                writer: connection_sender,
            },
        );
        self.node_connections.insert(node_id, Some(connection_id));
        tokio::spawn(async move {
            while let Some(chunk) = connection_reader.recv().await {
                if let Err(failure) = connection_writer.write_chunk(chunk).await {
                    tracing::warn!("connection write failure, failure={:?}", failure);
                    break;
                }
            }
            // connection_writer gets dropped here, so quinn::Connection::close() will be called
        });
    }

    /// Cleans up self.outbound_messages for a connection that's shutting down
    /// Will cause quinn::Connection::close() to be called if it still exists
    fn disconnected(&mut self, time: Duration, connection_id: &ConnectionId) {
        if let Some(connection) = self.canonical_connections.remove(connection_id) {
            self.gossip.disconnected(time, connection.node_id);
            self.node_connections.remove(&connection.node_id);
        }
    }

    fn handle_gossip_event(
        &mut self,
        time: Duration,
        gossip_event: GossipEvent,
    ) -> Option<M::Event> {
        match gossip_event {
            GossipEvent::RequestConnect(to) => {
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
            GossipEvent::Send(to, gossip_message) => {
                let connection_id = self
                    .node_connections
                    .get(&to)
                    .copied()
                    .flatten()
                    .expect("must only emit Send once connected");
                let connection = self.canonical_connections.get(&connection_id).expect(
                    "invariant broken: node_connections connection_id not in canonical_connections",
                );
                match connection.writer.try_send(gossip_message) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        todo!("outbound message channel full! Consider raising CONNNECTION_OUTBOUND_MESSAGE_BUFFER_SIZE={}?", CONNECTION_OUTBOUND_MESSAGE_BUFFER_SIZE);
                    }
                    Err(TrySendError::Closed(_)) => {
                        // this implies that the connection died
                        self.disconnected(time, &connection_id)
                    }
                };
                None
            }
            GossipEvent::Emit(from, app_message) => {
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
