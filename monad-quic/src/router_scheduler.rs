use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_crypto::{rustls::TlsVerifier, secp256k1::KeyPair, NopSignature};
use monad_gossip::{ConnectionManager, ConnectionManagerEvent, Gossip, GossipEvent};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_types::{Deserializable, NodeId, RouterTarget, Serializable};
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, EndpointConfig, StreamId,
    TransportConfig, VarInt, WriteError,
};
use rand::{rngs::StdRng, RngCore, SeedableRng};

use crate::timeout_queue::TimeoutQueue;

pub struct QuicRouterSchedulerConfig<G: Gossip> {
    pub zero_instant: Instant,

    pub all_peers: BTreeSet<NodeId<G::NodeIdPubKey>>,
    pub me: NodeId<G::NodeIdPubKey>,

    pub master_seed: u64,

    pub gossip: G,
}

struct ConnectionState {
    connection: Connection,
    stream: Option<StreamId>,
    pending_outbound_messages: VecDeque<Bytes>,
}

impl ConnectionState {
    fn new(connection: Connection) -> Self {
        Self {
            connection,
            stream: None,
            pending_outbound_messages: VecDeque::new(),
        }
    }
}

pub struct QuicRouterScheduler<G: Gossip, IM, OM> {
    zero_instant: Instant,

    me: NodeId<G::NodeIdPubKey>,
    endpoint: quinn_proto::Endpoint,
    client_config: ClientConfig,
    connections: BTreeMap<ConnectionHandle, ConnectionState>,
    node_connections: HashMap<NodeId<G::NodeIdPubKey>, ConnectionHandle>,

    peer_ids: HashMap<NodeId<G::NodeIdPubKey>, SocketAddr>,
    addresses: HashMap<SocketAddr, NodeId<G::NodeIdPubKey>>,

    gossip: ConnectionManager<G>,

    timeouts: TimeoutQueue,

    pending_events: BTreeMap<Duration, VecDeque<RouterEvent<G::NodeIdPubKey, IM, Bytes>>>,

    phantom: PhantomData<OM>,
}

impl<G: Gossip, IM, OM> RouterScheduler for QuicRouterScheduler<G, IM, OM>
where
    IM: Deserializable<Bytes>,
    OM: Serializable<Bytes>,
{
    type NodeIdPublicKey = G::NodeIdPubKey;
    type Config = QuicRouterSchedulerConfig<G>;

    type InboundMessage = IM;
    type OutboundMessage = OM;
    type TransportMessage = Bytes;

    fn new(config: Self::Config) -> Self {
        let mut rng = StdRng::seed_from_u64(config.master_seed);
        let transport_config = {
            let mut config = TransportConfig::default();
            config.max_idle_timeout(None);
            // TODO-1 reasonable initial window sizes
            Arc::new(config)
        };

        // We can generate a random keypair here, because we don't need to verify the sources of
        // messages using the TLS baked into quic
        let scratch_keypair = {
            let mut seed = [0; 32];
            rng.fill_bytes(&mut seed);
            KeyPair::from_bytes(&mut seed).expect("valid keypair")
        };

        let mut seed = [0; 32];
        rng.fill_bytes(&mut seed);
        let endpoint = quinn_proto::Endpoint::new(
            Arc::new(EndpointConfig::default()),
            Some(Arc::new(
                {
                    let mut server_config = quinn_proto::ServerConfig::with_crypto(Arc::new(
                        // We can use NopSignature here, because we don't need to verify the sources of
                        // messages using the TLS baked into quic
                        TlsVerifier::<NopSignature>::make_server_config(&scratch_keypair),
                    ));
                    server_config.transport_config(transport_config.clone());
                    server_config
                }, // TODO use_retry ?
            )),
            false,
            Some(seed),
        );

        let peer_ids: HashMap<_, _> = config
            .all_peers
            .iter()
            .enumerate()
            .filter(|(_, peer)| peer != &&config.me)
            .map(|(idx, peer)| {
                (
                    *peer,
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3000 + idx as u16)),
                )
            })
            .collect();
        let addresses = peer_ids.iter().map(|(x, y)| (*y, *x)).collect();

        Self {
            zero_instant: config.zero_instant,

            me: config.me,
            endpoint,
            client_config: {
                let mut client_config = ClientConfig::new(Arc::new(
                    // We can use NopSignature here, because we don't need to verify the sources of
                    // messages using the TLS baked into quic
                    TlsVerifier::<NopSignature>::make_client_config(&scratch_keypair),
                ));
                client_config.transport_config(transport_config);
                client_config
            },
            connections: Default::default(),
            node_connections: Default::default(),

            peer_ids,
            addresses,

            gossip: ConnectionManager::new(config.gossip),

            timeouts: Default::default(),
            pending_events: Default::default(),

            phantom: PhantomData,
        }
    }

    fn process_inbound(
        &mut self,
        time: std::time::Duration,
        from: NodeId<G::NodeIdPubKey>,
        message: Self::TransportMessage,
    ) {
        assert_ne!(from, self.me);
        if let Some(maybe_event) = self.endpoint.handle(
            self.zero_instant + time,
            *self.peer_ids.get(&from).expect("peer_id should exist"),
            None,
            None, // TODO ecn codepoint?
            message.into_iter().collect(),
        ) {
            match maybe_event {
                DatagramEvent::ConnectionEvent(connection_handle, event) => {
                    let state = self
                        .connections
                        .get_mut(&connection_handle)
                        .expect("connection should exist");

                    state.connection.handle_event(event);

                    self.poll_connection(time, &connection_handle);
                    self.poll_gossip(time);
                }
                DatagramEvent::NewConnection(connection_handle, connection) => {
                    let replaced = self
                        .connections
                        .insert(connection_handle, ConnectionState::new(connection));
                    assert!(replaced.is_none());

                    self.poll_connection(time, &connection_handle);
                    self.poll_gossip(time);
                }
                DatagramEvent::Response(quinn_proto::Transmit {
                    destination,
                    ecn,
                    contents,
                    segment_size: _,
                    src_ip: _,
                }) => {
                    assert_eq!(ecn, None);
                    let to = *self
                        .addresses
                        .get(&destination)
                        .expect("address should exist");
                    assert!(
                        time >= self
                            .pending_events
                            .last_key_value()
                            .map(|(time, _)| *time)
                            .unwrap_or(Duration::ZERO)
                    );
                    self.pending_events
                        .entry(time)
                        .or_default()
                        .push_back(RouterEvent::Tx(to, contents))
                }
            };
        }
    }

    fn send_outbound(
        &mut self,
        time: Duration,
        to: RouterTarget<G::NodeIdPubKey>,
        message: Self::OutboundMessage,
    ) {
        self.gossip.send(time, to, message.serialize());
        self.poll_gossip(time);
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _)| tick)
    }

    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<RouterEvent<G::NodeIdPubKey, Self::InboundMessage, Self::TransportMessage>> {
        while let Some((min_tick, event_type)) = self.peek_event() {
            if min_tick > until {
                break;
            }

            match event_type {
                QuicEventType::Event => {
                    let mut entry = self.pending_events.first_entry().unwrap();
                    assert_eq!(min_tick, *entry.key());
                    let events = entry.get_mut();
                    let event = events.pop_front().expect("events should never be empty");

                    if events.is_empty() {
                        self.pending_events.pop_first().unwrap();
                    }
                    return Some(event);
                }
                QuicEventType::Timeout => {
                    let (timeout_tick, connection_handle) =
                        self.timeouts.pop().expect("invariant broken");
                    assert_eq!(min_tick, timeout_tick);

                    self.connections
                        .get_mut(&connection_handle)
                        .expect("connection should exist")
                        .connection
                        .handle_timeout(self.zero_instant + timeout_tick);
                    self.poll_connection(timeout_tick, &connection_handle);
                    self.poll_gossip(timeout_tick);
                }
                QuicEventType::GossipTimeout => {
                    let gossip_tick = self.gossip.peek_tick().expect("invariant broken");
                    assert_eq!(min_tick, gossip_tick);
                    self.poll_gossip(gossip_tick);
                }
            }
        }
        None
    }
}

enum QuicEventType {
    GossipTimeout,
    Timeout,
    Event,
}

impl<G: Gossip, IM, OM> QuicRouterScheduler<G, IM, OM>
where
    IM: Deserializable<Bytes>,
    OM: Serializable<Bytes>,
{
    fn peek_event(&self) -> Option<(Duration, QuicEventType)> {
        std::iter::empty()
            .chain(
                self.gossip
                    .peek_tick()
                    .map(|tick| (tick, QuicEventType::GossipTimeout)),
            )
            .chain(
                self.timeouts
                    .peek_tick()
                    .map(|tick| (tick, QuicEventType::Timeout)),
            )
            .chain(
                self.pending_events
                    .first_key_value()
                    .map(|(tick, _)| (*tick, QuicEventType::Event)),
            )
            .min_by_key(|(tick, _)| *tick)
    }

    fn canonical_connection(
        &self,
        node: &NodeId<G::NodeIdPubKey>,
    ) -> Option<(ConnectionHandle, StreamId)> {
        let connection_handle = self.node_connections.get(node)?;
        let state = self
            .connections
            .get(connection_handle)
            .expect("connection should exist");

        Some((*connection_handle, state.stream?))
    }

    /// should be followed up with a self.poll_gossip (because connection events may be generated)
    fn poll_connection(&mut self, time: Duration, connection_handle: &ConnectionHandle) {
        let state = self
            .connections
            .get(connection_handle)
            .expect("connection should exist");
        let connection = &state.connection;
        let remote_peer_id = self
            .addresses
            .get(&connection.remote_address())
            .expect("address should exist");

        let mut should_poll = true;
        while should_poll {
            should_poll = false;

            let canonical_connection = self.canonical_connection(remote_peer_id);
            let this_is_canonical_connection = canonical_connection.and_then(|(ch, stream_id)| {
                if &ch == connection_handle {
                    Some(stream_id)
                } else {
                    None
                }
            });

            // we remove the connection state from the map so that we can close other connections
            // we add it back to the map at the end of the loop
            let mut state = self
                .connections
                .remove(connection_handle)
                .expect("connection should exist");
            let connection = &mut state.connection;

            while let Some(transmit) = connection.poll_transmit(self.zero_instant + time, 1)
            // TODO what do we set MAX_DATAGRAMS to?
            {
                let to = *self
                    .addresses
                    .get(&transmit.destination)
                    .expect("address should exist");
                self.pending_events
                    .entry(time)
                    .or_default()
                    .push_back(RouterEvent::Tx(to, transmit.contents))
            }

            if let Some(timeout) = connection.poll_timeout() {
                self.timeouts
                    .insert((timeout - self.zero_instant).max(time), connection_handle)
            }

            while let Some(endpoint_event) = connection.poll_endpoint_events() {
                if let Some(connection_event) = self
                    .endpoint
                    .handle_event(*connection_handle, endpoint_event)
                {
                    connection.handle_event(connection_event);

                    should_poll = true;
                }
            }

            if let Some(stream_id) = this_is_canonical_connection {
                while let Some(mut gossip_message) = state.pending_outbound_messages.pop_front() {
                    match connection.send_stream(stream_id).write(&gossip_message) {
                        Ok(num_sent) => {
                            should_poll = true;
                            if num_sent < gossip_message.len() {
                                state
                                    .pending_outbound_messages
                                    .push_front(gossip_message.split_off(num_sent));
                                break;
                            }
                        }
                        Err(WriteError::Blocked) => {
                            state.pending_outbound_messages.push_front(gossip_message);
                            break;
                        }
                        Err(WriteError::Stopped(_) | WriteError::UnknownStream) => {
                            unreachable!("expect no write error")
                        }
                    }
                }

                let mut recv_stream = connection.recv_stream(stream_id);
                let mut chunks = recv_stream.read(true).expect("failed to read chunks");
                while let Ok(Some(chunk)) = chunks.next(
                    10_000, // TODO ?
                ) {
                    self.gossip
                        .handle_unframed_gossip_message(time, *remote_peer_id, chunk.bytes);
                }
                let _should_transmit = chunks.finalize();
            }

            while let Some(event) = connection.poll() {
                match event {
                    quinn_proto::Event::HandshakeDataReady => (),
                    quinn_proto::Event::Connected => {
                        if let Some((canonical_handle, _)) = canonical_connection {
                            assert_ne!(&canonical_handle, connection_handle);
                            // there's an existing connection for the desired node_id
                            // tiebreak based on NodeId
                            if &self.me < remote_peer_id {
                                // disconnect new connection
                                connection.close(
                                    self.zero_instant + time,
                                    VarInt::from_u32(0),
                                    "existing connection".into(),
                                );
                                should_poll = true;
                                break;
                            } else {
                                // disconnect old connection
                                let old_state = self
                                    .connections
                                    .get_mut(&canonical_handle)
                                    .expect("connection should exist");
                                old_state.connection.close(
                                    self.zero_instant + time,
                                    VarInt::from_u32(0),
                                    "new connection replacing this one".into(),
                                );
                                self.timeouts.insert(time, &canonical_handle);
                                self.gossip.disconnected(time, *remote_peer_id);
                            }
                        }
                        self.node_connections
                            .insert(*remote_peer_id, *connection_handle);
                        if connection.side().is_client() {
                            let stream_id = connection
                                .streams()
                                .open(Dir::Bi)
                                .expect("creating stream_id failed");

                            state.stream = Some(stream_id);
                            // TODO if duplicate, dc one of them
                            self.gossip.connected(time, *remote_peer_id);
                            should_poll = true;
                            break;
                        }
                    }
                    quinn_proto::Event::ConnectionLost { reason } => {
                        if this_is_canonical_connection.is_some() {
                            self.gossip.disconnected(time, *remote_peer_id);
                        }
                        self.node_connections.remove(remote_peer_id);
                    }
                    quinn_proto::Event::Stream(stream_event) => match stream_event {
                        quinn_proto::StreamEvent::Opened { dir } => {
                            let stream_id = connection
                                .streams()
                                .accept(dir)
                                .expect("stream id should exist");
                            assert!(state.stream.is_none(), "only one bidi stream supported");
                            state.stream = Some(stream_id);

                            self.gossip.connected(time, *remote_peer_id);
                            should_poll = true;
                            break;
                        }
                        quinn_proto::StreamEvent::Readable { id } => should_poll = true,
                        quinn_proto::StreamEvent::Writable { id } => should_poll = true,
                        quinn_proto::StreamEvent::Finished { id } => todo!(),
                        quinn_proto::StreamEvent::Stopped { id, error_code } => todo!(),
                        quinn_proto::StreamEvent::Available { dir } => todo!(),
                    },
                    quinn_proto::Event::DatagramReceived => todo!(),
                }
            }
            if connection.is_drained() {
                assert_ne!(
                    self.node_connections.get(remote_peer_id),
                    Some(connection_handle)
                );
                assert!(!self.connections.contains_key(connection_handle));
                self.timeouts.remove(connection_handle);
                break;
            } else {
                self.connections.insert(*connection_handle, state);
            }
        }
    }

    fn poll_gossip(&mut self, time: Duration) {
        while let Some(event) = self.gossip.poll(time) {
            match event {
                ConnectionManagerEvent::RequestConnect(to) => {
                    if self.node_connections.contains_key(&to) {
                        // we already have an open connection, so we can ignore
                        continue;
                    }
                    let sock_addr = self
                        .peer_ids
                        .get(&to)
                        .expect("peer address should be known");
                    let (connection_handle, connection) = self
                        .endpoint
                        .connect(
                            self.zero_instant + time,
                            self.client_config.clone(),
                            *sock_addr,
                            "MONAD",
                        )
                        .expect("mock quic should never fail to connect");
                    self.connections
                        .insert(connection_handle, ConnectionState::new(connection));
                    self.node_connections.insert(to, connection_handle);
                    self.poll_connection(time, &connection_handle);
                }
                ConnectionManagerEvent::GossipEvent(GossipEvent::Send(to, gossip_message)) => {
                    let connection_handle = *self
                        .node_connections
                        .get(&to)
                        .expect("must only emit Send once connected");
                    if let Some(connection) = self.connections.get_mut(&connection_handle) {
                        connection
                            .pending_outbound_messages
                            .extend(gossip_message.into_inner());
                        self.poll_connection(time, &connection_handle);
                    }
                }
                ConnectionManagerEvent::GossipEvent(GossipEvent::Emit(peer_id, message)) => {
                    assert!(
                        time >= self
                            .pending_events
                            .last_key_value()
                            .map(|(time, _)| *time)
                            .unwrap_or(Duration::ZERO)
                    );

                    match IM::deserialize(&message) {
                        Ok(message) => self
                            .pending_events
                            .entry(time)
                            .or_default()
                            .push_back(RouterEvent::Rx(peer_id, message)),

                        Err(e) => {
                            todo!("Message deserialization should never fail! Error: {:?}", e)
                        }
                    }
                }
            }
        }
    }
}
