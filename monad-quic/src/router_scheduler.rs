use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::{Duration, Instant},
};

use monad_crypto::rustls::UnsafeTlsVerifier;
use monad_executor_glue::{PeerId, RouterTarget};
use monad_gossip::{Gossip, GossipEvent};
use monad_mock_swarm::mock::{RouterEvent, RouterScheduler};
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, EndpointConfig, StreamId,
    TransportConfig,
};

use crate::timeout_queue::TimeoutQueue;

pub struct QuicRouterSchedulerConfig<GC> {
    pub zero_instant: Instant,

    pub all_peers: BTreeSet<PeerId>,
    pub me: PeerId,

    pub gossip_config: GC,
}

pub struct QuicRouterScheduler<G: Gossip> {
    zero_instant: Instant,

    me: PeerId,
    endpoint: quinn_proto::Endpoint,
    connections: HashMap<ConnectionHandle, Connection>,
    outbound_connections: HashMap<PeerId, (ConnectionHandle, Option<StreamId>)>,

    peer_ids: HashMap<PeerId, SocketAddr>,
    addresses: HashMap<SocketAddr, PeerId>,

    gossip: G,

    timeouts: TimeoutQueue,

    pending_events: BTreeMap<Duration, Vec<RouterEvent<Vec<u8>, Vec<u8>>>>,
    pending_outbound_messages: HashMap<PeerId, VecDeque<Vec<u8>>>,
}

const SERVER_NAME: &str = "MONAD";

impl<G: Gossip> RouterScheduler for QuicRouterScheduler<G> {
    type Config = QuicRouterSchedulerConfig<G::Config>;
    type M = Vec<u8>;
    type Serialized = Vec<u8>;

    fn new(config: Self::Config) -> Self {
        let mut endpoint = quinn_proto::Endpoint::new(
            Arc::new(EndpointConfig::default()),
            Some(Arc::new(
                quinn_proto::ServerConfig::with_crypto(Arc::new(
                    UnsafeTlsVerifier::make_server_config(Vec::new()),
                )), // TODO use_retry ?
            )),
            false,
        );

        let mut connections = HashMap::new();
        let mut outbound_connections = HashMap::new();
        let mut peer_ids = HashMap::new();
        for (idx, peer) in config
            .all_peers
            .iter()
            .enumerate()
            .filter(|(_, peer)| peer != &&config.me)
        {
            let sock_addr =
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3000 + idx as u16));
            peer_ids.insert(*peer, sock_addr);

            let mut client_config =
                ClientConfig::new(Arc::new(UnsafeTlsVerifier::make_client_config(Vec::new())));
            client_config.transport_config(Arc::new(
                TransportConfig::default(), // TODO ?
            ));

            let (connection_handle, connection) = endpoint
                .connect(config.zero_instant, client_config, sock_addr, SERVER_NAME)
                .expect("mock quic should never fail to connect");
            connections.insert(connection_handle, connection);
            outbound_connections.insert(*peer, (connection_handle, None));
        }
        let addresses = peer_ids.iter().map(|(x, y)| (*y, *x)).collect();

        let mut scheduler = Self {
            zero_instant: config.zero_instant,

            me: config.me,
            endpoint,
            connections,
            outbound_connections,

            peer_ids,
            addresses,

            gossip: G::new(config.gossip_config),

            timeouts: Default::default(),
            pending_events: Default::default(),
            pending_outbound_messages: HashMap::new(),
        };

        let mut handles: Vec<_> = scheduler.connections.keys().cloned().collect();
        handles.sort();
        for handle in &handles {
            scheduler.poll_connection(Duration::ZERO, handle);
            // don't need to poll_gossip, because no read events may be generated here
        }

        scheduler
    }

    fn inbound(
        &mut self,
        time: std::time::Duration,
        from: monad_executor_glue::PeerId,
        message: Self::Serialized,
    ) {
        if from == self.me {
            self.gossip.handle_gossip_message(time, from, &message);
            self.poll_gossip(time);
        } else if let Some(maybe_event) = self.endpoint.handle(
            self.zero_instant + time,
            *self.peer_ids.get(&from).expect("peer_id should exist"),
            None,
            None, // TODO ecn codepoint?
            message.into_iter().collect(),
        ) {
            match maybe_event {
                DatagramEvent::ConnectionEvent(connection_handle, event) => {
                    let connection = self
                        .connections
                        .get_mut(&connection_handle)
                        .expect("connection should exist");

                    connection.handle_event(event);

                    self.poll_connection(time, &connection_handle);
                    self.poll_gossip(time);
                }
                DatagramEvent::NewConnection(connection_handle, connection) => {
                    let replaced = self.connections.insert(connection_handle, connection);
                    assert!(replaced.is_none());

                    self.poll_connection(time, &connection_handle);
                    self.poll_gossip(time);
                    return;
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
                        .push(RouterEvent::Tx(to, contents.into()))
                }
            };
        }
    }

    fn outbound<OM: Into<Self::M>>(&mut self, time: Duration, to: RouterTarget, message: OM) {
        let message = message.into();
        self.gossip.send(time, to, &message);
        self.poll_gossip(time);
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _)| tick)
    }

    fn step_until(&mut self, until: Duration) -> Option<RouterEvent<Self::M, Self::Serialized>> {
        while let Some((min_tick, event_type)) = self.peek_event() {
            if min_tick > until {
                break;
            }
            match event_type {
                QuicEventType::Event => {
                    let mut entry = self.pending_events.first_entry().unwrap();
                    assert_eq!(min_tick, *entry.key());
                    let events = entry.get_mut();
                    let event = events.pop().expect("events should never be empty");

                    if events.is_empty() {
                        self.pending_events.pop_first().unwrap();
                    }
                    return Some(event);
                }
                QuicEventType::Timeout => {
                    let (timeout_tick, connection_handle) =
                        self.timeouts.pop().expect("invariant broken");
                    assert_eq!(min_tick, timeout_tick);

                    let connection = self
                        .connections
                        .get_mut(&connection_handle)
                        .expect("connection should exist");

                    connection.handle_timeout(self.zero_instant + timeout_tick);
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

impl<G: Gossip> QuicRouterScheduler<G> {
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

    /// should be followed up with a self.poll_gossip (because connection events may be generated)
    fn poll_connection(&mut self, time: Duration, connection_handle: &ConnectionHandle) {
        let mut should_poll = true;
        while should_poll {
            should_poll = false;
            let connection = self
                .connections
                .get_mut(connection_handle)
                .expect("connection should exist");

            let remote_peer_id = self
                .addresses
                .get(&connection.remote_address())
                .expect("address should exist");

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
                    .push(RouterEvent::Tx(to, transmit.contents.into()))
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

            if let Some((outbound_connection_handle, Some(stream_id))) =
                self.outbound_connections.get(remote_peer_id)
            {
                if connection_handle == outbound_connection_handle {
                    // we only check pending_outbound if it's an outbound connection.
                    let pending_outbound = self
                        .pending_outbound_messages
                        .entry(*remote_peer_id)
                        .or_default();
                    while let Some(gossip_message) = pending_outbound.pop_front() {
                        let num_sent = connection
                            .send_stream(*stream_id)
                            .write(&gossip_message)
                            .expect("expect no write error for mock quic");
                        assert_eq!(gossip_message.len(), num_sent);
                    }
                }
            }

            while let Some(event) = connection.poll() {
                match event {
                    quinn_proto::Event::HandshakeDataReady => (),
                    quinn_proto::Event::Connected => {
                        if connection.side().is_client() {
                            let stream_id = connection
                                .streams()
                                .open(Dir::Uni)
                                .expect("creating stream_id failed");

                            self.outbound_connections
                                .get_mut(remote_peer_id)
                                .expect("outbound connection should already exist")
                                .1 = Some(stream_id);
                            should_poll = true;
                            break;
                        }
                    }
                    quinn_proto::Event::ConnectionLost { reason } => {
                        todo!("time: {:?}, {reason:?}", time)
                    }
                    quinn_proto::Event::Stream(stream_event) => match stream_event {
                        quinn_proto::StreamEvent::Opened { dir } => {
                            let stream_id = connection
                                .streams()
                                .accept(dir)
                                .expect("stream id should exist");

                            let mut recv_stream = connection.recv_stream(stream_id);
                            let mut chunks = recv_stream.read(true).expect("failed to read chunks");
                            while let Ok(Some(chunk)) = chunks.next(
                                10_000, // TODO ?
                            ) {
                                self.gossip.handle_gossip_message(
                                    time,
                                    *remote_peer_id,
                                    &chunk.bytes,
                                );
                            }
                            let _should_transmit = chunks.finalize();
                        }
                        quinn_proto::StreamEvent::Readable { id } => {
                            let mut recv_stream = connection.recv_stream(id);
                            let mut chunks = recv_stream.read(true).expect("failed to read chunks");
                            while let Ok(Some(chunk)) = chunks.next(
                                10_000, // TODO ?
                            ) {
                                self.gossip.handle_gossip_message(
                                    time,
                                    *remote_peer_id,
                                    &chunk.bytes,
                                );
                            }
                            let _should_transmit = chunks.finalize();
                        }
                        quinn_proto::StreamEvent::Writable { id } => todo!(),
                        quinn_proto::StreamEvent::Finished { id } => todo!(),
                        quinn_proto::StreamEvent::Stopped { id, error_code } => todo!(),
                        quinn_proto::StreamEvent::Available { dir } => todo!(),
                    },
                    quinn_proto::Event::DatagramReceived => todo!(),
                }
            }
        }
    }

    fn poll_gossip(&mut self, time: Duration) {
        while let Some(event) = self.gossip.poll(time) {
            match event {
                GossipEvent::Send(peer_id, gossip_message) => {
                    if peer_id == self.me {
                        self.pending_events
                            .entry(time)
                            .or_default()
                            .push(RouterEvent::Tx(peer_id, gossip_message))
                    } else {
                        let maybe_connection_handle = self
                            .outbound_connections
                            .get(&peer_id)
                            .map(|(handle, _)| handle)
                            .copied();
                        self.pending_outbound_messages
                            .entry(peer_id)
                            .or_default()
                            .push_back(gossip_message);
                        if let Some(connection_handle) = maybe_connection_handle {
                            self.poll_connection(time, &connection_handle);
                        }
                    }
                }
                GossipEvent::Emit(peer_id, message) => {
                    assert!(
                        time >= self
                            .pending_events
                            .last_key_value()
                            .map(|(time, _)| *time)
                            .unwrap_or(Duration::ZERO)
                    );
                    let event = RouterEvent::Rx(peer_id, message);
                    self.pending_events.entry(time).or_default().push(event)
                }
            }
        }
    }
}
