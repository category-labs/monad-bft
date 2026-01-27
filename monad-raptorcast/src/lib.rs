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

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future as _,
    marker::PhantomData,
    net::{IpAddr, SocketAddr, SocketAddrV4},
    ops::DerefMut,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use message::{InboundRouterMessage, OutboundRouterMessage};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::{
    udp::{DEFAULT_MTU, ETHERNET_SEGMENT_SIZE},
    DataplaneBuilder, DataplaneControl, RecvTcpMsg, TcpMsg, TcpSocketHandle, TcpSocketId,
    TcpSocketReader, TcpSocketWriter, UdpSocketHandle, UdpSocketId, UnicastMsg,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ControlPanelEvent, GetFullNodes, GetPeers, Message, MonadEvent, PeerEntry, RouterCommand,
};
use monad_node_config::{FullNodeConfig, FullNodeRaptorCastConfig};
use monad_peer_discovery::{
    driver::{PeerDiscoveryDriver, PeerDiscoveryEmit},
    message::PeerDiscoveryMessage,
    mock::{NopDiscovery, NopDiscoveryBuilder},
    NameRecord, PeerDiscoveryAlgo, PeerDiscoveryEvent,
};
use monad_types::{DropTimer, Epoch, ExecutionProtocol, NodeId, Round, RouterTarget, UdpPriority};
use monad_validator::{
    signature_collection::SignatureCollection,
    validator_set::{ValidatorSet, ValidatorSetType as _},
};
use protocol::{
    point_to_point::PointToPointTarget, PublishResultLogging, ReceiveResultLogging as _,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, debug_span, error, trace, warn};
use udp::GroupId;
use util::{
    AuthenticatedPayload, Collector, EmptyCollector, Group, KnownSocketAddr, PeerAddrLookup,
    Recipient, TcpMessage,
};

use crate::{
    metrics::{GAUGE_RAPTORCAST_TOTAL_MESSAGES_RECEIVED, GAUGE_RAPTORCAST_TOTAL_RECV_ERRORS},
    raptorcast_secondary::{
        group_message::FullNodesGroupMessage, SecondaryOutboundMessage,
        SecondaryRaptorCastModeConfig,
    },
    util::UdpMessage,
};

pub mod auth;
pub mod config;
pub mod decoding;
pub mod message;
pub mod metrics;
pub mod packet;
pub mod protocol;
pub mod raptorcast_secondary;
pub mod udp;
pub mod util;

const SIGNATURE_SIZE: usize = 65;
const DEFAULT_RETRY_ATTEMPTS: u64 = 3;

pub const UNICAST_MSG_BATCH_SIZE: usize = 32;

pub struct RaptorCast<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    is_dynamic_fullnode: bool,

    dedicated_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    // Currently used in two places:
    // - track epoch update to update trusted validator IPs
    // - track current epoch validators for validating secondary group messages
    current_epoch: Epoch,
    dispatcher: protocol::Dispatcher<ST>,

    tcp_reader: TcpSocketReader,
    tcp_writer: TcpSocketWriter,
    dual_socket: auth::DualSocketHandle<AP>,
    dataplane_control: DataplaneControl,
    pending_events: VecDeque<RaptorCastEvent<M::Event, ST>>,

    channel_to_secondary: Option<UnboundedSender<FullNodesGroupMessage<ST>>>,
    channel_from_secondary: Option<UnboundedReceiver<Group<CertificateSignaturePubKey<ST>>>>,
    channel_from_secondary_outbound:
        Option<UnboundedReceiver<SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>>>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    peer_discovery_metrics: ExecutorMetrics,
    _phantom: PhantomData<(OM, SE)>,
}

pub enum PeerManagerResponse<ST: CertificateSignatureRecoverable> {
    PeerList(Vec<PeerEntry<ST>>),
    FullNodes(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
}

pub enum RaptorCastEvent<E, ST: CertificateSignatureRecoverable> {
    Message(E),
    PeerManagerResponse(PeerManagerResponse<ST>),
    SecondaryRaptorcastPeersUpdate(Round, Vec<NodeId<CertificateSignaturePubKey<ST>>>),
}

impl<ST, M, OM, SE, PD, AP> RaptorCast<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: config::RaptorCastConfig<ST>,
        secondary_mode: SecondaryRaptorCastModeConfig,
        tcp_socket: TcpSocketHandle,
        authenticated_socket: Option<UdpSocketHandle>,
        non_authenticated_socket: UdpSocketHandle,
        control: DataplaneControl,
        peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        current_epoch: Epoch,
        auth_protocol: AP,
    ) -> Self {
        let (tcp_reader, tcp_writer) = tcp_socket.split();

        if config.primary_instance.raptor10_redundancy < 1f32 {
            panic!(
                "Configuration value raptor10_redundancy must be equal or greater than 1, \
                but got {}. This is a bug in the configuration for the primary instance.",
                config.primary_instance.raptor10_redundancy
            );
        }
        let self_id = NodeId::new(config.shared_key.pubkey());
        let is_dynamic_fullnode = matches!(secondary_mode, SecondaryRaptorCastModeConfig::Client);
        debug!(
            ?is_dynamic_fullnode, ?self_id, ?config.mtu, "RaptorCast::new",
        );

        let dual_socket = auth::DualSocketHandle::new(
            authenticated_socket
                .map(|socket| auth::AuthenticatedSocketHandle::new(socket, auth_protocol)),
            non_authenticated_socket,
        );

        let segment_size = dual_socket.segment_size(config.mtu);

        let dispatcher = {
            let primary = protocol::primary_raptorcast::PrimaryRaptorcastState::new(
                config.shared_key.clone(),
                config.primary_instance.raptor10_redundancy,
                config.udp_message_max_age_ms,
                segment_size as usize,
            );

            let secondary_publisher = match secondary_mode {
                SecondaryRaptorCastModeConfig::Publisher => Some(
                    protocol::secondary_raptorcast::SecondaryRaptorcastPublisherState::new(
                        config.shared_key.clone(),
                        config
                            .secondary_instance
                            .raptor10_fullnode_redundancy_factor,
                        segment_size as usize,
                    ),
                ),
                _ => None,
            };

            let secondary_client = match secondary_mode {
                SecondaryRaptorCastModeConfig::Client => Some(
                    protocol::secondary_raptorcast::SecondaryRaptorcastClientState::new(
                        self_id,
                        config.udp_message_max_age_ms,
                    ),
                ),
                _ => None,
            };

            let point_to_point = protocol::point_to_point::PointToPointState::new(
                config.shared_key.clone(),
                config.primary_instance.raptor10_redundancy,
                config.udp_message_max_age_ms,
                segment_size as usize,
            );
            let tcp_point_to_point =
                protocol::tcp_point_to_point::TcpPointToPointState::new(config.shared_key.clone());
            let signature_verifier = protocol::ChunkSignatureVerifier::new()
                .with_cache(udp::SIGNATURE_CACHE_SIZE.get())
                .with_rate_limit(config.sig_verification_rate_limit);
            let decoder_cache = decoding::DecoderCache::default();

            protocol::Dispatcher::new(
                primary,
                secondary_publisher,
                secondary_client,
                point_to_point,
                tcp_point_to_point,
            )
            .with_signature_verifier(signature_verifier)
            .with_decoder_cache(decoder_cache)
            .with_metrics()
        };

        Self {
            is_dynamic_fullnode,
            dedicated_full_nodes: config.primary_instance.fullnode_dedicated,
            peer_discovery_driver,

            current_epoch,

            dispatcher,

            tcp_reader,
            tcp_writer,
            dual_socket,
            dataplane_control: control,
            pending_events: Default::default(),
            channel_to_secondary: None,
            channel_from_secondary: None,
            channel_from_secondary_outbound: None,

            waker: None,
            metrics: Default::default(),
            peer_discovery_metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    // If we are a validator, then we don't need `channel_from_secondary`, since
    // we won't be receiving groups from secondary.
    // If we are a full-node, then we need both channels.
    pub fn bind_channel_to_secondary_raptorcast(
        &mut self,
        channel_to_secondary: UnboundedSender<FullNodesGroupMessage<ST>>,
        channel_from_secondary: UnboundedReceiver<Group<CertificateSignaturePubKey<ST>>>,
        channel_from_secondary_outbound: UnboundedReceiver<
            SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>,
        >,
    ) {
        self.channel_to_secondary = Some(channel_to_secondary);
        self.channel_from_secondary_outbound = Some(channel_from_secondary_outbound);
        if self.is_dynamic_fullnode {
            self.channel_from_secondary = Some(channel_from_secondary);
        } else {
            self.channel_from_secondary = None;
        }
    }

    pub fn set_is_dynamic_full_node(&mut self, is_dynamic: bool) {
        debug!(?is_dynamic, "updating primary raptorcast");
        self.is_dynamic_fullnode = is_dynamic;
    }

    pub fn set_dedicated_full_nodes(&mut self, nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>) {
        // self must not to be in dedicated full nodes list
        self.dedicated_full_nodes = nodes;
    }

    fn handle_secondary_outbound_message(
        &mut self,
        outbound_msg: SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>,
    ) {
        let mut udp_sink =
            DualUdpPacketSender::new(&mut self.dual_socket, &self.peer_discovery_driver);

        match outbound_msg {
            SecondaryOutboundMessage::SendSingle {
                msg_bytes,
                dest,
                group_id: _,
            } => {
                trace!(
                    ?dest,
                    msg_len = msg_bytes.len(),
                    "raptorcastprimary handling single message from secondary"
                );
                let target = PointToPointTarget::Single(dest);
                self.dispatcher
                    .publish_point_to_point(
                        target,
                        msg_bytes,
                        &mut udp_sink,
                        &mut EmptyCollector, // message loopback
                    )
                    .log_publish_error("secondary outbound single")
                    .ok();
            }
            SecondaryOutboundMessage::SendToGroup {
                msg_bytes,
                group_id,
                group,
            } => {
                trace!(
                    group_size = group.size_excl_self(),
                    msg_len = msg_bytes.len(),
                    "raptorcastprimary handling group message from secondary"
                );
                if group.size_excl_self() < 1 {
                    return;
                }
                let GroupId::Secondary(round) = group_id else {
                    return;
                };
                // TODO: check if we need to update the group in the
                // secondary raptorcast protocol state.
                self.dispatcher
                    .publish_secondary(round, msg_bytes, &mut udp_sink)
                    .log_publish_error("secondary outbound group")
                    .ok();
            }
        }
    }

    fn handle_publish(
        &mut self,
        target: RouterTarget<CertificateSignaturePubKey<ST>>,
        message: OM,
        priority: UdpPriority,
    ) {
        let _span = debug_span!("router publish").entered();
        let outbound_message =
            match OutboundRouterMessage::<OM, ST>::AppMessage(message).try_serialize() {
                Ok(msg) => msg,
                Err(err) => {
                    error!(?err, "failed to serialize a message");
                    return;
                }
            };

        let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
            warn!(?elapsed, "long time to publish message")
        });

        let mut udp_sink =
            DualUdpPacketSender::new(&mut self.dual_socket, &self.peer_discovery_driver)
                .with_priority(priority);
        let mut tcp_sink = TcpPacketSender::new(&self.tcp_writer, &self.peer_discovery_driver);
        let mut loopback_sink: DeserializingReceiver<M, ST> = DeserializingReceiver::new();

        self.dispatcher
            .publish(
                target,
                outbound_message,
                &mut udp_sink,
                &mut tcp_sink,
                &mut loopback_sink,
            )
            .log_publish_error("publish")
            .ok();

        for (author, msg) in loopback_sink {
            if let InboundRouterMessage::AppMessage(message) = msg {
                self.pending_events
                    .push_back(RaptorCastEvent::Message(message.event(author)));
                if let Some(waker) = self.waker.take() {
                    waker.wake()
                }
            }
        }
    }

    fn handle_publish_to_dedicated_fullnodes(&mut self, message: OM) {
        if self.is_dynamic_fullnode {
            debug!("self is dynamic full node, skipping publishing to full nodes");
            return;
        }

        // self as a dedicated full node will have empty
        // full_nodes_view, so it won't attempt to publish.
        if self.dedicated_full_nodes.is_empty() {
            debug!("full_nodes view empty, skipping publishing to full nodes");
            return;
        }

        let app_message = OutboundRouterMessage::<OM, ST>::AppMessage(message).try_serialize();
        let outbound_message = match app_message {
            Ok(msg) => msg,
            Err(err) => {
                error!(?err, "failed to serialize a message");
                return;
            }
        };

        let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
            warn!(
                ?elapsed,
                app_msg_len = outbound_message.len(),
                "long time to build message"
            )
        });

        let mut udp_sink =
            DualUdpPacketSender::new(&mut self.dual_socket, &self.peer_discovery_driver);

        let target = PointToPointTarget::Multi(self.dedicated_full_nodes.clone());
        self.dispatcher
            .publish_point_to_point(
                target,
                outbound_message.clone(),
                &mut udp_sink,
                &mut EmptyCollector, // no loopback for publishing to full nodes
            )
            .log_publish_error("publish to full nodes")
            .ok();
    }
}

pub struct DataplaneHandles {
    pub tcp_socket: monad_dataplane::TcpSocketHandle,
    pub authenticated_socket: Option<UdpSocketHandle>,
    pub non_authenticated_socket: UdpSocketHandle,
    pub control: DataplaneControl,
    pub tcp_addr: SocketAddrV4,
    pub auth_addr: Option<SocketAddrV4>,
    pub non_auth_addr: SocketAddrV4,
}

pub fn create_dataplane_for_tests(with_auth: bool) -> DataplaneHandles {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let up_bandwidth_mbps = 1_000;

    let mut udp_sockets: Vec<(UdpSocketId, SocketAddr)> =
        vec![(UdpSocketId::Raptorcast, bind_addr)];

    if with_auth {
        udp_sockets.insert(0, (UdpSocketId::AuthenticatedRaptorcast, bind_addr));
    }

    let mut dp = DataplaneBuilder::new(up_bandwidth_mbps)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, bind_addr)])
        .with_udp_sockets(udp_sockets)
        .build();

    let tcp_socket = dp.tcp_sockets.take(TcpSocketId::Raptorcast).unwrap();
    let tcp_addr = match tcp_socket.local_addr() {
        SocketAddr::V4(addr) => addr,
        _ => panic!("expected v4 address"),
    };

    let (authenticated_socket, auth_addr) = if with_auth {
        let socket = dp
            .udp_sockets
            .take(UdpSocketId::AuthenticatedRaptorcast)
            .expect("authenticated socket");
        let addr = match socket.local_addr() {
            SocketAddr::V4(addr) => addr,
            _ => panic!("expected v4 address"),
        };
        (Some(socket), Some(addr))
    } else {
        (None, None)
    };

    let non_authenticated_socket = dp
        .udp_sockets
        .take(UdpSocketId::Raptorcast)
        .expect("non-authenticated socket");
    let non_auth_addr = match non_authenticated_socket.local_addr() {
        SocketAddr::V4(addr) => addr,
        _ => panic!("expected v4 address"),
    };

    DataplaneHandles {
        tcp_socket,
        authenticated_socket,
        non_authenticated_socket,
        control: dp.control,
        tcp_addr,
        auth_addr,
        non_auth_addr,
    }
}

pub fn new_defaulted_raptorcast_for_tests<ST, M, OM, SE>(
    dataplane: DataplaneHandles,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    shared_key: Arc<ST::KeyPairType>,
) -> RaptorCast<
    ST,
    M,
    OM,
    SE,
    NopDiscovery<ST>,
    auth::NoopAuthProtocol<CertificateSignaturePubKey<ST>>,
>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
{
    let peer_discovery_builder = NopDiscoveryBuilder {
        known_addresses,
        ..Default::default()
    };
    let config = config::RaptorCastConfig {
        shared_key,
        mtu: DEFAULT_MTU,
        udp_message_max_age_ms: u64::MAX, // No timestamp validation for tests
        sig_verification_rate_limit: 10_000,
        primary_instance: Default::default(),
        secondary_instance: FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            raptor10_fullnode_redundancy_factor: 2f32,
            full_nodes_prioritized: FullNodeConfig { identities: vec![] },
            round_span: Round(10),
            invite_lookahead: Round(5),
            max_invite_wait: Round(3),
            deadline_round_dist: Round(3),
            init_empty_round_span: Round(1),
            max_group_size: 10,
            max_num_group: 5,
            invite_future_dist_min: Round(1),
            invite_future_dist_max: Round(5),
            invite_accept_heartbeat_ms: 100,
        },
    };
    let pd = PeerDiscoveryDriver::new(peer_discovery_builder);
    let shared_pd = Arc::new(Mutex::new(pd));
    let auth_protocol = auth::NoopAuthProtocol::new();
    RaptorCast::<ST, M, OM, SE, NopDiscovery<ST>, _>::new(
        config,
        SecondaryRaptorCastModeConfig::None,
        dataplane.tcp_socket,
        dataplane.authenticated_socket,
        dataplane.non_authenticated_socket,
        dataplane.control,
        shared_pd,
        Epoch(0),
        auth_protocol,
    )
}

pub fn new_wireauth_raptorcast_for_tests<ST, M, OM, SE>(
    dataplane: DataplaneHandles,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    shared_key: Arc<ST::KeyPairType>,
) -> RaptorCast<ST, M, OM, SE, NopDiscovery<ST>, auth::WireAuthProtocol>
where
    ST: CertificateSignatureRecoverable<KeyPairType = monad_secp::KeyPair>,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
{
    let peer_discovery_builder = NopDiscoveryBuilder {
        known_addresses,
        ..Default::default()
    };
    let config = config::RaptorCastConfig {
        shared_key: shared_key.clone(),
        mtu: DEFAULT_MTU,
        udp_message_max_age_ms: u64::MAX,
        sig_verification_rate_limit: 10_000,
        primary_instance: Default::default(),
        secondary_instance: FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            raptor10_fullnode_redundancy_factor: 2f32,
            full_nodes_prioritized: FullNodeConfig { identities: vec![] },
            round_span: Round(10),
            invite_lookahead: Round(5),
            max_invite_wait: Round(3),
            deadline_round_dist: Round(3),
            init_empty_round_span: Round(1),
            max_group_size: 10,
            max_num_group: 5,
            invite_future_dist_min: Round(1),
            invite_future_dist_max: Round(5),
            invite_accept_heartbeat_ms: 100,
        },
    };
    let pd = PeerDiscoveryDriver::new(peer_discovery_builder);
    let shared_pd = Arc::new(Mutex::new(pd));
    let wireauth_config = monad_wireauth::Config::default();
    let auth_protocol = auth::WireAuthProtocol::new(wireauth_config, shared_key);
    RaptorCast::<ST, M, OM, SE, NopDiscovery<ST>, _>::new(
        config,
        SecondaryRaptorCastModeConfig::None,
        dataplane.tcp_socket,
        dataplane.authenticated_socket,
        dataplane.non_authenticated_socket,
        dataplane.control,
        shared_pd,
        Epoch(0),
        auth_protocol,
    )
}

impl<ST, M, OM, SE, PD, AP> Executor for RaptorCast<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    type Command = RouterCommand<ST, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    if self.current_epoch < epoch {
                        trace!(?epoch, ?round, "RaptorCast UpdateCurrentRound");
                        let pd_driver = self.peer_discovery_driver.lock().unwrap();
                        let added: Vec<_> = self
                            .dispatcher
                            .get_validator_set(epoch)
                            .into_iter()
                            .flat_map(|valset| lookup_validator_ips(valset, &*pd_driver))
                            .collect();
                        let removed: Vec<_> = self
                            .dispatcher
                            .get_validator_set(self.current_epoch)
                            .into_iter()
                            .flat_map(|valset| lookup_validator_ips(valset, &*pd_driver))
                            .collect();

                        drop(pd_driver);
                        self.dataplane_control.update_trusted(added, removed);
                        self.current_epoch = epoch;
                    }
                    self.dispatcher.enter_round(self.current_epoch, round);
                    self.peer_discovery_driver
                        .lock()
                        .unwrap()
                        .update(PeerDiscoveryEvent::UpdateCurrentRound { round, epoch });
                }
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    trace!(?epoch, ?validator_set, "RaptorCast AddEpochValidatorSet");
                    // SAFETY: the validator_set comes from
                    // ValidatorSetData, which should not have
                    // duplicates or invalid entries.
                    let validators = Arc::new(ValidatorSet::new_unchecked(
                        validator_set.clone().into_iter().collect(),
                    ));
                    self.dispatcher.update_epoch_validators(epoch, validators);
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdateValidatorSet {
                            epoch,
                            validators: validator_set.iter().map(|(id, _)| *id).collect(),
                        },
                    );
                }
                RouterCommand::Publish { target, message } => {
                    self.handle_publish(target, message, UdpPriority::Regular);
                }
                RouterCommand::PublishWithPriority {
                    target,
                    message,
                    priority,
                } => {
                    self.handle_publish(target, message, priority);
                }
                RouterCommand::PublishToFullNodes {
                    epoch: _,
                    round: _,
                    message,
                } => {
                    self.handle_publish_to_dedicated_fullnodes(message);
                }
                RouterCommand::GetPeers => {
                    let name_records = self
                        .peer_discovery_driver
                        .lock()
                        .unwrap()
                        .get_name_records();
                    let peer_list = name_records
                        .iter()
                        .map(|(node_id, name_record)| {
                            name_record.with_pubkey(node_id.pubkey()).into()
                        })
                        .collect::<Vec<_>>();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::PeerList(peer_list),
                        ));
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                RouterCommand::UpdatePeers {
                    peer_entries,
                    dedicated_full_nodes,
                    prioritized_full_nodes,
                } => {
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdatePeers {
                            peers: peer_entries,
                        },
                    );
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdatePinnedNodes {
                            dedicated_full_nodes: dedicated_full_nodes.into_iter().collect(),
                            prioritized_full_nodes: prioritized_full_nodes.into_iter().collect(),
                        },
                    );
                }
                RouterCommand::GetFullNodes => {
                    let full_nodes = self.dedicated_full_nodes.clone();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::FullNodes(full_nodes),
                        ));
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                RouterCommand::UpdateFullNodes {
                    dedicated_full_nodes,
                    prioritized_full_nodes: _,
                } => {
                    self.dedicated_full_nodes = dedicated_full_nodes;
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(self.metrics.as_ref())
            .push(self.peer_discovery_metrics.as_ref())
            .chain(self.dispatcher.metrics())
            .chain(self.dual_socket.metrics())
    }
}

impl<ST, M, OM, E, PD, AP> Stream for RaptorCast<ST, M, OM, E, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PeerDiscoveryDriver<PD>: Unpin,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        trace!("polling raptorcast");
        let this = self.deref_mut();

        if let Some(waker) = this.waker.as_mut() {
            waker.clone_from(cx.waker());
        } else {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event.into()));
        }

        loop {
            let message = {
                let mut sock = pin!(this.dual_socket.recv());

                match sock.poll_unpin(cx) {
                    Poll::Ready(Ok(msg)) => {
                        this.metrics[GAUGE_RAPTORCAST_TOTAL_MESSAGES_RECEIVED] += 1;
                        msg
                    }
                    Poll::Ready(Err(e)) => {
                        this.metrics[GAUGE_RAPTORCAST_TOTAL_RECV_ERRORS] += 1;
                        trace!(error=?e, "socket recv error");
                        continue;
                    }
                    Poll::Pending => break,
                }
            };

            trace!(
                "RaptorCastPrimary rx message len {} from: {}",
                message.payload.len(),
                message.src_addr
            );

            let auth_sender = message.auth_public_key.map(NodeId::new);
            let stride = message.stride as usize;
            let mut rebroadcast_udp_sink =
                DualUdpPacketSender::new(&mut this.dual_socket, &this.peer_discovery_driver);
            let mut messages = DeserializingReceiver::<M, ST>::new();

            for payload_start_idx in (0..message.payload.len()).step_by(stride) {
                let payload_end_idx = (payload_start_idx + stride).min(message.payload.len());
                let packet = message.payload.slice(payload_start_idx..payload_end_idx);

                this.dispatcher
                    .receive_udp(
                        auth_sender,
                        packet,
                        &mut rebroadcast_udp_sink,
                        &mut messages,
                    )
                    .log_receive_error("receive udp packet")
                    .ok();
            }

            trace!("RaptorCastPrimary rx decoded {} messages", messages.len());

            for (from, deserialized) in messages {
                match deserialized {
                    InboundRouterMessage::AppMessage(app_message) => {
                        trace!("RaptorCastPrimary rx deserialized AppMessage");
                        this.pending_events
                            .push_back(RaptorCastEvent::Message(app_message.event(from)));
                    }
                    InboundRouterMessage::PeerDiscoveryMessage(peer_disc_message) => {
                        trace!(
                            "RaptorCastPrimary rx deserialized PeerDiscoveryMessage: {:?}",
                            peer_disc_message
                        );
                        // handle peer discovery message in driver
                        this.peer_discovery_driver
                            .lock()
                            .unwrap()
                            .update(peer_disc_message.event(from));
                    }
                    InboundRouterMessage::FullNodesGroup(full_nodes_group_message) => {
                        trace!(
                            "RaptorCastPrimary rx deserialized {:?}",
                            full_nodes_group_message
                        );
                        match &this.channel_to_secondary {
                            Some(channel) => {
                                // drop full node group message with unauthorized sender
                                let Some(current_epoch_validators) =
                                    this.dispatcher.get_validator_set(this.current_epoch)
                                else {
                                    warn!(
                                        "No validators found for current epoch: {:?}",
                                        this.current_epoch
                                    );
                                    continue;
                                };

                                let is_valid = raptorcast_secondary::validate_group_message_sender(
                                    &from,
                                    &full_nodes_group_message,
                                    current_epoch_validators,
                                );

                                if !is_valid {
                                    warn!(
                                        ?from,
                                        "Received FullNodesGroup message from unauthorized sender"
                                    );
                                    continue;
                                }

                                if channel.send(full_nodes_group_message).is_err() {
                                    error!(
                                        "Could not send InboundRouterMessage to \
                                    secondary Raptorcast instance: channel closed",
                                    );
                                }
                            }
                            None => {
                                debug!(
                                    ?from,
                                    "Received FullNodesGroup message but the primary \
                                Raptorcast instance is not setup to forward messages \
                                to a secondary instance."
                                );
                            }
                        }
                    }
                }
            }

            if let Some(event) = this.pending_events.pop_front() {
                return Poll::Ready(Some(event.into()));
            }
        }

        loop {
            let Poll::Ready(msg) = pin!(this.tcp_reader.recv()).poll_unpin(cx) else {
                break;
            };
            let RecvTcpMsg { payload, src_addr } = msg;
            let auth_sender = None;
            let disconnect = || this.dataplane_control.disconnect(src_addr);
            let mut messages = DeserializingReceiver::<M, ST>::new();

            this.dispatcher
                .receive_tcp(auth_sender, payload, disconnect, &mut messages)
                .log_receive_error("receive tcp message")
                .ok();

            for (author, message) in messages {
                match message {
                    InboundRouterMessage::AppMessage(message) => {
                        return Poll::Ready(Some(
                            RaptorCastEvent::Message(message.event(author)).into(),
                        ));
                    }
                    InboundRouterMessage::PeerDiscoveryMessage(message) => {
                        // peer discovery message should come through udp
                        debug!(
                            ?message,
                            "dropping peer discovery message, should come through udp channel"
                        );
                        this.dataplane_control.disconnect(src_addr);
                        continue;
                    }
                    InboundRouterMessage::FullNodesGroup(_group_message) => {
                        warn!("FullNodesGroup protocol via TCP not implemented");
                        this.dataplane_control.disconnect(src_addr);
                        continue;
                    }
                }
            }
        }

        {
            let send_peer_disc_msg =
                |this: &mut RaptorCast<ST, M, OM, E, PD, AP>,
                 target: NodeId<CertificateSignaturePubKey<ST>>,
                 target_name_record: Option<NameRecord>,
                 message: PeerDiscoveryMessage<ST>| {
                    let _span = debug_span!("publish discovery").entered();
                    let Ok(router_message) =
                        OutboundRouterMessage::<OM, ST>::PeerDiscoveryMessage(message)
                            .try_serialize()
                    else {
                        error!("failed to serialize peer discovery message");
                        return;
                    };

                    let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                        warn!(
                            ?elapsed,
                            app_msg_len = router_message.len(),
                            "long time to build discovery message"
                        )
                    });

                    let mut sink = DualUdpPacketSender::new(
                        &mut this.dual_socket,
                        &this.peer_discovery_driver,
                    );

                    if let Some(name_record) = target_name_record {
                        let addr = SocketAddr::V4(name_record.udp_socket());
                        sink = sink.with_target_addr(&target, addr);
                    }

                    this.dispatcher
                        .publish_point_to_point(
                            PointToPointTarget::Single(target),
                            router_message.clone(),
                            &mut sink,
                            &mut EmptyCollector,
                        )
                        .log_publish_error("peer discovery message")
                        .ok();
                };

            loop {
                let mut pd_driver = this.peer_discovery_driver.lock().unwrap();
                let Poll::Ready(Some(peer_disc_emit)) = pd_driver.poll_next_unpin(cx) else {
                    break;
                };
                // unlock pd driver so it can be used for lookup peers in `send_peer_disc_msg`.
                drop(pd_driver);

                match peer_disc_emit {
                    PeerDiscoveryEmit::RouterCommand { target, message } => {
                        send_peer_disc_msg(this, target, None, message);
                    }
                    PeerDiscoveryEmit::PingPongCommand {
                        target,
                        name_record,
                        message,
                    } => {
                        send_peer_disc_msg(this, target, Some(name_record), message);
                    }
                    PeerDiscoveryEmit::MetricsCommand(executor_metrics) => {
                        this.peer_discovery_metrics = executor_metrics;
                    }
                }
            }
        }

        // The secondary Raptorcast instance (Client) will be periodically sending us
        // updates about new raptorcast groups that we should use when re-broadcasting
        if let Some(channel_from_secondary) = this.channel_from_secondary.as_mut() {
            loop {
                match pin!(channel_from_secondary.recv()).poll(cx) {
                    Poll::Ready(Some(group)) => {
                        let publisher = *group.get_validator_id();
                        let round_span = group.get_round_span().to_range();
                        let peers = group.iter_peers().copied().collect();

                        this.dispatcher
                            .update_secondary_group(publisher, round_span, peers);
                    }
                    Poll::Ready(None) => {
                        error!("RaptorCast secondary->primary channel disconnected.");
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }

        loop {
            let msg_option =
                this.channel_from_secondary_outbound
                    .as_mut()
                    .and_then(|ch| match pin!(ch.recv()).poll(cx) {
                        Poll::Ready(Some(msg)) => Some(Ok(msg)),
                        Poll::Ready(None) => Some(Err(())),
                        Poll::Pending => None,
                    });

            match msg_option {
                Some(Ok(outbound_msg)) => {
                    this.handle_secondary_outbound_message(outbound_msg);
                }
                Some(Err(())) => {
                    error!("RaptorCast secondary->primary outbound channel disconnected.");
                    this.channel_from_secondary_outbound = None;
                    break;
                }
                None => break,
            }
        }

        Poll::Pending
    }
}

impl<ST, SCT, EPT> From<RaptorCastEvent<MonadEvent<ST, SCT, EPT>, ST>> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: RaptorCastEvent<MonadEvent<ST, SCT, EPT>, ST>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(peer_manager_response) => {
                match peer_manager_response {
                    PeerManagerResponse::PeerList(peer) => MonadEvent::ControlPanelEvent(
                        ControlPanelEvent::GetPeers(GetPeers::Response(peer)),
                    ),
                    PeerManagerResponse::FullNodes(full_nodes) => MonadEvent::ControlPanelEvent(
                        ControlPanelEvent::GetFullNodes(GetFullNodes::Response(full_nodes)),
                    ),
                }
            }
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate(expiry_round, confirm_group_peers) => {
                MonadEvent::SecondaryRaptorcastPeersUpdate {
                    expiry_round,
                    confirm_group_peers,
                }
            }
        }
    }
}

struct DualUdpPacketSender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    dual_socket: &'a mut auth::DualSocketHandle<AP>,
    peer_disc_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    target_addr: Option<KnownSocketAddr<'a, CertificateSignaturePubKey<ST>>>,
    targets: HashSet<Recipient<CertificateSignaturePubKey<ST>>>,
    priority: UdpPriority,
    // TODO: add optional batching
    _signature_type: PhantomData<ST>,
}

impl<'a, ST, AP, PD> DualUdpPacketSender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    fn new(
        dual_socket: &'a mut auth::DualSocketHandle<AP>,
        peer_disc_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    ) -> Self {
        Self {
            dual_socket,
            peer_disc_driver,
            target_addr: None,
            targets: Default::default(),
            priority: UdpPriority::Regular,
            _signature_type: PhantomData,
        }
    }

    fn with_priority(mut self, priority: UdpPriority) -> DualUdpPacketSender<'a, ST, PD, AP> {
        self.priority = priority;
        self
    }

    fn with_target_addr(
        mut self,
        target_node: &'a NodeId<CertificateSignaturePubKey<ST>>,
        addr: SocketAddr,
    ) -> DualUdpPacketSender<'a, ST, PD, AP> {
        self.target_addr = Some(KnownSocketAddr(target_node, addr));
        self
    }
}

impl<'a, ST, PD, AP> Drop for DualUdpPacketSender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn drop(&mut self) {
        let targets = self.targets.iter().map(|r| r.node_id());
        ensure_authenticated_sessions(self.dual_socket, self.peer_disc_driver, targets);
    }
}

impl<'a, ST, PD, AP> Collector<UdpMessage<CertificateSignaturePubKey<ST>>>
    for DualUdpPacketSender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn push(&mut self, item: UdpMessage<CertificateSignaturePubKey<ST>>) {
        let recipient_dest = if let Some(target_addr) = self.target_addr.as_ref() {
            let peer_lookup = (&*self.dual_socket, target_addr);
            item.recipient.lookup(&peer_lookup)
        } else {
            let peer_lookup = (&*self.dual_socket, &*self.peer_disc_driver);
            item.recipient.lookup(&peer_lookup)
        };

        let Some(dest) = recipient_dest else {
            return;
        };

        self.targets.insert(item.recipient.clone());
        let msg = UnicastMsg {
            stride: item.payload.len() as u16,
            msgs: vec![(*dest, item.payload)],
        };

        self.dual_socket
            .write_unicast_with_priority(msg, self.priority);
    }
}

struct TcpPacketSender<'a, ST, PD>
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    tcp_writer: &'a TcpSocketWriter,
    peer_disc_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    // TODO: add optional batching
    _signature_type: PhantomData<ST>,
}

impl<'a, ST, PD> TcpPacketSender<'a, ST, PD>
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn new(
        tcp_writer: &'a TcpSocketWriter,
        peer_disc_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    ) -> Self {
        Self {
            tcp_writer,
            peer_disc_driver,
            _signature_type: PhantomData,
        }
    }
}

impl<'a, ST, PD> Collector<TcpMessage<CertificateSignaturePubKey<ST>>>
    for TcpPacketSender<'a, ST, PD>
where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn push(&mut self, item: TcpMessage<CertificateSignaturePubKey<ST>>) {
        let TcpMessage {
            recipient,
            payload,
            completion,
        } = item;
        let Some(dest) = recipient.lookup(&*self.peer_disc_driver) else {
            return;
        };

        let tcp_msg = TcpMsg {
            msg: payload,
            completion,
        };
        self.tcp_writer.write(*dest, tcp_msg);
    }
}

struct DeserializingReceiver<M, ST>
where
    ST: CertificateSignatureRecoverable,
{
    decoded_messages: Vec<(
        NodeId<CertificateSignaturePubKey<ST>>,
        InboundRouterMessage<M, ST>,
    )>,
    bad_senders: HashSet<NodeId<CertificateSignaturePubKey<ST>>>,
    _signature_type: PhantomData<ST>,
}

impl<M, ST> DeserializingReceiver<M, ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn new() -> Self {
        Self {
            decoded_messages: Vec::new(),
            bad_senders: HashSet::new(),
            _signature_type: PhantomData,
        }
    }

    fn bad_senders(&self) -> &HashSet<NodeId<CertificateSignaturePubKey<ST>>> {
        &self.bad_senders
    }

    fn len(&self) -> usize {
        self.decoded_messages.len()
    }
}

impl<M, ST> IntoIterator for DeserializingReceiver<M, ST>
where
    ST: CertificateSignatureRecoverable,
{
    type Item = (
        NodeId<CertificateSignaturePubKey<ST>>,
        InboundRouterMessage<M, ST>,
    );
    type IntoIter = std::vec::IntoIter<(
        NodeId<CertificateSignaturePubKey<ST>>,
        InboundRouterMessage<M, ST>,
    )>;

    fn into_iter(self) -> Self::IntoIter {
        self.decoded_messages.into_iter()
    }
}

impl<M, ST> Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>
    for DeserializingReceiver<M, ST>
where
    M: Decodable,
    ST: CertificateSignatureRecoverable,
{
    fn push(&mut self, item: AuthenticatedPayload<CertificateSignaturePubKey<ST>>) {
        let deserialized_message = match InboundRouterMessage::try_deserialize(&item.payload) {
            Ok(message) => message,
            Err(err) => {
                warn!(?err, "failed to deserialize message");
                self.bad_senders.insert(item.author);
                return;
            }
        };

        self.decoded_messages
            .push((item.author, deserialized_message));
    }
}

#[allow(dead_code)]
fn rebroadcast_packet<ST, PD, AP>(
    dual_socket: &mut auth::DualSocketHandle<AP>,
    peer_discovery_driver: &Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    target: &NodeId<CertificateSignaturePubKey<ST>>,
    payload: Bytes,
    bcast_stride: u16,
) where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    // if the packet was created by non-upgraded node we won't be able to fit auth header
    let fits_with_auth_header =
        payload.len() + AP::HEADER_SIZE as usize <= ETHERNET_SEGMENT_SIZE as usize;

    // if we can fit auth header, check if connection exists, otherwise fallback to non-auth socket
    let target_addr = if fits_with_auth_header {
        dual_socket
            .get_socket_by_public_key(&target.pubkey())
            .or_else(|| {
                peer_discovery_driver
                    .lock()
                    .ok()
                    .and_then(|pd| pd.get_addr(target))
            })
    } else {
        peer_discovery_driver
            .lock()
            .ok()
            .and_then(|pd| pd.get_addr(target))
    };

    let Some(target_addr) = target_addr else {
        warn!(target=?target, "failed to find address for rebroadcast target");
        return;
    };

    dual_socket.write_unicast_with_priority(
        UnicastMsg {
            msgs: vec![(target_addr, payload)],
            stride: bcast_stride,
        },
        UdpPriority::High,
    );

    ensure_authenticated_sessions(dual_socket, peer_discovery_driver, std::iter::once(target));
}

fn ensure_authenticated_sessions<'a, ST, PD, AP>(
    dual_socket: &mut auth::DualSocketHandle<AP>,
    peer_discovery_driver: &Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    targets: impl Iterator<Item = &'a NodeId<CertificateSignaturePubKey<ST>>>,
) where
    ST: CertificateSignatureRecoverable,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    let pd_driver = peer_discovery_driver.lock().unwrap();

    targets
        .filter_map(|target| {
            pd_driver
                .get_name_record(target)
                .and_then(|record| record.name_record.authenticated_udp_socket())
                .map(|addr| (target, addr))
        })
        .for_each(|(target, auth_addr)| {
            if dual_socket.has_any_session_by_public_key(&target.pubkey()) {
                return;
            }

            if let Err(e) = dual_socket.connect(
                &target.pubkey(),
                SocketAddr::V4(auth_addr),
                DEFAULT_RETRY_ATTEMPTS,
            ) {
                warn!(
                    target=?target,
                    auth_addr=?auth_addr,
                    error=?e,
                    "failed to initiate connection to authenticated endpoint"
                );
            }
        });

    dual_socket.flush();
}

impl<PT, AP> PeerAddrLookup<PT> for auth::DualSocketHandle<AP>
where
    PT: PubKey,
    AP: auth::AuthenticationProtocol<PublicKey = PT>,
{
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.get_socket_by_public_key(&node_id.pubkey())
    }
}

fn lookup_validator_ips<'a, PT, PL>(
    valset: &'a ValidatorSet<PT>,
    peer_lookup: &'a PL,
) -> impl Iterator<Item = IpAddr> + 'a
where
    PT: PubKey,
    PL: PeerAddrLookup<PT>,
{
    valset
        .get_members()
        .iter()
        .filter_map(move |(node_id, _stake)| {
            peer_lookup
                .lookup(node_id)
                .map(|socket_addr| socket_addr.ip())
        })
}
