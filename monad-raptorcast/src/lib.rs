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
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
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
use bytes::{Bytes, BytesMut};
use futures::{channel::oneshot, FutureExt, Stream, StreamExt};
use itertools::Itertools;
use message::{InboundRouterMessage, OutboundRouterMessage};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable, PubKey,
    },
    signing_domain,
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
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, debug_span, error, trace, warn};
use udp::GroupId;
use util::{
    BuildTarget, Collector, EpochValidators, FullNodes, Group, PeerAddrLookup, ReBroadcastGroupMap,
    Recipient, Redundancy, UdpMessage,
};

use crate::{
    metrics::{GAUGE_RAPTORCAST_TOTAL_MESSAGES_RECEIVED, GAUGE_RAPTORCAST_TOTAL_RECV_ERRORS},
    packet::RetrofitResult as _,
    raptorcast_secondary::{
        group_message::FullNodesGroupMessage, SecondaryOutboundMessage,
        SecondaryRaptorCastModeConfig,
    },
};

pub mod auth;
pub mod config;
pub mod decoding;
pub mod message;
pub mod metrics;
pub mod packet;
pub mod parser;
pub mod raptorcast_secondary;
pub mod udp;
pub mod util;

const SIGNATURE_SIZE: usize = 65;
const DEFAULT_RETRY_ATTEMPTS: u64 = 3;

pub const UNICAST_MSG_BATCH_SIZE: usize = 32;

pub(crate) type OwnedMessageBuilder<ST> = packet::MessageBuilder<'static, ST>;

pub struct RaptorCast<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    signing_key: Arc<ST::KeyPairType>,
    is_dynamic_fullnode: bool,

    epoch_validators: BTreeMap<Epoch, EpochValidators<CertificateSignaturePubKey<ST>>>,
    rebroadcast_map: ReBroadcastGroupMap<CertificateSignaturePubKey<ST>>,

    dedicated_full_nodes: FullNodes<CertificateSignaturePubKey<ST>>,
    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,
    message_builder: OwnedMessageBuilder<ST>,
    secondary_message_builder: Option<OwnedMessageBuilder<ST>>,

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

        let redundancy = Redundancy::from_f32(config.primary_instance.raptor10_redundancy)
            .expect("primary raptor10_redundancy doesn't fit");
        let segment_size = dual_socket.segment_size(config.mtu);
        let message_builder = OwnedMessageBuilder::new(config.shared_key.clone())
            .segment_size(segment_size)
            .group_id(GroupId::Primary(current_epoch))
            .redundancy(redundancy);

        let secondary_redundancy = Redundancy::from_f32(
            config
                .secondary_instance
                .raptor10_fullnode_redundancy_factor,
        )
        .expect("secondary raptor10_redundancy doesn't fit");
        let secondary_message_builder = OwnedMessageBuilder::new(config.shared_key.clone())
            .segment_size(segment_size)
            .group_id(GroupId::Primary(current_epoch))
            .redundancy(secondary_redundancy);

        Self {
            is_dynamic_fullnode,
            epoch_validators: Default::default(),
            rebroadcast_map: ReBroadcastGroupMap::new(self_id),
            dedicated_full_nodes: FullNodes::new(
                config.primary_instance.fullnode_dedicated.clone(),
            ),
            peer_discovery_driver,

            signing_key: config.shared_key.clone(),
            message_builder,
            secondary_message_builder: Some(secondary_message_builder),

            current_epoch,

            udp_state: udp::UdpState::new(
                self_id,
                config.udp_message_max_age_ms,
                config.sig_verification_rate_limit,
            ),

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
        self.dedicated_full_nodes = FullNodes::new(nodes);
    }

    pub fn get_rebroadcast_groups(&self) -> &ReBroadcastGroupMap<CertificateSignaturePubKey<ST>> {
        &self.rebroadcast_map
    }

    pub fn is_connected_to(
        &self,
        socket_addr: &SocketAddr,
        public_key: &CertificateSignaturePubKey<ST>,
    ) -> bool {
        self.dual_socket
            .is_connected_socket_and_public_key(socket_addr, public_key)
    }

    fn enqueue_message_to_self(
        message: OM,
        pending_events: &mut VecDeque<RaptorCastEvent<M::Event, ST>>,
        waker: &mut Option<Waker>,
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) {
        let message: M = message.into();
        pending_events.push_back(RaptorCastEvent::Message(message.event(self_id)));
        if let Some(waker) = waker.take() {
            waker.wake()
        }
    }

    fn tcp_build_and_send(
        &mut self,
        to: &NodeId<CertificateSignaturePubKey<ST>>,
        make_app_message: impl FnOnce() -> Bytes,
        completion: Option<oneshot::Sender<()>>,
    ) {
        match self.peer_discovery_driver.lock().unwrap().get_addr(to) {
            None => {
                warn!(
                    ?to,
                    "RaptorCastPrimary TcpPointToPoint not sending message, address unknown"
                );
            }
            Some(address) => {
                let app_message = make_app_message();
                // TODO make this more sophisticated
                // include timestamp, etc
                let mut signed_message = BytesMut::zeroed(SIGNATURE_SIZE + app_message.len());
                let signature = <ST as CertificateSignature>::serialize(&ST::sign::<
                    signing_domain::RaptorcastAppMessage,
                >(
                    &app_message,
                    &self.signing_key,
                ));
                assert_eq!(signature.len(), SIGNATURE_SIZE);
                signed_message[..SIGNATURE_SIZE].copy_from_slice(&signature);
                signed_message[SIGNATURE_SIZE..].copy_from_slice(&app_message);
                self.tcp_writer.write(
                    address,
                    TcpMsg {
                        msg: signed_message.freeze(),
                        completion,
                    },
                );
            }
        };
    }

    fn handle_secondary_outbound_message(
        &mut self,
        outbound_msg: SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>,
    ) {
        let Some(builder) = self.secondary_message_builder.as_mut() else {
            error!("secondary_message_builder not configured");
            return;
        };

        let mut sink = DualUdpPacketSender::new(&mut self.dual_socket, &self.peer_discovery_driver);

        match outbound_msg {
            SecondaryOutboundMessage::SendSingle {
                msg_bytes,
                dest,
                group_id,
            } => {
                trace!(
                    ?dest,
                    msg_len = msg_bytes.len(),
                    "raptorcastprimary handling single message from secondary"
                );
                let build_target = BuildTarget::PointToPoint(&dest);
                builder
                    .prepare()
                    .group_id(group_id)
                    .build_into(&msg_bytes, &build_target, &mut sink)
                    .unwrap_log_on_error(&msg_bytes, &build_target)
            }
            SecondaryOutboundMessage::SendToGroup {
                msg_bytes,
                group,
                group_id,
            } => {
                trace!(
                    group_size = group.size_excl_self(),
                    msg_len = msg_bytes.len(),
                    "raptorcastprimary handling group message from secondary"
                );
                if group.size_excl_self() < 1 {
                    return;
                }
                let build_target = BuildTarget::FullNodeRaptorCast(&group);
                builder
                    .prepare()
                    .group_id(group_id)
                    .build_into(&msg_bytes, &build_target, &mut sink)
                    .unwrap_log_on_error(&msg_bytes, &build_target)
            }
        };
    }

    fn handle_publish(
        &mut self,
        target: RouterTarget<CertificateSignaturePubKey<ST>>,
        message: OM,
        priority: UdpPriority,
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) {
        let _span = debug_span!("router publish").entered();
        let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
            warn!(?elapsed, "long time to publish message")
        });

        match target {
            RouterTarget::Broadcast(epoch) | RouterTarget::Raptorcast(epoch) => {
                let Some(epoch_validators) = self.epoch_validators.get(&epoch) else {
                    error!(
                        "don't have epoch validators populated for epoch: {:?}",
                        epoch
                    );
                    return;
                };

                if epoch_validators.validators.is_member(&self_id) {
                    Self::enqueue_message_to_self(
                        message.clone(),
                        &mut self.pending_events,
                        &mut self.waker,
                        self_id,
                    );
                }

                let epoch_validators_without_self = epoch_validators.view_without(vec![&self_id]);

                if epoch_validators_without_self.is_empty() {
                    return;
                }

                let build_target = match &target {
                    RouterTarget::Broadcast(_) => {
                        BuildTarget::Broadcast(epoch_validators_without_self.into())
                    }
                    RouterTarget::Raptorcast(_) => {
                        BuildTarget::Raptorcast(epoch_validators_without_self)
                    }
                    _ => unreachable!(),
                };
                let outbound_message =
                    match OutboundRouterMessage::<OM, ST>::AppMessage(message).try_serialize() {
                        Ok(msg) => msg,
                        Err(err) => {
                            error!(?err, "failed to serialize a message");
                            return;
                        }
                    };

                let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                    warn!(
                        ?elapsed,
                        app_msg_len = outbound_message.len(),
                        "long time to build raptorcast/broadcast message"
                    )
                });

                let mut sink =
                    DualUdpPacketSender::new(&mut self.dual_socket, &self.peer_discovery_driver)
                        .with_priority(priority);
                self.message_builder
                    .prepare()
                    .group_id(GroupId::Primary(epoch))
                    .build_into(&outbound_message, &build_target, &mut sink)
                    .unwrap_log_on_error(&outbound_message, &build_target);
            }

            RouterTarget::PointToPoint(to) => {
                if to == self_id {
                    Self::enqueue_message_to_self(
                        message,
                        &mut self.pending_events,
                        &mut self.waker,
                        self_id,
                    );
                } else {
                    let outbound_message = match OutboundRouterMessage::<OM, ST>::AppMessage(
                        message,
                    )
                    .try_serialize()
                    {
                        Ok(msg) => msg,
                        Err(err) => {
                            error!(?err, "failed to serialize a message");
                            return;
                        }
                    };
                    let build_target = BuildTarget::PointToPoint(&to);

                    let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                        warn!(
                            ?elapsed,
                            app_msg_len = outbound_message.len(),
                            "long time to build point-to-point message"
                        )
                    });

                    let mut sink = DualUdpPacketSender::new(
                        &mut self.dual_socket,
                        &self.peer_discovery_driver,
                    )
                    .with_priority(priority);
                    self.message_builder
                        .prepare()
                        .group_id(GroupId::Primary(self.current_epoch))
                        .build_into(&outbound_message, &build_target, &mut sink)
                        .unwrap_log_on_error(&outbound_message, &build_target);
                }
            }

            RouterTarget::TcpPointToPoint { to, completion } => {
                if to == self_id {
                    Self::enqueue_message_to_self(
                        message,
                        &mut self.pending_events,
                        &mut self.waker,
                        self_id,
                    );
                } else {
                    let app_message = OutboundRouterMessage::<OM, ST>::AppMessage(message);
                    match app_message.try_serialize() {
                        Ok(serialized) => self.tcp_build_and_send(&to, || serialized, completion),
                        Err(err) => {
                            error!(?err, "failed to serialize a message");
                        }
                    }
                }
            }
        };
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
        let self_id = NodeId::new(self.signing_key.pubkey());

        for command in commands {
            match command {
                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    if self.current_epoch < epoch {
                        trace!(?epoch, ?round, "RaptorCast UpdateCurrentRound");

                        {
                            let pd_driver = self.peer_discovery_driver.lock().unwrap();
                            let added: Vec<_> = self
                                .epoch_validators
                                .get(&epoch)
                                .into_iter()
                                .flat_map(|val| iter_ips(val, &*pd_driver))
                                .collect();
                            let removed: Vec<_> = self
                                .epoch_validators
                                .get(&self.current_epoch)
                                .into_iter()
                                .flat_map(|val| iter_ips(val, &*pd_driver))
                                .collect();
                            drop(pd_driver);
                            self.dataplane_control.update_trusted(added, removed);
                        }

                        self.current_epoch = epoch;
                        self.message_builder.set_group_id(GroupId::Primary(epoch));
                        if let Some(secondary_mb) = self.secondary_message_builder.as_mut() {
                            secondary_mb.set_group_id(GroupId::Primary(epoch));
                        }

                        while let Some(entry) = self.epoch_validators.first_entry() {
                            if *entry.key() + Epoch(1) < self.current_epoch {
                                entry.remove();
                            } else {
                                break;
                            }
                        }
                    }
                    self.rebroadcast_map.delete_expired_groups(epoch, round);
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
                    self.rebroadcast_map
                        .push_group_validator_set(validator_set.clone(), epoch);
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.len());

                        assert!(validator_set
                            .iter()
                            .all(|(validator_key, validator_stake)| {
                                epoch_validators.get(validator_key) == Some(*validator_stake)
                            }));

                        warn!("duplicate validator set update (this is safe but unexpected)")
                    } else {
                        // SAFETY: the validator_set comes from
                        // ValidatorSetData, which should not have
                        // duplicates or invalid entries.
                        let validators = ValidatorSet::new_unchecked(
                            validator_set.clone().into_iter().collect(),
                        );
                        let removed = self
                            .epoch_validators
                            .insert(epoch, EpochValidators { validators });
                        assert!(removed.is_none());
                    }
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdateValidatorSet {
                            epoch,
                            validators: validator_set.iter().map(|(id, _)| *id).collect(),
                        },
                    );
                }
                RouterCommand::Publish { target, message } => {
                    self.handle_publish(target, message, UdpPriority::Regular, self_id);
                }
                RouterCommand::PublishWithPriority {
                    target,
                    message,
                    priority,
                } => {
                    self.handle_publish(target, message, priority, self_id);
                }
                RouterCommand::PublishToFullNodes {
                    epoch,
                    round: _,
                    message,
                } => {
                    let full_nodes_view = self.dedicated_full_nodes.view();
                    if self.is_dynamic_fullnode {
                        debug!("self is dynamic full node, skipping publishing to full nodes");
                        continue;
                    }

                    // self as a dedicated full node will have empty
                    // full_nodes_view, so it won't attempt to
                    // publish.
                    if full_nodes_view.is_empty() {
                        debug!("full_nodes view empty, skipping publishing to full nodes");
                        continue;
                    }

                    let app_message =
                        OutboundRouterMessage::<OM, ST>::AppMessage(message).try_serialize();
                    let outbound_message = match app_message {
                        Ok(msg) => msg,
                        Err(err) => {
                            error!(?err, "failed to serialize a message");
                            continue;
                        }
                    };

                    let node_addrs = self
                        .peer_discovery_driver
                        .lock()
                        .unwrap()
                        .get_known_addresses();

                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        warn!(
                            ?elapsed,
                            app_msg_len = outbound_message.len(),
                            "long time to build message"
                        )
                    });

                    for node in full_nodes_view.iter() {
                        if !node_addrs.contains_key(node) {
                            continue;
                        }

                        let build_target = BuildTarget::PointToPoint(node);
                        let mut sink = DualUdpPacketSender::new(
                            &mut self.dual_socket,
                            &self.peer_discovery_driver,
                        );
                        self.message_builder
                            .prepare()
                            .group_id(GroupId::Primary(epoch))
                            .build_into(&outbound_message, &build_target, &mut sink)
                            .unwrap_log_on_error(&outbound_message, &build_target);
                    }
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
                    let full_nodes = self.dedicated_full_nodes.list.clone();
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
                    self.dedicated_full_nodes.list = dedicated_full_nodes;
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(self.metrics.as_ref())
            .push(self.peer_discovery_metrics.as_ref())
            .push(self.udp_state.metrics().executor_metrics())
            .chain(self.udp_state.decoder_metrics())
            .chain(self.dual_socket.metrics())
    }
}

fn iter_ips<'a, ST: CertificateSignatureRecoverable, PD: PeerDiscoveryAlgo<SignatureType = ST>>(
    validators: &'a EpochValidators<CertificateSignaturePubKey<ST>>,
    peer_discovery: &'a PeerDiscoveryDriver<PD>,
) -> impl Iterator<Item = IpAddr> + 'a {
    validators
        .validators
        .get_members()
        .iter()
        .filter_map(|(node_id, _)| peer_discovery.get_addr(node_id))
        .map(|socket| socket.ip())
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

            // Enter the received raptorcast chunk into the udp_state for reassembly.
            // If the field "first-hop recipient" in the chunk has our node Id, then
            // we are responsible for broadcasting this chunk to other validators.
            // Once we have enough (redundant) raptorcast chunks, recreate the
            // decoded (AKA parsed, original) message.
            // Stream the chunks to our dedicated full-nodes as we receive them.
            let src_addr = message.src_addr;
            let decoded_app_messages = {
                this.udp_state.handle_message(
                    &this.rebroadcast_map,
                    &this.epoch_validators,
                    |targets, payload, bcast_stride| {
                        for target in targets {
                            rebroadcast_packet(
                                &mut this.dual_socket,
                                &this.peer_discovery_driver,
                                &target,
                                payload.clone(),
                                bcast_stride,
                            );
                        }
                    },
                    message,
                )
            };

            trace!(
                "RaptorCastPrimary rx decoded {} messages, sized: {:?}",
                decoded_app_messages.len(),
                decoded_app_messages
                    .iter()
                    .map(|(_node_id, bytes)| bytes.len())
                    .collect_vec()
            );

            for (from, decoded) in decoded_app_messages {
                match InboundRouterMessage::<M, ST>::try_deserialize(&decoded) {
                    Ok(inbound) => match inbound {
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
                            this.peer_discovery_driver
                                .lock()
                                .unwrap()
                                .update(peer_disc_message.event_with_source(from, src_addr));
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
                                        this.epoch_validators.get(&this.current_epoch)
                                    else {
                                        warn!(
                                            "No validators found for current epoch: {:?}",
                                            this.current_epoch
                                        );
                                        continue;
                                    };

                                    if !validate_group_message_sender(
                                        &from,
                                        &full_nodes_group_message,
                                        current_epoch_validators,
                                    ) {
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
                    },
                    Err(err) => {
                        warn!(
                            ?from,
                            ?err,
                            decoded = hex::encode(&decoded),
                            "failed to deserialize message"
                        );
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
            // check message length to prevent panic during message slicing
            if payload.len() < SIGNATURE_SIZE {
                warn!(
                    ?src_addr,
                    "invalid message, message length less than signature size"
                );
                this.dataplane_control.disconnect(src_addr);
                continue;
            }
            let signature_bytes = &payload[..SIGNATURE_SIZE];
            let signature = match <ST as CertificateSignature>::deserialize(signature_bytes) {
                Ok(signature) => signature,
                Err(err) => {
                    warn!(?err, ?src_addr, "invalid signature");
                    this.dataplane_control.disconnect(src_addr);
                    continue;
                }
            };
            let app_message_bytes = payload.slice(SIGNATURE_SIZE..);
            let deserialized_message =
                match InboundRouterMessage::<M, ST>::try_deserialize(&app_message_bytes) {
                    Ok(message) => message,
                    Err(err) => {
                        warn!(?err, ?src_addr, "failed to deserialize message");
                        this.dataplane_control.disconnect(src_addr);
                        continue;
                    }
                };
            let from = match signature
                .recover_pubkey::<signing_domain::RaptorcastAppMessage>(app_message_bytes.as_ref())
            {
                Ok(from) => from,
                Err(err) => {
                    warn!(?err, ?src_addr, "failed to recover pubkey");
                    this.dataplane_control.disconnect(src_addr);
                    continue;
                }
            };

            // Dispatch messages received via TCP
            match deserialized_message {
                InboundRouterMessage::AppMessage(message) => {
                    return Poll::Ready(Some(
                        RaptorCastEvent::Message(message.event(NodeId::new(from))).into(),
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

                    let build_target =
                        BuildTarget::<CertificateSignaturePubKey<ST>>::PointToPoint(&target);

                    let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                        warn!(
                            ?elapsed,
                            app_msg_len = router_message.len(),
                            "long time to build discovery message"
                        )
                    });

                    match target_name_record {
                        Some(name_record) => {
                            let mut sink = DualUdpPacketSender::new(
                                &mut this.dual_socket,
                                &this.peer_discovery_driver,
                            )
                            .with_target_name_record(&target, &name_record);
                            this.message_builder
                                .prepare()
                                .group_id(GroupId::Primary(this.current_epoch))
                                .build_into(&router_message, &build_target, &mut sink)
                                .unwrap_log_on_error(&router_message, &build_target);
                        }
                        None => {
                            let mut sink = DualUdpPacketSender::new(
                                &mut this.dual_socket,
                                &this.peer_discovery_driver,
                            );
                            this.message_builder
                                .prepare()
                                .group_id(GroupId::Primary(this.current_epoch))
                                .build_into(&router_message, &build_target, &mut sink)
                                .unwrap_log_on_error(&router_message, &build_target);
                        }
                    }
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
                        this.rebroadcast_map.push_group_fullnodes(group);
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

fn validate_group_message_sender<ST>(
    sender: &NodeId<CertificateSignaturePubKey<ST>>,
    group_message: &FullNodesGroupMessage<ST>,
    epoch_validators: &EpochValidators<CertificateSignaturePubKey<ST>>,
) -> bool
where
    ST: CertificateSignatureRecoverable,
{
    match group_message {
        // Prepare group message should originate from a validator
        FullNodesGroupMessage::PrepareGroup(msg) => {
            &msg.validator_id == sender && epoch_validators.validators.is_member(sender)
        }
        FullNodesGroupMessage::PrepareGroupResponse(msg) => &msg.node_id == sender,
        FullNodesGroupMessage::ConfirmGroup(msg) => &msg.prepare.validator_id == sender,
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
    target_name_record: Option<TargetNameRecord<'a, ST>>,
    targets: HashSet<Recipient<CertificateSignaturePubKey<ST>>>,
    priority: UdpPriority,
    _signature_type: PhantomData<ST>,
}

#[derive(Clone, Copy)]
struct TargetNameRecord<'a, ST: CertificateSignatureRecoverable> {
    target: &'a NodeId<CertificateSignaturePubKey<ST>>,
    name_record: &'a NameRecord,
}

impl<'a, ST, PD, AP> DualUdpPacketSender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn new(
        dual_socket: &'a mut auth::DualSocketHandle<AP>,
        peer_disc_driver: &'a Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    ) -> Self {
        Self {
            dual_socket,
            peer_disc_driver,
            target_name_record: None,
            targets: Default::default(),
            priority: UdpPriority::Regular,
            _signature_type: PhantomData,
        }
    }

    fn with_priority(mut self, priority: UdpPriority) -> DualUdpPacketSender<'a, ST, PD, AP> {
        self.priority = priority;
        self
    }

    fn with_target_name_record(
        mut self,
        target: &'a NodeId<CertificateSignaturePubKey<ST>>,
        name_record: &'a NameRecord,
    ) -> DualUdpPacketSender<'a, ST, PD, AP> {
        // currently we have no use for multiple target name records
        debug_assert!(self.target_name_record.is_none());
        self.target_name_record = Some(TargetNameRecord {
            target,
            name_record,
        });
        self
    }

    fn lookup_addr(
        &self,
        recipient: &Recipient<CertificateSignaturePubKey<ST>>,
    ) -> Option<SocketAddr> {
        if let Some(target_name_record) = self.target_name_record {
            if recipient.node_id() != target_name_record.target {
                return None;
            }

            if let Some(auth_addr) = target_name_record.name_record.authenticated_udp_socket() {
                let addr = SocketAddr::V4(auth_addr);
                if self
                    .dual_socket
                    .is_connected_socket_and_public_key(&addr, &recipient.node_id().pubkey())
                {
                    return Some(addr);
                }
            }

            Some(SocketAddr::V4(target_name_record.name_record.udp_socket()))
        } else {
            // otherwise lookup address using peer-discovery
            let peer_lookup = (&*self.dual_socket, self.peer_disc_driver);
            *recipient.lookup(&peer_lookup)
        }
    }
}

impl<'a, ST, PD, AP> Drop for DualUdpPacketSender<'a, ST, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: auth::AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn drop(&mut self) {
        if let Some(target_name_record) = self.target_name_record {
            if let Some(auth_addr) = target_name_record.name_record.authenticated_udp_socket() {
                let addr = SocketAddr::V4(auth_addr);
                if !self
                    .dual_socket
                    .is_connected_socket_and_public_key(&addr, &target_name_record.target.pubkey())
                {
                    if let Err(e) = self.dual_socket.connect(
                        &target_name_record.target.pubkey(),
                        addr,
                        DEFAULT_RETRY_ATTEMPTS,
                    ) {
                        warn!(
                            target = ?target_name_record.target,
                            auth_addr = ?auth_addr,
                            error = ?e,
                            "failed to initiate connection to authenticated endpoint"
                        );
                    }
                    self.dual_socket.flush();
                }
            }
        } else {
            let targets = self.targets.iter().map(|recipient| recipient.node_id());
            ensure_authenticated_sessions(self.dual_socket, self.peer_disc_driver, targets);
        }
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
        let Some(dest) = self.lookup_addr(&item.recipient) else {
            return;
        };

        if !self.targets.contains(&item.recipient) {
            // used to initiate auth udp session
            self.targets.insert(item.recipient.clone());
        }

        let msg = UnicastMsg {
            stride: item.stride as u16,
            msgs: vec![(dest, item.payload)],
        };

        self.dual_socket
            .write_unicast_with_priority(msg, self.priority);
    }
}

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
