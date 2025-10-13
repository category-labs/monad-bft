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
    collections::{BTreeMap, HashMap, VecDeque},
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
        CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_dataplane::{
    udp::{segment_size_for_mtu, DEFAULT_MTU},
    BroadcastMsg, DataplaneBuilder, DataplaneReader, DataplaneWriter, RecvTcpMsg, TcpMsg,
    UnicastMsg,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    BlockSyncEvent, ControlPanelEvent, GetFullNodes, GetPeers, Message, MonadEvent, PeerEntry,
    RouterCommand,
};
use monad_node_config::{FullNodeConfig, FullNodeRaptorCastConfig};
use monad_peer_discovery::{
    driver::{PeerDiscoveryDriver, PeerDiscoveryEmit},
    message::PeerDiscoveryMessage,
    mock::{NopDiscovery, NopDiscoveryBuilder},
    PeerDiscoveryAlgo, PeerDiscoveryEvent,
};
use monad_types::{DropTimer, Epoch, ExecutionProtocol, NodeId, Round, RouterTarget};
use monad_validator::signature_collection::SignatureCollection;
use raptorcast_secondary::{group_message::FullNodesGroupMessage, SecondaryRaptorCastModeConfig};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, debug_span, error, trace, warn};
use util::{
    unix_ts_ms_now, BuildTarget, EpochValidators, FullNodes, Group, ReBroadcastGroupMap, Redundancy,
};

pub mod config;
pub mod decoding;
pub mod message;
pub mod raptorcast_secondary;
pub mod udp;
pub mod util;

const SIGNATURE_SIZE: usize = 65;

pub struct RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    signing_key: Arc<ST::KeyPairType>,
    redundancy: Redundancy,
    is_dynamic_fullnode: bool,

    // Raptorcast group with stake information. For the send side (i.e., initiating proposals)
    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    rebroadcast_map: ReBroadcastGroupMap<ST>,

    dedicated_full_nodes: FullNodes<CertificateSignaturePubKey<ST>>,
    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,
    mtu: u16,

    dataplane_reader: DataplaneReader,
    dataplane_writer: DataplaneWriter,
    pending_events: VecDeque<RaptorCastEvent<M::Event, ST>>,

    channel_to_secondary: Option<UnboundedSender<FullNodesGroupMessage<ST>>>,
    channel_from_secondary: Option<UnboundedReceiver<Group<ST>>>,

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
    SecondaryRaptorcastPeersUpdate(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
}

impl<ST, M, OM, SE, PD> RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    pub fn new(
        config: config::RaptorCastConfig<ST>,
        secondary_mode: SecondaryRaptorCastModeConfig,
        dataplane_reader: DataplaneReader,
        dataplane_writer: DataplaneWriter,
        peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        current_epoch: Epoch,
    ) -> Self {
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
        Self {
            is_dynamic_fullnode,
            epoch_validators: Default::default(),
            rebroadcast_map: ReBroadcastGroupMap::new(self_id),
            dedicated_full_nodes: FullNodes::new(
                config.primary_instance.fullnode_dedicated.clone(),
            ),
            peer_discovery_driver,

            signing_key: config.shared_key.clone(),
            redundancy: Redundancy::from_f32(config.primary_instance.raptor10_redundancy)
                .expect("primary raptor10_redundancy doesn't fit"),

            current_epoch,

            udp_state: udp::UdpState::new(self_id, config.udp_message_max_age_ms),
            mtu: config.mtu,

            dataplane_reader,
            dataplane_writer,
            pending_events: Default::default(),
            channel_to_secondary: None,
            channel_from_secondary: None,

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
        channel_from_secondary: UnboundedReceiver<Group<ST>>,
    ) {
        self.channel_to_secondary = Some(channel_to_secondary);
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

    pub fn get_rebroadcast_groups(&self) -> &ReBroadcastGroupMap<ST> {
        &self.rebroadcast_map
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
                self.dataplane_writer.tcp_write(
                    address,
                    TcpMsg {
                        msg: signed_message.freeze(),
                        completion,
                    },
                );
            }
        };
    }

    // Prepares an app message (e.g. proposal) to be sent out via UDP, by signing
    // it and then splitting into raptorcast chunks fitting MTU (depending on build_target)
    fn udp_build(
        epoch: &Epoch,
        build_target: BuildTarget<ST>,
        outbound_message: Bytes,
        mtu: u16,
        signing_key: &Arc<ST::KeyPairType>,
        redundancy: Redundancy,
        known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    ) -> UnicastMsg {
        let segment_size = segment_size_for_mtu(mtu);

        let unix_ts_ms = unix_ts_ms_now();

        let messages = udp::build_messages::<ST>(
            signing_key,
            segment_size,
            outbound_message,
            redundancy,
            epoch.0,
            unix_ts_ms,
            build_target,
            known_addresses,
        );

        UnicastMsg {
            msgs: messages,
            stride: segment_size,
        }
    }
}

pub fn new_defaulted_raptorcast_for_tests<ST, M, OM, SE>(
    local_addr: SocketAddr,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    shared_key: Arc<ST::KeyPairType>,
) -> RaptorCast<ST, M, OM, SE, NopDiscovery<ST>>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
{
    let peer_discovery_builder = NopDiscoveryBuilder {
        known_addresses,
        ..Default::default()
    };
    let up_bandwidth_mbps = 1_000;
    let dp = DataplaneBuilder::new(&local_addr, up_bandwidth_mbps).build();
    assert!(dp.block_until_ready(Duration::from_secs(1)));
    let (dp_reader, dp_writer) = dp.split();
    let config = config::RaptorCastConfig {
        shared_key,
        mtu: DEFAULT_MTU,
        udp_message_max_age_ms: u64::MAX, // No timestamp validation for tests
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
    RaptorCast::<ST, M, OM, SE, NopDiscovery<ST>>::new(
        config,
        SecondaryRaptorCastModeConfig::None,
        dp_reader,
        dp_writer,
        shared_pd,
        Epoch(0),
    )
}

impl<ST, M, OM, SE, PD> Executor for RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
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
                            self.dataplane_writer.update_trusted(added, removed);
                        }

                        self.current_epoch = epoch;
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
                        let removed = self.epoch_validators.insert(
                            epoch,
                            EpochValidators {
                                validators: validator_set.clone().into_iter().collect(),
                            },
                        );
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
                    let _span = debug_span!("router publish").entered();
                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        warn!(?elapsed, "long time to publish message")
                    });

                    // TODO: perhaps pass this directly to udp_build to avoid calling on every exec
                    let known_addresses = self
                        .peer_discovery_driver
                        .lock()
                        .unwrap()
                        .get_known_addresses();

                    // send message to self if applicable
                    match target {
                        RouterTarget::Broadcast(epoch) | RouterTarget::Raptorcast(epoch) => {
                            let Some(epoch_validators) = self.epoch_validators.get_mut(&epoch)
                            else {
                                error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            if epoch_validators.validators.contains_key(&self_id) {
                                Self::enqueue_message_to_self(
                                    message.clone(),
                                    &mut self.pending_events,
                                    &mut self.waker,
                                    self_id,
                                );
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);

                            if epoch_validators_without_self.is_empty() {
                                // this is degenerate case where the only
                                // validator is self and we have no full nodes
                                // to forward
                                continue;
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
                                match OutboundRouterMessage::<OM, ST>::AppMessage(message)
                                    .try_serialize()
                                {
                                    Ok(msg) => msg,
                                    Err(err) => {
                                        error!(?err, "failed to serialize a message");
                                        continue;
                                    }
                                };
                            let rc_chunks: UnicastMsg = Self::udp_build(
                                &epoch,
                                build_target,
                                outbound_message,
                                self.mtu,
                                &self.signing_key,
                                self.redundancy,
                                &known_addresses,
                            );
                            self.dataplane_writer.udp_write_unicast(rc_chunks);
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
                                let outbound_message =
                                    match OutboundRouterMessage::<OM, ST>::AppMessage(message)
                                        .try_serialize()
                                    {
                                        Ok(msg) => msg,
                                        Err(err) => {
                                            error!(?err, "failed to serialize a message");
                                            continue;
                                        }
                                    };
                                let rc_chunks: UnicastMsg = Self::udp_build(
                                    &self.current_epoch,
                                    BuildTarget::<ST>::PointToPoint(&to),
                                    outbound_message,
                                    self.mtu,
                                    &self.signing_key,
                                    self.redundancy,
                                    &known_addresses,
                                );
                                self.dataplane_writer.udp_write_unicast(rc_chunks);
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
                                let app_message =
                                    OutboundRouterMessage::<OM, ST>::AppMessage(message);
                                match app_message.try_serialize() {
                                    Ok(serialized) => {
                                        self.tcp_build_and_send(&to, || serialized, completion)
                                    }
                                    Err(err) => {
                                        error!(?err, "failed to serialize a message");
                                    }
                                }
                            }
                        }
                    };
                }
                RouterCommand::PublishToFullNodes { epoch, message } => {
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
                    for node in full_nodes_view.iter() {
                        if !node_addrs.contains_key(node) {
                            continue;
                        }

                        // TODO: optimize the build of copies of the
                        // same message to multiple recipients.
                        let build_target = BuildTarget::PointToPoint(node);
                        let rc_chunks: UnicastMsg = Self::udp_build(
                            &epoch,
                            build_target,
                            outbound_message.clone(),
                            self.mtu,
                            &self.signing_key,
                            self.redundancy,
                            &node_addrs,
                        );
                        self.dataplane_writer.udp_write_unicast(rc_chunks);
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
                        .map(|(node_id, name_record)| PeerEntry {
                            pubkey: node_id.pubkey(),
                            addr: name_record.address(),
                            signature: name_record.signature,
                            record_seq_num: name_record.seq(),
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
                    pinned_nodes,
                } => {
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdatePeers {
                            peers: peer_entries,
                        },
                    );
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdatePinnedNodes {
                            pinned_full_nodes: pinned_nodes.into_iter().collect(),
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

    fn metrics(&self) -> ExecutorMetricsChain {
        // FIXME: avoid copying metrics
        ExecutorMetricsChain::default()
            .push(self.metrics.as_ref())
            .push(self.peer_discovery_metrics.as_ref())
    }
}

fn iter_ips<'a, ST: CertificateSignatureRecoverable, PD: PeerDiscoveryAlgo<SignatureType = ST>>(
    validators: &'a EpochValidators<ST>,
    peer_discovery: &'a PeerDiscoveryDriver<PD>,
) -> impl Iterator<Item = IpAddr> + 'a {
    validators
        .validators
        .iter()
        .filter_map(|(node_id, _)| peer_discovery.get_addr(node_id))
        .map(|socket| socket.ip())
}

impl<ST, M, OM, E, PD> Stream for RaptorCast<ST, M, OM, E, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    PeerDiscoveryDriver<PD>: Unpin,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
            let dataplane = &mut this.dataplane_reader;
            let Poll::Ready(message) = pin!(dataplane.udp_read()).poll_unpin(cx) else {
                break;
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
            let decoded_app_messages = {
                // FIXME: pass dataplane as arg to handle_message
                this.udp_state.handle_message(
                    this.current_epoch,
                    &this.rebroadcast_map, // contains the NodeIds for all the RC participants for each epoch
                    &this.epoch_validators,
                    |targets, payload, bcast_stride| {
                        // Callback for re-broadcasting raptorcast chunks to other RaptorCast participants (validator peers)
                        let target_addrs: Vec<SocketAddr> = targets
                            .into_iter()
                            .filter_map(|target| {
                                this.peer_discovery_driver.lock().unwrap().get_addr(&target)
                            })
                            .collect();

                        this.dataplane_writer.udp_write_broadcast(BroadcastMsg {
                            targets: target_addrs,
                            payload,
                            stride: bcast_stride,
                        });
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
                                    if !validate_group_message_sender(
                                        &from,
                                        &full_nodes_group_message,
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
            let dataplane = &mut this.dataplane_reader;
            let Poll::Ready(msg) = pin!(dataplane.tcp_read()).poll_unpin(cx) else {
                break;
            };
            let RecvTcpMsg { payload, src_addr } = msg;
            // check message length to prevent panic during message slicing
            if payload.len() < SIGNATURE_SIZE {
                warn!(
                    ?src_addr,
                    "invalid message, message length less than signature size"
                );
                this.dataplane_writer.disconnect(src_addr);
                continue;
            }
            let signature_bytes = &payload[..SIGNATURE_SIZE];
            let signature = match <ST as CertificateSignature>::deserialize(signature_bytes) {
                Ok(signature) => signature,
                Err(err) => {
                    warn!(?err, ?src_addr, "invalid signature");
                    this.dataplane_writer.disconnect(src_addr);
                    continue;
                }
            };
            let app_message_bytes = payload.slice(SIGNATURE_SIZE..);
            let deserialized_message =
                match InboundRouterMessage::<M, ST>::try_deserialize(&app_message_bytes) {
                    Ok(message) => message,
                    Err(err) => {
                        warn!(?err, ?src_addr, "failed to deserialize message");
                        this.dataplane_writer.disconnect(src_addr);
                        continue;
                    }
                };
            let from = match signature
                .recover_pubkey::<signing_domain::RaptorcastAppMessage>(app_message_bytes.as_ref())
            {
                Ok(from) => from,
                Err(err) => {
                    warn!(?err, ?src_addr, "failed to recover pubkey");
                    this.dataplane_writer.disconnect(src_addr);
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
                    this.dataplane_writer.disconnect(src_addr);
                    continue;
                }
                InboundRouterMessage::FullNodesGroup(_group_message) => {
                    // pass TCP message to MultiRouter
                    warn!("FullNodesGroup protocol via TCP not implemented");
                    this.dataplane_writer.disconnect(src_addr);
                    continue;
                }
            }
        }

        {
            let mut pd_driver = this.peer_discovery_driver.lock().unwrap();

            let send_peer_disc_msg = |target: NodeId<CertificateSignaturePubKey<ST>>,
                                      message: PeerDiscoveryMessage<ST>,
                                      known_addresses: HashMap<
                NodeId<CertificateSignaturePubKey<ST>>,
                SocketAddr,
            >| {
                let _span = debug_span!("publish discovery").entered();
                let Ok(router_message) =
                    OutboundRouterMessage::<OM, ST>::PeerDiscoveryMessage(message).try_serialize()
                else {
                    error!("failed to serialize peer discovery message");
                    return;
                };

                let unicast_msg = Self::udp_build(
                    &this.current_epoch,
                    BuildTarget::<ST>::PointToPoint(&target),
                    router_message,
                    this.mtu,
                    &this.signing_key,
                    this.redundancy,
                    &known_addresses,
                );
                this.dataplane_writer.udp_write_unicast(unicast_msg);
            };

            while let Poll::Ready(Some(peer_disc_emit)) = pd_driver.poll_next_unpin(cx) {
                match peer_disc_emit {
                    PeerDiscoveryEmit::RouterCommand { target, message } => {
                        send_peer_disc_msg(target, message, pd_driver.get_known_addresses());
                    }
                    PeerDiscoveryEmit::PingPongCommand {
                        target,
                        socket_address,
                        message,
                    } => {
                        let addrs = HashMap::from_iter([(target, SocketAddr::V4(socket_address))]);
                        send_peer_disc_msg(target, message, addrs);
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
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate(confirm_group_peers) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::SecondaryRaptorcastPeersUpdate {
                    confirm_group_peers,
                })
            }
        }
    }
}

fn validate_group_message_sender<ST>(
    sender: &NodeId<CertificateSignaturePubKey<ST>>,
    group_message: &FullNodesGroupMessage<ST>,
) -> bool
where
    ST: CertificateSignatureRecoverable,
{
    match group_message {
        FullNodesGroupMessage::PrepareGroup(msg) => &msg.validator_id == sender,
        FullNodesGroupMessage::PrepareGroupResponse(msg) => &msg.node_id == sender,
        FullNodesGroupMessage::ConfirmGroup(msg) => &msg.prepare.validator_id == sender,
    }
}
