use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future as _,
    marker::PhantomData,
    net::{SocketAddr, SocketAddrV4},
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
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{
    udp::{segment_size_for_mtu, DEFAULT_MTU},
    BroadcastMsg, Dataplane, DataplaneBuilder, RecvTcpMsg, RecvUdpMsg, TcpMsg, UnicastMsg,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ControlPanelEvent, GetFullNodes, GetPeers, Message, MonadEvent, PeerEntry, RouterCommand,
};
use monad_peer_discovery::{
    driver::{PeerDiscoveryDriver, PeerDiscoveryEmit},
    mock::{NopDiscovery, NopDiscoveryBuilder},
    PeerDiscoveryAlgo, PeerDiscoveryEvent,
};
use monad_types::{DropTimer, Epoch, ExecutionProtocol, NodeId, RouterTarget};
use raptorcast_secondary::group_message::FullNodesGroupMessage;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, warn};
use util::{
    parse_redundancy_config, BuildTarget, EpochValidators, FullNodes, Group, ReBroadcastGroupMap,
    Redundancy, Validator,
};

pub mod config;
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
    p2p_redundancy: Redundancy,
    is_fullnode: bool,

    // Raptorcast group with stake information. For the send side (i.e., initiating proposals)
    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    rebroadcast_map: ReBroadcastGroupMap<ST>,

    dedicated_full_nodes: FullNodes<CertificateSignaturePubKey<ST>>,
    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,
    mtu: u16,

    dataplane: Arc<Mutex<Dataplane>>,
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
        dataplane: Arc<Mutex<Dataplane>>,
        peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    ) -> Self {
        let self_id = NodeId::new(config.shared_key.pubkey());
        let is_fullnode = matches!(
            config.secondary_instance.mode,
            config::SecondaryRaptorCastModeConfig::Client(_)
        );
        tracing::trace!(
            ?is_fullnode, ?self_id, ?config.mtu, "RaptorCast::new",
        );
        let redundancy = parse_redundancy_config(
            config.primary_instance.default_redundancy,
            "primary_instance.default_redundancy",
        );
        let p2p_redundancy = parse_redundancy_config(
            config.primary_instance.p2p_redundancy,
            "primary_instance.p2p_redundancy",
        );
        Self {
            is_fullnode,
            epoch_validators: Default::default(),
            rebroadcast_map: ReBroadcastGroupMap::new(self_id, is_fullnode),
            dedicated_full_nodes: FullNodes::new(
                config.primary_instance.fullnode_dedicated.clone(),
            ),
            peer_discovery_driver,

            signing_key: config.shared_key.clone(),
            redundancy,
            p2p_redundancy,

            current_epoch: Epoch(0),

            udp_state: udp::UdpState::new(self_id, config.udp_message_max_age_ms),
            mtu: config.mtu,

            dataplane,
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
        mut self,
        channel_to_secondary: UnboundedSender<FullNodesGroupMessage<ST>>,
        channel_from_secondary: UnboundedReceiver<Group<ST>>,
    ) -> Self {
        self.channel_to_secondary = Some(channel_to_secondary);
        if self.is_fullnode {
            self.channel_from_secondary = Some(channel_from_secondary);
        }
        self
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
                let signature = ST::sign(&app_message, &self.signing_key).serialize();
                assert_eq!(signature.len(), SIGNATURE_SIZE);
                signed_message[..SIGNATURE_SIZE].copy_from_slice(&signature);
                signed_message[SIGNATURE_SIZE..].copy_from_slice(&app_message);
                self.dataplane.lock().unwrap().tcp_write(
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

        let unix_ts_ms = std::time::UNIX_EPOCH
            .elapsed()
            .expect("time went backwards")
            .as_millis()
            .try_into()
            .expect("unix epoch doesn't fit in u64");

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
    let dp_builder = DataplaneBuilder::new(&local_addr, up_bandwidth_mbps);
    let shared_dataplane = Arc::new(Mutex::new(dp_builder.build()));
    let config = config::RaptorCastConfig {
        shared_key,
        mtu: DEFAULT_MTU,
        udp_message_max_age_ms: u64::MAX, // No timestamp validation for tests
        primary_instance: Default::default(),
        secondary_instance: config::RaptorCastConfigSecondary {
            default_redundancy: 2.0,
            mode: config::SecondaryRaptorCastModeConfig::None,
        },
    };
    let pd = PeerDiscoveryDriver::new(peer_discovery_builder);
    let shared_pd = Arc::new(Mutex::new(pd));
    RaptorCast::<ST, M, OM, SE, NopDiscovery<ST>>::new(config, shared_dataplane, shared_pd)
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
                        tracing::trace!(?epoch, ?round, "RaptorCast UpdateCurrentRound");
                        self.current_epoch = epoch;
                        self.rebroadcast_map.delete_expired_groups(epoch, round);
                        while let Some(entry) = self.epoch_validators.first_entry() {
                            if *entry.key() + Epoch(1) < self.current_epoch {
                                entry.remove();
                            } else {
                                break;
                            }
                        }
                        self.peer_discovery_driver
                            .lock()
                            .unwrap()
                            .update(PeerDiscoveryEvent::UpdateCurrentRound { round, epoch });
                    }
                }
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    tracing::trace!(?epoch, ?validator_set, "RaptorCast AddEpochValidatorSet");
                    self.rebroadcast_map
                        .push_group_validator_set(validator_set.clone(), epoch);
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.validators.len());
                        assert!(validator_set.clone().into_iter().all(
                            |(validator_key, validator_stake)| epoch_validators
                                .validators
                                .get(&validator_key)
                                .map(|v| v.stake)
                                == Some(validator_stake)
                        ));
                        warn!("duplicate validator set update (this is safe but unexpected)")
                    } else {
                        let removed = self.epoch_validators.insert(
                            epoch,
                            EpochValidators {
                                validators: validator_set
                                    .clone()
                                    .into_iter()
                                    .map(|(validator_key, validator_stake)| {
                                        (
                                            validator_key,
                                            Validator {
                                                stake: validator_stake,
                                            },
                                        )
                                    })
                                    .collect(),
                            },
                        );
                        assert!(removed.is_none());
                    }
                    self.peer_discovery_driver.lock().unwrap().update(
                        PeerDiscoveryEvent::UpdateValidatorSet {
                            epoch,
                            validators: validator_set.into_iter().map(|(id, _)| id).collect(),
                        },
                    );
                }
                RouterCommand::Publish { target, message } => {
                    tracing::trace!(?target, "RaptorCast Publish AppMessage");
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
                            let full_nodes_view = self.dedicated_full_nodes.view();

                            if epoch_validators_without_self.view().is_empty()
                                && full_nodes_view.view().is_empty()
                            {
                                // this is degenerate case where the only
                                // validator is self and we have no full nodes
                                // to forward
                                continue;
                            }

                            let build_target = match &target {
                                RouterTarget::Broadcast(_) => {
                                    BuildTarget::Broadcast(epoch_validators_without_self)
                                }
                                RouterTarget::Raptorcast(_) => BuildTarget::Raptorcast((
                                    epoch_validators_without_self,
                                    full_nodes_view,
                                )),
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
                            self.dataplane.lock().unwrap().udp_write_unicast(rc_chunks);
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
                                    self.p2p_redundancy,
                                    &known_addresses,
                                );
                                self.dataplane.lock().unwrap().udp_write_unicast(rc_chunks);
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
                RouterCommand::PublishToFullNodes { .. } => {}
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
                RouterCommand::UpdatePeers(peers) => {
                    self.peer_discovery_driver
                        .lock()
                        .unwrap()
                        .update(PeerDiscoveryEvent::UpdatePeers { peers });
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
                RouterCommand::UpdateFullNodes(new_full_nodes) => {
                    self.dedicated_full_nodes.list = new_full_nodes;
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

        let full_node_addrs = this
            .dedicated_full_nodes
            .list
            .iter()
            .filter_map(|node_id| this.peer_discovery_driver.lock().unwrap().get_addr(node_id))
            .collect::<Vec<_>>();

        loop {
            let message: RecvUdpMsg;
            {
                let mut dataplane = this.dataplane.lock().unwrap();
                let Poll::Ready(msg) = pin!(dataplane.udp_read()).poll_unpin(cx) else {
                    break;
                };
                message = msg;
            }

            tracing::trace!(
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
                    &this.rebroadcast_map, // contains the NodeIds for all the RC participants for each epoch
                    |targets, payload, bcast_stride| {
                        // Callback for re-broadcasting raptorcast chunks to other RaptorCast participants (validator peers)
                        let target_addrs: Vec<SocketAddr> = targets
                            .into_iter()
                            .filter_map(|target| {
                                this.peer_discovery_driver.lock().unwrap().get_addr(&target)
                            })
                            .collect();

                        this.dataplane
                            .lock()
                            .unwrap()
                            .udp_write_broadcast(BroadcastMsg {
                                targets: target_addrs,
                                payload,
                                stride: bcast_stride,
                            });
                    },
                    |payload, bcast_stride| {
                        // Callback for forwarding chunks to full nodes
                        this.dataplane
                            .lock()
                            .unwrap()
                            .udp_write_broadcast(BroadcastMsg {
                                targets: full_node_addrs.clone(),
                                payload,
                                stride: bcast_stride,
                            });
                    },
                    message,
                )
            };

            tracing::trace!(
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
                            tracing::trace!("RaptorCastPrimary rx deserialized AppMessage");
                            this.pending_events
                                .push_back(RaptorCastEvent::Message(app_message.event(from)));
                        }
                        InboundRouterMessage::PeerDiscoveryMessage(peer_disc_message) => {
                            tracing::trace!(
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
                            tracing::trace!(
                                "RaptorCastPrimary rx deserialized {:?}",
                                full_nodes_group_message
                            );
                            match &this.channel_to_secondary {
                                Some(channel) => {
                                    if channel.send(full_nodes_group_message).is_err() {
                                        tracing::error!(
                                            "Could not send InboundRouterMessage to \
                                    secondary Raptorcast instance: channel closed",
                                        );
                                    }
                                }
                                None => {
                                    tracing::debug!(
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

        while let Poll::Ready(recv_msg) =
            pin!(this.dataplane.lock().unwrap().tcp_read()).poll_unpin(cx)
        {
            let RecvTcpMsg { payload, src_addr } = recv_msg;
            // check message length to prevent panic during message slicing
            if payload.len() < SIGNATURE_SIZE {
                warn!(
                    ?src_addr,
                    "invalid message, message length less than signature size"
                );
                continue;
            }
            let signature_bytes = &payload[..SIGNATURE_SIZE];
            let signature = match ST::deserialize(signature_bytes) {
                Ok(signature) => signature,
                Err(err) => {
                    warn!(?err, ?src_addr, "invalid signature");
                    continue;
                }
            };
            let app_message_bytes = payload.slice(SIGNATURE_SIZE..);
            let deserialized_message =
                match InboundRouterMessage::<M, ST>::try_deserialize(&app_message_bytes) {
                    Ok(message) => message,
                    Err(err) => {
                        warn!(?err, ?src_addr, "failed to deserialize message");
                        continue;
                    }
                };
            let from = match signature.recover_pubkey(app_message_bytes.as_ref()) {
                Ok(from) => from,
                Err(err) => {
                    warn!(?err, ?src_addr, "failed to recover pubkey");
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
                    continue;
                }
                InboundRouterMessage::FullNodesGroup(_group_message) => {
                    // pass TCP message to MultiRouter
                    warn!("FullNodesGroup protocol via TCP not implemented");
                    continue;
                }
            }
        }

        {
            let mut pd_driver = this.peer_discovery_driver.lock().unwrap();
            while let Poll::Ready(Some(peer_disc_emit)) = pd_driver.poll_next_unpin(cx) {
                match peer_disc_emit {
                    PeerDiscoveryEmit::RouterCommand { target, message } => {
                        let router_message =
                            match OutboundRouterMessage::<OM, ST>::PeerDiscoveryMessage(message)
                                .try_serialize()
                            {
                                Ok(msg) => msg,
                                Err(err) => {
                                    error!(?err, "failed to serialize peer discovery message");
                                    continue;
                                }
                            };
                        let current_epoch = this.current_epoch;
                        let unicast_msg = Self::udp_build(
                            &current_epoch,
                            BuildTarget::<ST>::PointToPoint(&target),
                            router_message,
                            this.mtu,
                            &this.signing_key,
                            this.redundancy,
                            &pd_driver.get_known_addresses(),
                        );
                        this.dataplane
                            .lock()
                            .unwrap()
                            .udp_write_unicast(unicast_msg);
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
                        tracing::error!("RaptorCast secondary->primary channel disconnected.");
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
        }
    }
}
