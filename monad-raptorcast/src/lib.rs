use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::{pin, Pin},
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use futures::{channel::oneshot, FutureExt, Stream, StreamExt};
use message::{DeserializeError, InboundRouterMessage, OutboundRouterMessage};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::{
    udp::segment_size_for_mtu, BroadcastMsg, Dataplane, DataplaneBuilder, TcpMsg, UnicastMsg,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ControlPanelEvent, GetFullNodes, GetPeers, Message, MonadEvent, RouterCommand,
};
use monad_peer_discovery::{
    driver::{PeerDiscoveryDriver, PeerDiscoveryEmit},
    PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
};
use monad_types::{
    Deserializable, DropTimer, Epoch, ExecutionProtocol, NodeId, RouterTarget, Serializable,
};
use tracing::{debug, error, warn};

pub mod message;
pub mod udp;
pub mod util;
use util::{BuildTarget, EpochValidators, FullNodes, ReBroadcastGroupMap, Validator};

const SIGNATURE_SIZE: usize = 65;
const PEER_DISCOVERY_ENABLED: bool = false;

pub struct RaptorCastConfig<ST, B>
where
    ST: CertificateSignatureRecoverable,
{
    // TODO support dynamic updating
    pub known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    pub full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    pub peer_discovery_builder: B,

    pub key: ST::KeyPairType,
    /// amount of redundancy to send
    /// a value of 2 == send 2x total payload size total
    pub redundancy: u8,

    pub local_addr: SocketAddr,

    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    pub up_bandwidth_mbps: u64,
    pub mtu: u16,
    pub buffer_size: Option<usize>,
}

pub struct RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    key: ST::KeyPairType,
    redundancy: u8,

    // Raptorcast group with stake information. For the send side (i.e., initiating proposals)
    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    rebroadcast_map: ReBroadcastGroupMap<ST>,

    // TODO support dynamic updating
    full_nodes: FullNodes<CertificateSignaturePubKey<ST>>,
    // TODO: replace known_addresses with peer_discovery_driver
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    peer_discovery_driver: PeerDiscoveryDriver<PD>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,
    mtu: u16,

    dataplane: Dataplane,
    pending_events: VecDeque<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(OM, SE)>,
}

pub enum PeerManagerResponse<P: PubKey> {
    PeerList(Vec<(NodeId<P>, SocketAddr)>),
    FullNodes(Vec<NodeId<P>>),
}

pub enum RaptorCastEvent<E, P: PubKey> {
    Message(E),
    PeerManagerResponse(PeerManagerResponse<P>),
}

impl<ST, M, OM, SE, PD> RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    pub fn new<B>(config: RaptorCastConfig<ST, B>) -> Self
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>,
    {
        assert!(config.redundancy >= 1);
        let self_id = NodeId::new(config.key.pubkey());
        let mut builder = DataplaneBuilder::new(&config.local_addr, config.up_bandwidth_mbps);
        if let Some(buffer_size) = config.buffer_size {
            builder = builder.with_buffer_size(buffer_size);
        }
        let dataplane = builder.build();
        let is_fullnode = false; // This will come from the config in upcoming PR

        Self {
            epoch_validators: Default::default(),
            rebroadcast_map: ReBroadcastGroupMap::new(self_id, is_fullnode),
            full_nodes: FullNodes::new(config.full_nodes),
            known_addresses: config.known_addresses,
            peer_discovery_driver: PeerDiscoveryDriver::new(config.peer_discovery_builder),

            key: config.key,
            redundancy: config.redundancy,

            current_epoch: Epoch(0),

            udp_state: udp::UdpState::new(self_id),
            mtu: config.mtu,

            dataplane,
            pending_events: Default::default(),

            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    fn tcp_build_and_send(
        &mut self,
        to: &NodeId<CertificateSignaturePubKey<ST>>,
        make_app_message: impl FnOnce() -> Bytes,
        completion: Option<oneshot::Sender<()>>,
    ) {
        match self.known_addresses.get(to) {
            None => {
                warn!(?to, "not sending message, address unknown");
            }
            Some(address) => {
                let app_message = make_app_message();
                // TODO make this more sophisticated
                // include timestamp, etc
                let mut signed_message = BytesMut::zeroed(SIGNATURE_SIZE + app_message.len());
                let signature = ST::sign(&app_message, &self.key).serialize();
                assert_eq!(signature.len(), SIGNATURE_SIZE);
                signed_message[..SIGNATURE_SIZE].copy_from_slice(&signature);
                signed_message[SIGNATURE_SIZE..].copy_from_slice(&app_message);
                self.dataplane.tcp_write(
                    *address,
                    TcpMsg {
                        msg: signed_message.freeze(),
                        completion,
                    },
                );
            }
        };
    }
}

impl<ST, M, OM, SE, PD> Executor for RaptorCast<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let self_id = NodeId::new(self.key.pubkey());
        for command in commands {
            match command {
                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    assert!(epoch >= self.current_epoch);
                    self.current_epoch = epoch;
                    self.rebroadcast_map.delete_expired_groups(epoch, round);
                    while let Some(entry) = self.epoch_validators.first_entry() {
                        if *entry.key() + Epoch(1) < self.current_epoch {
                            entry.remove();
                        } else {
                            break;
                        }
                    }
                }
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    self.rebroadcast_map
                        .push_group_validator_set(validator_set.clone(), epoch);
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.validators.len());
                        assert!(validator_set.into_iter().all(
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
                    // TODO: peer discovery to discover new peers
                }
                RouterCommand::Publish { target, message } => {
                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        warn!(?elapsed, "long time to publish message")
                    });

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

                            let app_message = message.serialize();

                            if epoch_validators.validators.contains_key(&self_id) {
                                let message: M = message.into();
                                self.pending_events
                                    .push_back(RaptorCastEvent::Message(message.event(self_id)));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            if epoch_validators_without_self.view().is_empty() {
                                // this is degenerate case where the only validator is self
                                continue;
                            }

                            let full_nodes_view = self.full_nodes.view();

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

                            let unicast_msg = udp_build(
                                &epoch,
                                build_target,
                                app_message,
                                self.mtu,
                                &self.key,
                                self.redundancy,
                                &self.known_addresses,
                            );
                            self.dataplane.udp_write_unicast(unicast_msg);
                        }
                        RouterTarget::PointToPoint(to) => {
                            if to == self_id {
                                let message: M = message.into();
                                self.pending_events
                                    .push_back(RaptorCastEvent::Message(message.event(self_id)));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            } else {
                                let app_message = message.serialize();
                                let unicast_msg = udp_build(
                                    &self.current_epoch,
                                    BuildTarget::<ST>::PointToPoint(&to),
                                    app_message,
                                    self.mtu,
                                    &self.key,
                                    self.redundancy,
                                    &self.known_addresses,
                                );
                                self.dataplane.udp_write_unicast(unicast_msg);
                            }
                        }
                        RouterTarget::TcpPointToPoint { to, completion } => {
                            if to == self_id {
                                let message: M = message.into();
                                self.pending_events
                                    .push_back(RaptorCastEvent::Message(message.event(self_id)));
                                if let Some(waker) = self.waker.take() {
                                    waker.wake()
                                }
                            } else {
                                self.tcp_build_and_send(&to, || message.serialize(), completion)
                            }
                        }
                    };
                }
                RouterCommand::GetPeers => {
                    let peer_list = self
                        .known_addresses
                        .iter()
                        .map(|(node_id, sock_addr)| (*node_id, *sock_addr))
                        .collect::<Vec<_>>();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::PeerList(peer_list),
                        ));
                }
                RouterCommand::UpdatePeers(new_peers) => {
                    self.known_addresses = new_peers.into_iter().collect();
                }
                RouterCommand::GetFullNodes => {
                    let full_nodes = self.full_nodes.list.clone();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::FullNodes(full_nodes),
                        ));
                }
                RouterCommand::UpdateFullNodes(new_full_nodes) => {
                    self.full_nodes.list = new_full_nodes;
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

fn udp_build<ST: CertificateSignatureRecoverable>(
    epoch: &Epoch,
    build_target: BuildTarget<ST>,
    app_message: Bytes,
    mtu: u16,
    key: &ST::KeyPairType,
    redundancy: u8,
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
        key,
        segment_size,
        app_message,
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

fn try_deserialize_router_message<
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
>(
    bytes: &Bytes,
) -> Result<InboundRouterMessage<M, ST>, DeserializeError> {
    // try to deserialize as a new message first
    let Ok(inbound) = InboundRouterMessage::<M, ST>::try_deserialize(bytes) else {
        // if that fails, try to deserialize as an old message instead
        return match M::deserialize(bytes) {
            Ok(old_message) => Ok(InboundRouterMessage::AppMessage(old_message)),
            Err(err) => Err(DeserializeError(format!("{:?}", err))),
        };
    };
    Ok(inbound)
}

impl<ST, M, OM, E, PD> Stream for RaptorCast<ST, M, OM, E, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    PeerDiscoveryDriver<PD>: Unpin,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event.into()));
        }

        let full_node_addrs = this
            .full_nodes
            .list
            .iter()
            .filter_map(|node_id| this.known_addresses.get(node_id).copied())
            .collect::<Vec<_>>();

        loop {
            // while let doesn't compile
            let Poll::Ready(message) = pin!(this.dataplane.udp_read()).poll_unpin(cx) else {
                break;
            };

            // Enter the received raptorcast chunk into the udp_state for reassembly.
            // If the field "first-hop recipient" in the chunk has our node Id, then
            // we are responsible for broadcasting this chunk to other validators.
            // Once we have enough (redundant) raptorcast chunks, recreate the
            // decoded (AKA parsed, original) message.
            // Stream the chunks to our dedicated full-nodes as we receive them.
            let decoded_app_messages = {
                // FIXME: pass dataplane as arg to handle_message
                let dataplane = RefCell::new(&mut this.dataplane);
                this.udp_state.handle_message(
                    &this.rebroadcast_map,
                    |targets, payload, bcast_stride| {
                        // Callback for re-broadcasting raptorcast chunks to other RC participants (validator peers)
                        let target_addrs: Vec<SocketAddr> = targets
                            .into_iter()
                            .filter_map(|target| this.known_addresses.get(&target).copied())
                            .collect();

                        dataplane.borrow_mut().udp_write_broadcast(BroadcastMsg {
                            targets: target_addrs,
                            payload,
                            stride: bcast_stride,
                        });
                    },
                    |payload, bcast_stride| {
                        // callback for forwarding chunks to full nodes
                        dataplane.borrow_mut().udp_write_broadcast(BroadcastMsg {
                            targets: full_node_addrs.clone(),
                            payload,
                            stride: bcast_stride,
                        });
                    },
                    message,
                )
            };
            let deserialized_app_messages =
                decoded_app_messages
                    .into_iter()
                    .filter_map(|(from, decoded)| {
                        match try_deserialize_router_message::<ST, M>(&decoded) {
                            Ok(inbound) => match inbound {
                                InboundRouterMessage::AppMessage(app_message) => {
                                    Some(app_message.event(from))
                                }
                                InboundRouterMessage::PeerDiscoveryMessage(peer_disc_message) => {
                                    if PEER_DISCOVERY_ENABLED {
                                        // handle peer discovery message in driver
                                        this.peer_discovery_driver
                                            .update(peer_disc_message.event(from));
                                    }
                                    None
                                }
                            },
                            Err(err) => {
                                warn!(
                                    ?from,
                                    ?err,
                                    decoded = hex::encode(&decoded),
                                    "failed to deserialize message"
                                );
                                None
                            }
                        }
                    });
            this.pending_events
                .extend(deserialized_app_messages.map(RaptorCastEvent::Message));
            if let Some(event) = this.pending_events.pop_front() {
                return Poll::Ready(Some(event.into()));
            }
        }

        while let Poll::Ready((from_addr, message)) = pin!(this.dataplane.tcp_read()).poll_unpin(cx)
        {
            let signature_bytes = &message[..SIGNATURE_SIZE];
            let signature = match ST::deserialize(signature_bytes) {
                Ok(signature) => signature,
                Err(err) => {
                    warn!(?err, ?from_addr, "invalid signature");
                    continue;
                }
            };
            let app_message_bytes = message.slice(SIGNATURE_SIZE..);
            let deserialized_message =
                match try_deserialize_router_message::<ST, M>(&app_message_bytes) {
                    Ok(message) => message,
                    Err(err) => {
                        warn!(?err, ?from_addr, "failed to deserialize message");
                        continue;
                    }
                };
            let from = match signature.recover_pubkey(app_message_bytes.as_ref()) {
                Ok(from) => from,
                Err(err) => {
                    warn!(?err, ?from_addr, "failed to recover pubkey");
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
            }
        }

        // TODO: remove the following block when we want to enable peer discovery messages
        if PEER_DISCOVERY_ENABLED {
            while let Poll::Ready(Some(peer_disc_emit)) =
                this.peer_discovery_driver.poll_next_unpin(cx)
            {
                match peer_disc_emit {
                    PeerDiscoveryEmit::RouterCommand { target, message } => {
                        match OutboundRouterMessage::try_serialize(
                            OutboundRouterMessage::<OM, ST>::PeerDiscoveryMessage(message),
                        ) {
                            Ok(router_message) => {
                                let current_epoch = this.current_epoch;
                                let unicast_msg = udp_build(
                                    &current_epoch,
                                    BuildTarget::<ST>::PointToPoint(&target),
                                    router_message,
                                    this.mtu,
                                    &this.key,
                                    this.redundancy,
                                    &this.known_addresses,
                                );
                                this.dataplane.udp_write_unicast(unicast_msg);
                            }
                            Err(e) => {
                                warn!("unable to serialize peer discovery message {:?}", e);
                                continue;
                            }
                        }
                    }
                }
            }
        }

        Poll::Pending
    }
}

impl<ST, SCT, EPT> From<RaptorCastEvent<MonadEvent<ST, SCT, EPT>, CertificateSignaturePubKey<ST>>>
    for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(
        value: RaptorCastEvent<MonadEvent<ST, SCT, EPT>, CertificateSignaturePubKey<ST>>,
    ) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(peer_manager_response) => {
                match peer_manager_response {
                    PeerManagerResponse::PeerList(peer_list) => MonadEvent::ControlPanelEvent(
                        ControlPanelEvent::GetPeers(GetPeers::Response(peer_list)),
                    ),
                    PeerManagerResponse::FullNodes(full_nodes) => MonadEvent::ControlPanelEvent(
                        ControlPanelEvent::GetFullNodes(GetFullNodes::Response(full_nodes)),
                    ),
                }
            }
        }
    }
}
