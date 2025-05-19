use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::{pin, Pin},
    sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable};
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, Stream};
use message::InboundRouterMessage;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::{
    udp::{segment_size_for_mtu, DEFAULT_MTU},
    BroadcastMsg, Dataplane, DataplaneBuilder, TcpMsg, UnicastMsg,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ControlPanelEvent, GetFullNodes, GetPeers, Message, MonadEvent, RouterCommand,
};
use monad_types::{
    Deserializable, DropTimer, Epoch, ExecutionProtocol, NodeId, RouterTarget, Serializable,
};
use rlp::DeserializeError;
use util::{BuildTarget, EpochValidators, FullNodes, Group, ReBroadcastGroupMap, Validator};

pub mod config;
pub mod message;
pub mod raptorcast_secondary;
pub mod rlp;
pub mod udp;
pub mod util;

const SIGNATURE_SIZE: usize = 65;

pub struct RaptorCast<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
{
    signing_key: Arc<ST::KeyPairType>,
    redundancy: u8,

    // Raptorcast group with stake information. For the send side (i.e., initiating proposals)
    epoch_validators: BTreeMap<Epoch, EpochValidators<ST>>,
    rebroadcast_map: ReBroadcastGroupMap<ST>,

    // TODO support dynamic updating
    dedicated_full_nodes: FullNodes<CertificateSignaturePubKey<ST>>,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,

    current_epoch: Epoch,

    udp_state: udp::UdpState<ST>,
    mtu: u16,

    dataplane: Arc<Mutex<Dataplane>>,
    pending_events: VecDeque<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
    channel_to_secondary:
        Option<Sender<raptorcast_secondary::group_message::FullNodesGroupMessage<ST>>>,
    channel_from_secondary: Option<Receiver<Group<ST>>>,

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

impl<ST, M, OM, SE> RaptorCast<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
{
    pub fn new(
        config: &config::RaptorCastConfig<ST>,
        shared_dataplane: Arc<Mutex<Dataplane>>,
    ) -> Self {
        let self_id = NodeId::new(config.shared_key.pubkey());
        let is_fullnode = matches!(
            config.secondary_instance,
            config::SecondaryRcModeConfig::Client(_)
        );
        Self {
            epoch_validators: Default::default(),
            rebroadcast_map: ReBroadcastGroupMap::new(self_id, is_fullnode),
            dedicated_full_nodes: FullNodes::new(
                config.primary_instance.full_nodes_dedicated.clone(),
            ),
            known_addresses: config.known_addresses.clone(),

            signing_key: config.shared_key.clone(),
            redundancy: config.primary_instance.raptor10_redundancy,

            current_epoch: Epoch(0),

            udp_state: udp::UdpState::new(self_id),
            mtu: config.mtu,

            dataplane: shared_dataplane,
            pending_events: Default::default(),
            channel_to_secondary: None,
            channel_from_secondary: None,

            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    // Convenience constructor for tests, which were previously using this default via copy-paste
    pub fn new_defaulted_for_tests(
        local_addr: &SocketAddr,
        known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
        shared_key: Arc<ST::KeyPairType>,
    ) -> Self {
        let up_bandwidth_mbps = 1_000;
        let dp_builder = DataplaneBuilder::new(local_addr, up_bandwidth_mbps);
        let shared_dataplane = Arc::new(Mutex::new(dp_builder.build()));
        let cfg = config::RaptorCastConfig {
            shared_key,
            known_addresses,
            mtu: DEFAULT_MTU,
            primary_instance: Default::default(),
            secondary_instance: config::SecondaryRcModeConfig::None,
        };
        Self::new(&cfg, shared_dataplane)
    }

    pub fn bind_channel_to_secondary_raptorcast(
        mut self,
        channel_to_secondary: Sender<
            raptorcast_secondary::group_message::FullNodesGroupMessage<ST>,
        >,
        channel_from_secondary: Receiver<Group<ST>>,
    ) -> Self {
        self.channel_to_secondary = Some(channel_to_secondary);
        self.channel_from_secondary = Some(channel_from_secondary);
        self
    }

    fn enqueue_message_to_self(
        a_message: OM,
        pending_events: &mut VecDeque<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
        waker: &mut Option<Waker>,
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) {
        let message: M = a_message.into();
        pending_events.push_back(RaptorCastEvent::Message(message.event(self_id)));
        if let Some(waker) = waker.take() {
            waker.wake()
        }
    }

    fn build_rc_chunks(
        epoch: &Epoch,
        build_target: BuildTarget<ST>,
        app_message: Bytes,
        mtu: u16,
        signing_key: &Arc<ST::KeyPairType>,
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
            signing_key,
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
}

impl<ST, M, OM, SE> Executor for RaptorCast<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let self_id = NodeId::new(self.signing_key.pubkey());

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
                        tracing::warn!(
                            "duplicate validator set update (this is safe but unexpected)"
                        )
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
                }
                RouterCommand::Publish { target, message } => {
                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        tracing::warn!(?elapsed, "long time to publish message")
                    });

                    match target {
                        RouterTarget::Broadcast(epoch) | RouterTarget::Raptorcast(epoch) => {
                            let Some(epoch_validators) = self.epoch_validators.get_mut(&epoch)
                            else {
                                tracing::error!(
                                    "don't have epoch validators populated for epoch: {:?}",
                                    epoch
                                );
                                continue;
                            };

                            let app_message = message.serialize();

                            if epoch_validators.validators.contains_key(&self_id) {
                                Self::enqueue_message_to_self(
                                    message,
                                    &mut self.pending_events,
                                    &mut self.waker,
                                    self_id,
                                );
                            }
                            let epoch_validators_without_self =
                                epoch_validators.view_without(vec![&self_id]);
                            if epoch_validators_without_self.view().is_empty() {
                                // this is degenerate case where the only validator is self
                                continue;
                            }

                            let full_nodes_view = self.dedicated_full_nodes.view();

                            // Create the UDP message(s) to send to validators and dedicated full-nodes
                            let build_target = match &target {
                                RouterTarget::Broadcast(_) => {
                                    // <-- current testground
                                    BuildTarget::Broadcast(epoch_validators_without_self)
                                }
                                RouterTarget::Raptorcast(_) => BuildTarget::Raptorcast((
                                    epoch_validators_without_self,
                                    full_nodes_view,
                                )),
                                _ => unreachable!(),
                            };

                            let rc_chunks: UnicastMsg = Self::build_rc_chunks(
                                &epoch,
                                build_target,
                                app_message,
                                self.mtu,
                                &self.signing_key,
                                self.redundancy,
                                &self.known_addresses,
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
                                let app_message = message.serialize();
                                let rc_chunks: UnicastMsg = Self::build_rc_chunks(
                                    &self.current_epoch,
                                    BuildTarget::PointToPoint(&to),
                                    app_message,
                                    self.mtu,
                                    &self.signing_key,
                                    self.redundancy,
                                    &self.known_addresses,
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
                                match self.known_addresses.get(&to) {
                                    None => {
                                        tracing::warn!(?to, "not sending message, address unknown");
                                    }
                                    Some(address) => {
                                        let app_message = message.serialize(); //make_app_message();
                                                                               // TODO make this more sophisticated
                                                                               // include timestamp, etc
                                        let mut signed_message =
                                            BytesMut::zeroed(SIGNATURE_SIZE + app_message.len());
                                        let signature =
                                            ST::sign(&app_message, &self.signing_key).serialize();
                                        assert_eq!(signature.len(), SIGNATURE_SIZE);
                                        signed_message[..SIGNATURE_SIZE]
                                            .copy_from_slice(&signature);
                                        signed_message[SIGNATURE_SIZE..]
                                            .copy_from_slice(&app_message);
                                        self.dataplane.lock().unwrap().tcp_write(
                                            *address,
                                            TcpMsg {
                                                msg: signed_message.freeze(),
                                                completion,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                    };
                }
                RouterCommand::PublishToFullNodes { .. } => {}
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
                    let full_nodes = self.dedicated_full_nodes.list.clone();
                    self.pending_events
                        .push_back(RaptorCastEvent::PeerManagerResponse(
                            PeerManagerResponse::FullNodes(full_nodes),
                        ));
                }
                RouterCommand::UpdateFullNodes(new_full_nodes) => {
                    self.dedicated_full_nodes.list = new_full_nodes;
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
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

impl<ST, M, OM, E> Stream for RaptorCast<ST, M, OM, E>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
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
            .dedicated_full_nodes
            .list
            .iter()
            .filter_map(|node_id| this.known_addresses.get(node_id).copied())
            .collect::<Vec<_>>();

        loop {
            // while let doesn't compile
            let Poll::Ready(message) =
                pin!(this.dataplane.lock().unwrap().udp_read()).poll_unpin(cx)
            else {
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
                this.udp_state.handle_message(
                    &this.rebroadcast_map, // contains the NodeIds for all the RC participants for each epoch
                    |targets, payload, bcast_stride| {
                        // Callback for re-broadcasting raptorcast chunks to other RC participants (validator peers)
                        let target_addrs: Vec<SocketAddr> = targets
                            .into_iter()
                            .filter_map(|target| this.known_addresses.get(&target).copied())
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
                        // Callback for forwarding complete messages to full nodes
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
            let deserialized_app_messages =
                decoded_app_messages
                    .into_iter()
                    .filter_map(|(from, decoded)| {
                        match try_deserialize_router_message::<ST, M>(&decoded) {
                            Ok(inbound) => match inbound {
                                InboundRouterMessage::AppMessage(app_message) => {
                                    Some(app_message.event(from))
                                }
                                InboundRouterMessage::FullNodesGroup(full_nodes_group_message) => {
                                    match &this.channel_to_secondary {
                                        Some(channel) => {
                                            if let Err(err) = channel.send(full_nodes_group_message)
                                            {
                                                tracing::error!(
                                                    "Could not send InboundRouterMessage to \
                                    secondary Raptorcast instance: {}",
                                                    err
                                                );
                                            }
                                        }
                                        None => {
                                            tracing::warn!(
                                                ?from,
                                                "Received FullNodesGroup message but the primary \
                                Raptorcast instance is not setup to forward messages \
                                to a secondary instance."
                                            );
                                        }
                                    }
                                    None
                                }
                            },
                            Err(err) => {
                                tracing::warn!(
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

        while let Poll::Ready((from_addr, message)) =
            pin!(this.dataplane.lock().unwrap().tcp_read()).poll_unpin(cx)
        {
            let signature_bytes = &message[..SIGNATURE_SIZE];
            let signature = match ST::deserialize(signature_bytes) {
                Ok(signature) => signature,
                Err(err) => {
                    tracing::warn!(?err, ?from_addr, "invalid signature");
                    continue;
                }
            };
            let app_message_bytes = message.slice(SIGNATURE_SIZE..);
            let deserialized_message =
                match try_deserialize_router_message::<ST, M>(&app_message_bytes) {
                    Ok(message) => message,
                    Err(err) => {
                        tracing::warn!(?err, ?from_addr, "failed to deserialize message");
                        continue;
                    }
                };
            let from = match signature.recover_pubkey(app_message_bytes.as_ref()) {
                Ok(from) => from,
                Err(err) => {
                    tracing::warn!(?err, ?from_addr, "failed to recover pubkey");
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
                InboundRouterMessage::FullNodesGroup(_group_message) => {
                    // pass TCP message to MultiRouter
                    unimplemented!("FullNodesGroup protocol via TCP not implemented");
                }
            }
        }

        // The secondary Raptorcast instance (Client) will be periodically sending us
        // updates about new rc groups that we should use when re-broadcasting
        if let Some(channel_from_secondary) = &this.channel_from_secondary {
            loop {
                match channel_from_secondary.try_recv() {
                    Ok(group) => {
                        this.rebroadcast_map.push_group_fullnodes(group);
                    }
                    Err(err) => {
                        if let std::sync::mpsc::TryRecvError::Disconnected = err {
                            tracing::error!("RaptorCastPrimary channel disconnected.");
                        }
                        break;
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
