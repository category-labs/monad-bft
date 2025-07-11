use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

mod client;
pub mod group_message;
mod publisher;

use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use client::Client;
use futures::Stream;
use group_message::FullNodesGroupMessage;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{udp::segment_size_for_mtu, Dataplane, UnicastMsg};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, PeerEntry, RouterCommand};
use monad_peer_discovery::{driver::PeerDiscoveryDriver, PeerDiscoveryAlgo, PeerDiscoveryEvent};
use monad_types::{DropTimer, Epoch, NodeId};
use publisher::Publisher;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tracing::{error, trace, warn};

use super::{
    config::RaptorCastConfig,
    message::OutboundRouterMessage,
    udp,
    util::{BuildTarget, FullNodes, Group},
    RaptorCastEvent,
};

// We're planning to merge monad-node (validator binary) and monad-full-node
// (full node binary), so it's possible for a node to switch between roles at
// runtime.
enum Role<ST>
where
    ST: CertificateSignatureRecoverable,
{
    Publisher(Publisher<ST>),
    Client(Client<ST>),
}

pub struct RaptorCastSecondary<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    // Main state machine, depending on wether we are playing the publisher role
    // (i.e. we are a validator) or a client role (full-node raptor-casted to)
    // Represents only the group logic, excluding everything network related.
    role: Role<ST>,

    // Args for encoding outbound (validator -> full-node) messages
    signing_key: Arc<ST::KeyPairType>, // for re-signing app messages
    raptor10_redundancy: u8,
    curr_epoch: Epoch,

    mtu: u16,
    dataplane: Arc<Mutex<Dataplane>>,
    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    pending_events: VecDeque<RaptorCastEvent<M::Event, ST>>,
    channel_from_primary: Receiver<FullNodesGroupMessage<ST>>,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(OM, SE)>,
}

impl<ST, M, OM, SE, PD> RaptorCastSecondary<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    pub fn new(
        config: RaptorCastConfig<ST>,
        dataplane: Arc<Mutex<Dataplane>>,
        peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        channel_from_primary: Receiver<FullNodesGroupMessage<ST>>,
        channel_to_primary: Sender<Group<ST>>,
    ) -> Self {
        let node_id = NodeId::new(config.shared_key.pubkey());
        let sec_config = config.secondary_instance.mode.clone();

        // Instantiate either publisher or client state machine
        let role = match sec_config {
            super::config::SecondaryRaptorCastModeConfig::Publisher(publisher_cfg) => {
                let rng = ChaCha8Rng::from_entropy();
                let publisher = Publisher::new(node_id, publisher_cfg, rng);
                Role::Publisher(publisher)
            }
            super::config::SecondaryRaptorCastModeConfig::Client(client_cfg) => {
                let client = Client::new(node_id, channel_to_primary, client_cfg);
                Role::Client(client)
            }
            super::config::SecondaryRaptorCastModeConfig::None => panic!(
                "secondary_instance is not set in config during \
                    instantiation of RaptorCastSecondary"
            ),
        };

        let raptor10_redundancy = config.secondary_instance.raptor10_redundancy;
        trace!(
            self_id =? node_id, mtu =? config.mtu, ?raptor10_redundancy,
            "RaptorCastSecondary::new()",
        );

        if raptor10_redundancy < 1 {
            panic!(
                "Configuration value raptor10_redundancy must be equal or greater than 1, \
                but got {}. This is a bug in the configuration for the secondary instance.",
                raptor10_redundancy
            );
        }

        Self {
            role,
            signing_key: config.shared_key.clone(),
            raptor10_redundancy,
            curr_epoch: Epoch(0),
            mtu: config.mtu,
            dataplane,
            peer_discovery_driver,
            pending_events: Default::default(),
            channel_from_primary,
            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    fn udp_build(
        epoch: &Epoch,
        build_target: BuildTarget<ST>,
        outbound_message: Bytes,
        mtu: u16,
        signing_key: &Arc<ST::KeyPairType>,
        redundancy: u8,
        known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    ) -> UnicastMsg {
        let outbound_message_len = outbound_message.len();
        let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
            warn!(?elapsed, outbound_message_len, "long time to udp_build")
        });
        let segment_size = segment_size_for_mtu(mtu);

        let unix_ts_ms = std::time::UNIX_EPOCH
            .elapsed()
            .expect("time went backwards")
            .as_millis()
            .try_into()
            .expect("unix epoch doesn't fit in u64");

        trace!(
            ?mtu,
            ?outbound_message_len,
            ?redundancy,
            ?segment_size,
            "RaptorCastSecondary::udp_build()"
        );

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

    fn send_single_msg(
        &self,
        group_msg: FullNodesGroupMessage<ST>,
        dest_node: &NodeId<CertificateSignaturePubKey<ST>>,
        known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    ) {
        trace!(
            ?dest_node,
            ?group_msg,
            "RaptorCastSecondary send_single_msg"
        );
        let router_msg: OutboundRouterMessage<OM, ST> =
            OutboundRouterMessage::FullNodesGroup(group_msg);
        let msg_bytes = match router_msg.try_serialize() {
            Ok(msg) => msg,
            Err(err) => {
                error!(?err, "failed to serialize a message");
                return;
            }
        };
        let udp_messages = Self::udp_build(
            &self.curr_epoch,
            BuildTarget::<ST>::PointToPoint(dest_node),
            msg_bytes,
            self.mtu,
            &self.signing_key,
            self.raptor10_redundancy,
            known_addresses,
        );
        self.dataplane
            .lock()
            .unwrap()
            .udp_write_unicast(udp_messages);
    }

    fn send_group_msg(
        &self,
        group_msg: FullNodesGroupMessage<ST>,
        dest_node_ids: FullNodes<CertificateSignaturePubKey<ST>>,
        known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    ) {
        trace!(
            ?dest_node_ids,
            ?group_msg,
            "RaptorCastSecondary send_group_msg"
        );
        let _timer = DropTimer::start(Duration::from_millis(100), |elapsed| {
            warn!(?elapsed, "long time to send_group_msg")
        });
        let group_msg = self.try_fill_name_records(group_msg, &dest_node_ids);
        // Can udp_write_broadcast() be used? Optimize later
        for nid in dest_node_ids.list {
            self.send_single_msg(group_msg.clone(), &nid, known_addresses);
        }
    }

    fn try_fill_name_records(
        &self,
        group_msg: FullNodesGroupMessage<ST>,
        dest_node_ids: &FullNodes<CertificateSignaturePubKey<ST>>,
    ) -> FullNodesGroupMessage<ST> {
        if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = &group_msg {
            let name_records = {
                self.peer_discovery_driver
                    .lock()
                    .unwrap()
                    .get_name_records()
            };
            let mut filled_confirm_msg = confirm_msg.clone();
            filled_confirm_msg.name_records = Vec::default();
            for node_id in &dest_node_ids.list {
                if let Some(name_record) = name_records.get(node_id) {
                    filled_confirm_msg.name_records.push(*name_record);
                } else {
                    // Maybe can happen if peer discovery gets pruned just
                    // before sending a ConfirmGroup message.
                    warn!( ?node_id, ?group_msg,
                        "RaptorCastSecondary: No name record found for node_id when sending out ConfirmGroup message",
                    );
                }
            }
            return FullNodesGroupMessage::ConfirmGroup(filled_confirm_msg);
        }
        group_msg
    }
}

impl<ST, M, OM, SE, PD> Executor for RaptorCastSecondary<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    type Command = RouterCommand<ST, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                Self::Command::Publish { .. } => {
                    panic!("Command routed to secondary RaptorCast: Publish")
                }
                Self::Command::AddEpochValidatorSet { .. } => {
                    panic!("Command routed to secondary RaptorCast: AddEpochValidatorSet")
                }
                Self::Command::GetPeers { .. } => {
                    panic!("Command routed to secondary RaptorCast: GetPeers")
                }
                Self::Command::UpdatePeers { .. } => {
                    panic!("Command routed to secondary RaptorCast: UpdatePeers")
                }
                Self::Command::GetFullNodes => {
                    panic!("Command routed to secondary RaptorCast: GetFullNodes")
                }
                Self::Command::UpdateFullNodes(..) => {
                    panic!("Command routed to secondary RaptorCast: UpdateFullNodes")
                }

                Self::Command::UpdateCurrentRound(epoch, round) => match &mut self.role {
                    Role::Client(client) => {
                        trace!(
                            ?epoch,
                            ?round,
                            "RaptorCastSecondary UpdateCurrentRound (Client)"
                        );
                        client.enter_round(round);
                    }
                    Role::Publisher(publisher) => {
                        trace!(
                            ?epoch,
                            ?round,
                            "RaptorCastSecondary UpdateCurrentRound (Publisher)"
                        );
                        self.curr_epoch = epoch;
                        // The publisher needs to be periodically informed about new nodes out there,
                        // so that it can randomize when creating new groups.
                        {
                            let known_addresses = {
                                self.peer_discovery_driver
                                    .lock()
                                    .unwrap()
                                    .get_known_addresses()
                            };
                            let nodes: Vec<_> = known_addresses.keys().copied().collect();
                            trace!(
                                "RaptorCastSecondary updating {} full nodes from PeerDiscovery",
                                nodes.len()
                            );
                            publisher.upsert_peer_disc_full_nodes(FullNodes::new(nodes));
                        }

                        if let Some((group_msg, full_nodes_set)) =
                            publisher.enter_round_and_step_until(round)
                        {
                            let known_addresses = {
                                self.peer_discovery_driver
                                    .lock()
                                    .unwrap()
                                    .get_known_addresses()
                            };
                            self.send_group_msg(group_msg, full_nodes_set, &known_addresses);
                        }
                    }
                },

                Self::Command::PublishToFullNodes { epoch, message } => {
                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        warn!(?elapsed, "long time to publish message")
                    });

                    let curr_group: &Group<ST> = match &mut self.role {
                        Role::Client(_) => {
                            continue;
                        }
                        Role::Publisher(publisher) => {
                            match publisher.get_current_raptorcast_group() {
                                Some(group) => {
                                    trace!(?group, size_excl_self =? group.size_excl_self(),
                                        "RaptorCastSecondary PublishToFullNodes");
                                    group
                                }
                                None => {
                                    trace!("RaptorCastSecondary PublishToFullNodes; group: NONE");
                                    continue;
                                }
                            }
                        }
                    };

                    if curr_group.size_excl_self() < 1 {
                        trace!("RaptorCastSecondary PublishToFullNodes; Not sending anything because size_excl_self = 0");
                        continue;
                    }

                    let build_target = BuildTarget::FullNodeRaptorCast(curr_group);

                    let known_addresses = self
                        .peer_discovery_driver
                        .lock()
                        .unwrap()
                        .get_known_addresses();

                    let outbound_message = match OutboundRouterMessage::<OM, ST>::AppMessage(
                        message,
                    )
                    .try_serialize()
                    {
                        Ok(msg) => msg,
                        Err(err) => {
                            error!(?err, "failed to serialize a message");
                            continue;
                        }
                    };

                    // Split outbound_message into raptorcast chunks that we can
                    // send to full nodes.
                    let rc_chunks: UnicastMsg = Self::udp_build(
                        &epoch,
                        build_target,
                        outbound_message,
                        self.mtu,
                        &self.signing_key,
                        self.raptor10_redundancy,
                        &known_addresses,
                    );

                    // Send the raptorcast chunks via UDP to all peers in group
                    self.dataplane.lock().unwrap().udp_write_unicast(rc_chunks);
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
impl<ST, M, OM, E, PD> Stream for RaptorCastSecondary<ST, M, OM, E, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    Self: Unpin,
{
    type Item = E;

    // Since we are sending to full-nodes only, and not receiving anything from them,
    // we don't need to handle any receive here and this is just to satisfy traits
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event.into()));
        }

        match this.channel_from_primary.try_recv() {
            Ok(inbound_grp_msg) => match &mut this.role {
                Role::Publisher(publisher) => {
                    publisher.on_candidate_response(inbound_grp_msg);
                }

                Role::Client(client) => {
                    trace!("RaptorCastSecondary received group message");
                    // Received group message from validator
                    if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = &inbound_grp_msg {
                        let num_mappings = confirm_msg.name_records.len();
                        if num_mappings > 0 && num_mappings == confirm_msg.peers.len() {
                            let mut peers: Vec<PeerEntry<ST>> = Vec::new();
                            // FIXME: bind name records with peers and ask peer discovery to verify
                            for ii in 0..num_mappings {
                                let rec = &confirm_msg.name_records[ii];
                                let peer_entry = PeerEntry {
                                    pubkey: confirm_msg.peers[ii].pubkey(),
                                    addr: rec.name_record.address,
                                    signature: rec.signature,
                                    record_seq_num: rec.name_record.seq,
                                };
                                peers.push(peer_entry);
                            }
                            this.peer_discovery_driver
                                .lock()
                                .unwrap()
                                .update(PeerDiscoveryEvent::UpdatePeers { peers });
                        } else if num_mappings > 0 {
                            warn!( ?confirm_msg, num_peers =? confirm_msg.peers.len(), num_name_recs =? confirm_msg.name_records.len(),
                                "Number of peers does not match the number \
                                of name records in ConfirmGroup message. \
                                Skipping PeerDiscovery update"
                            );
                        }
                    }
                    if let Some((response_msg, validator_id)) =
                        client.on_receive_group_message(inbound_grp_msg)
                    {
                        // Send back a response to the validator
                        trace!("RaptorCastSecondary sending back response for group message");

                        let known_addresses = {
                            this.peer_discovery_driver
                                .lock()
                                .unwrap()
                                .get_known_addresses()
                        };

                        this.send_single_msg(response_msg, &validator_id, &known_addresses);
                    }
                }
            },

            Err(err) => {
                if let std::sync::mpsc::TryRecvError::Disconnected = err {
                    error!("RaptorCastSecondary channel disconnected.");
                }
            }
        }

        Poll::Pending
    }
}
