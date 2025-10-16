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
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    net::SocketAddr,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

mod client;
pub mod group_message;
mod publisher;

use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use client::Client;
use futures::{Future, Stream};
use group_message::FullNodesGroupMessage;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{udp::segment_size_for_mtu, DataplaneWriter, UnicastMsg};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, PeerEntry, RouterCommand};
use monad_peer_discovery::{driver::PeerDiscoveryDriver, PeerDiscoveryAlgo, PeerDiscoveryEvent};
use monad_types::{DropTimer, Epoch, NodeId};
use publisher::Publisher;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, trace, warn};

use super::{
    config::{RaptorCastConfig, SecondaryRaptorCastMode},
    message::OutboundRouterMessage,
    udp,
    util::{BuildTarget, FullNodes, Group, Redundancy},
    RaptorCastEvent,
};

#[derive(Debug, Clone, Copy)]
pub enum SecondaryRaptorCastModeConfig {
    Client,
    Publisher,
    None, // Disables secondary raptorcast
}

// It's possible to switch role during runtime
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
    raptor10_redundancy: Redundancy,
    curr_epoch: Epoch,

    mtu: u16,
    dataplane_writer: DataplaneWriter,
    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    channel_from_primary: UnboundedReceiver<FullNodesGroupMessage<ST>>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(OM, SE, M)>,
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
        secondary_mode: SecondaryRaptorCastMode<ST>,
        dataplane_writer: DataplaneWriter,
        peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        channel_from_primary: UnboundedReceiver<FullNodesGroupMessage<ST>>,
        channel_to_primary: UnboundedSender<Group<ST>>,
        current_epoch: Epoch,
    ) -> Self {
        let node_id = NodeId::new(config.shared_key.pubkey());

        // Instantiate either publisher or client state machine
        let role = match secondary_mode {
            SecondaryRaptorCastMode::Publisher(publisher_cfg) => {
                let rng = ChaCha8Rng::from_entropy();
                let publisher = Publisher::new(node_id, publisher_cfg, rng);
                Role::Publisher(publisher)
            }
            SecondaryRaptorCastMode::Client(client_cfg) => {
                let client = Client::new(node_id, channel_to_primary, client_cfg);
                Role::Client(client)
            }
            SecondaryRaptorCastMode::None => panic!(
                "secondary_instance is not set in config during \
                    instantiation of RaptorCastSecondary"
            ),
        };

        let raptor10_redundancy = config
            .secondary_instance
            .raptor10_fullnode_redundancy_factor;
        trace!(
            self_id =? node_id, mtu =? config.mtu, ?raptor10_redundancy,
            "RaptorCastSecondary::new()",
        );

        if raptor10_redundancy < 1f32 {
            panic!(
                "Configuration value raptor10_redundancy must be equal or greater than 1, \
                but got {}. This is a bug in the configuration for the secondary instance.",
                raptor10_redundancy
            );
        }

        Self {
            role,
            signing_key: config.shared_key.clone(),
            raptor10_redundancy: Redundancy::from_f32(raptor10_redundancy)
                .expect("secondary raptor10_redundancy doesn't fit"),
            curr_epoch: current_epoch,
            mtu: config.mtu,
            dataplane_writer,
            peer_discovery_driver,
            channel_from_primary,
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
        redundancy: Redundancy,
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
        self.dataplane_writer.udp_write_unicast(udp_messages);
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
                Self::Command::UpdateFullNodes {
                    dedicated_full_nodes: _,
                    prioritized_full_nodes,
                } => match &mut self.role {
                    Role::Client(_) => {
                        // client don't care about dedicated and prioritized full nodes
                        debug!(
                            ?prioritized_full_nodes,
                            "RaptorCastSecondary Client ignoring UpdateFullNodes command"
                        );
                    }
                    Role::Publisher(publisher) => {
                        debug!(
                            ?prioritized_full_nodes,
                            "RaptorCastSecondary Publisher updating prioritized full nodes"
                        );
                        publisher.update_always_ask_full_nodes(prioritized_full_nodes);
                    }
                },

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
                        let full_nodes = self
                            .peer_discovery_driver
                            .lock()
                            .unwrap()
                            .get_secondary_fullnodes();
                        trace!(
                            "RaptorCastSecondary updating {} full nodes from PeerDiscovery",
                            full_nodes.len()
                        );
                        publisher.upsert_peer_disc_full_nodes(FullNodes::new(full_nodes));

                        if let Some((group_msg, full_nodes_set)) =
                            publisher.enter_round_and_step_until(round)
                        {
                            // if group_msg is a ConfirmGroup message, update peer discovery with the group information
                            if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = &group_msg {
                                let mut participated_nodes: BTreeSet<
                                    NodeId<CertificateSignaturePubKey<ST>>,
                                > = confirm_msg.peers.clone().into_iter().collect();
                                participated_nodes.insert(confirm_msg.prepare.validator_id);
                                self.peer_discovery_driver.lock().unwrap().update(
                                    PeerDiscoveryEvent::UpdateConfirmGroup {
                                        end_round: confirm_msg.prepare.end_round,
                                        peers: participated_nodes,
                                    },
                                );
                            }

                            let known_addresses = self
                                .peer_discovery_driver
                                .lock()
                                .unwrap()
                                .get_known_addresses();
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
                    self.dataplane_writer.udp_write_unicast(rc_chunks);
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        match &self.role {
            Role::Publisher(publisher) => publisher.metrics().into(),
            Role::Client(client) => client.metrics().into(),
        }
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

        let inbound_grp_msg = match pin!(this.channel_from_primary.recv()).poll(cx) {
            Poll::Ready(Some(inbound_grp_msg)) => inbound_grp_msg,
            Poll::Ready(None) => {
                tracing::error!("RaptorCastSecondary channel disconnected.");
                // TODO: should we return Poll::Ready(None) here?
                return Poll::Pending;
            }
            Poll::Pending => {
                // No group message received, so we are not ready to process anything
                return Poll::Pending;
            }
        };

        let mut ret = Poll::Pending;

        match &mut this.role {
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

                        // participated_nodes contains the validator and all full nodes in the group
                        let mut participated_nodes: BTreeSet<
                            NodeId<CertificateSignaturePubKey<ST>>,
                        > = confirm_msg.peers.clone().into_iter().collect();
                        participated_nodes.insert(confirm_msg.prepare.validator_id);
                        this.peer_discovery_driver.lock().unwrap().update(
                            PeerDiscoveryEvent::UpdateConfirmGroup {
                                end_round: confirm_msg.prepare.end_round,
                                peers: participated_nodes.clone(),
                            },
                        );

                        ret = Poll::Ready(Some(
                            RaptorCastEvent::SecondaryRaptorcastPeersUpdate(
                                participated_nodes.into_iter().collect(),
                            )
                            .into(),
                        ));
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
        }

        ret
    }
}
