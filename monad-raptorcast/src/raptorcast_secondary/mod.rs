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
    collections::BTreeSet,
    marker::PhantomData,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

mod client;
pub mod group_message;
mod publisher;

use alloy_rlp::{Decodable, Encodable};
use client::Client;
use futures::{Future, Stream};
use group_message::FullNodesGroupMessage;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, PeerEntry, RouterCommand};
use monad_peer_discovery::{driver::PeerDiscoveryDriver, PeerDiscoveryAlgo, PeerDiscoveryEvent};
use monad_types::{Epoch, NodeId};
use publisher::Publisher;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender};
use tracing::{debug, error, trace, warn};

use crate::{
    config::{RaptorCastConfig, SecondaryRaptorCastMode},
    message::OutboundRouterMessage,
    metrics::GAUGE_RAPTORCAST_BRIDGE_SECONDARY_TO_PRIMARY_OUTBOUND_DROPPED,
    udp::GroupId,
    util::{FullNodes, Group},
    RaptorCastEvent,
};

#[derive(Debug, Clone, Copy)]
pub enum SecondaryRaptorCastModeConfig {
    Client,
    Publisher,
    None, // Disables secondary raptorcast
}

#[derive(Debug, Clone)]
pub enum SecondaryOutboundMessage<PT: PubKey> {
    SendSingle {
        msg_bytes: bytes::Bytes,
        dest: NodeId<PT>,
        group_id: GroupId,
    },
    SendToGroup {
        msg_bytes: bytes::Bytes,
        group: Group<PT>,
        group_id: GroupId,
    },
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

    curr_epoch: Epoch,

    peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    channel_from_primary: Receiver<FullNodesGroupMessage<ST>>,
    channel_to_primary_outbound: Sender<SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>>,

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
        secondary_mode: SecondaryRaptorCastMode<CertificateSignaturePubKey<ST>>,
        peer_discovery_driver: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        channel_from_primary: Receiver<FullNodesGroupMessage<ST>>,
        channel_to_primary: Sender<Group<CertificateSignaturePubKey<ST>>>,
        channel_to_primary_outbound: Sender<
            SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>,
        >,
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

        trace!(self_id =? node_id, "RaptorCastSecondary::new()");

        Self {
            role,
            curr_epoch: current_epoch,
            peer_discovery_driver,
            channel_from_primary,
            channel_to_primary_outbound,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    fn send_single_msg(
        &mut self,
        group_msg: FullNodesGroupMessage<ST>,
        dest_node: NodeId<CertificateSignaturePubKey<ST>>,
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
                error!(?err, "failed to serialize message from secondary");
                return;
            }
        };

        let outbound = SecondaryOutboundMessage::SendSingle {
            msg_bytes,
            dest: dest_node,
            group_id: GroupId::Primary(self.curr_epoch),
        };
        match self.channel_to_primary_outbound.try_send(outbound) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.metrics[GAUGE_RAPTORCAST_BRIDGE_SECONDARY_TO_PRIMARY_OUTBOUND_DROPPED] += 1;
                debug!(
                    dropped =
                        self.metrics[GAUGE_RAPTORCAST_BRIDGE_SECONDARY_TO_PRIMARY_OUTBOUND_DROPPED],
                    "dropping message: secondary->primary outbound channel full"
                );
            }
            Err(TrySendError::Closed(_)) => {
                error!("failed to send message to primary: channel closed");
            }
        }
    }

    fn send_group_msg(
        &mut self,
        group_msg: FullNodesGroupMessage<ST>,
        dest_node_ids: FullNodes<CertificateSignaturePubKey<ST>>,
    ) {
        trace!(
            ?dest_node_ids,
            ?group_msg,
            "RaptorCastSecondary send_group_msg"
        );
        let group_msg = self.try_fill_name_records(group_msg, &dest_node_ids);
        for nid in dest_node_ids.list {
            self.send_single_msg(group_msg.clone(), nid);
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
            filled_confirm_msg.name_records = Default::default();
            for node_id in &dest_node_ids.list {
                if let Some(name_record) = name_records.get(node_id) {
                    filled_confirm_msg.name_records.push(name_record.clone());
                } else {
                    // Maybe can happen if peer discovery gets pruned just
                    // before sending a ConfirmGroup message.
                    warn!(
                        ?node_id,
                        ?group_msg,
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
                Self::Command::PublishWithPriority { .. } => {
                    panic!("Command routed to secondary RaptorCast: PublishWithPriority")
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

                            self.send_group_msg(group_msg, full_nodes_set);
                        }
                    }
                },

                Self::Command::PublishToFullNodes {
                    epoch: _,
                    round,
                    message,
                } => {
                    let curr_group: Group<_> = match &mut self.role {
                        Role::Client(_) => {
                            continue;
                        }
                        Role::Publisher(publisher) => {
                            match publisher.get_current_raptorcast_group() {
                                Some(group) => {
                                    trace!(?group, size_excl_self =? group.size_excl_self(),
                                        "RaptorCastSecondary PublishToFullNodes");
                                    group.clone()
                                }
                                None => {
                                    trace!("RaptorCastSecondary PublishToFullNodes; group: NONE");
                                    continue;
                                }
                            }
                        }
                    };

                    if curr_group.size_excl_self() < 1 {
                        trace!(
                            "RaptorCastSecondary PublishToFullNodes; Not sending anything because size_excl_self = 0"
                        );
                        continue;
                    }

                    let outbound_message = match OutboundRouterMessage::<OM, ST>::AppMessage(
                        message,
                    )
                    .try_serialize()
                    {
                        Ok(msg) => msg,
                        Err(err) => {
                            error!(?err, "failed to serialize message from secondary");
                            continue;
                        }
                    };

                    let outbound = SecondaryOutboundMessage::SendToGroup {
                        msg_bytes: outbound_message,
                        group: curr_group,
                        group_id: GroupId::Secondary(round),
                    };
                    match self.channel_to_primary_outbound.try_send(outbound) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            self.metrics
                                [GAUGE_RAPTORCAST_BRIDGE_SECONDARY_TO_PRIMARY_OUTBOUND_DROPPED] +=
                                1;
                            debug!(
                                dropped = self.metrics
                                    [GAUGE_RAPTORCAST_BRIDGE_SECONDARY_TO_PRIMARY_OUTBOUND_DROPPED],
                                "dropping message: secondary->primary outbound channel full"
                            );
                        }
                        Err(TrySendError::Closed(_)) => {
                            error!("failed to send message to primary: channel closed");
                        }
                    }
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        match &self.role {
            Role::Publisher(publisher) => ExecutorMetricsChain::default()
                .push(&self.metrics)
                .push(publisher.metrics()),
            Role::Client(client) => ExecutorMetricsChain::default()
                .push(&self.metrics)
                .push(client.metrics()),
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
                match inbound_grp_msg {
                    FullNodesGroupMessage::PrepareGroup(invite_msg) => {
                        let dest_node_id = invite_msg.validator_id;
                        let resp = client.handle_prepare_group_message(invite_msg);

                        // Send back a response to the validator
                        trace!("RaptorCastSecondary sending back response for group message");
                        this.send_single_msg(
                            FullNodesGroupMessage::PrepareGroupResponse(resp),
                            dest_node_id,
                        );
                    }
                    FullNodesGroupMessage::PrepareGroupResponse(_) => {
                        error!(
                            "RaptorCastSecondary client received a \
                                PrepareGroupResponse message"
                        );
                    }
                    FullNodesGroupMessage::ConfirmGroup(confirm_msg) => {
                        let is_valid = client.handle_confirm_group_message(confirm_msg.clone());
                        if is_valid {
                            // Update peer discovery with peers from confirm group message
                            let num_mappings = confirm_msg.name_records.len();
                            if num_mappings > 0 && num_mappings == confirm_msg.peers.len() {
                                let peers: Vec<PeerEntry<ST>> = confirm_msg
                                    .name_records
                                    .iter()
                                    .zip(confirm_msg.peers.iter())
                                    .map(|(rec, peer)| rec.with_pubkey(peer.pubkey()).into())
                                    .collect();

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
                                        confirm_msg.prepare.end_round,
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
                    }
                }
            }
        }

        ret
    }
}
