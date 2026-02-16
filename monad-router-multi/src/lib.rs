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
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable};
use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{DataplaneBuilder, TcpSocketId, UdpSocketId};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_node_config::{FullNodeConfig, FullNodeIdentityConfig};
use monad_peer_discovery::{
    driver::PeerDiscoveryDriver, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
};
use monad_raptorcast::{
    auth::AuthenticationProtocol,
    config::{
        GroupSchedulingConfig, RaptorCastConfig, RaptorCastConfigSecondary,
        RaptorCastConfigSecondaryClient, RaptorCastConfigSecondaryPublisher,
        SecondaryRaptorCastMode,
    },
    raptorcast_secondary::{
        group_message::FullNodesGroupMessage, RaptorCastSecondary, SecondaryOutboundMessage,
        SecondaryRaptorCastModeConfig,
    },
    util::Group,
    RaptorCast, RaptorCastEvent,
};
use monad_types::{Epoch, NodeId};
use tokio::sync::mpsc::{channel, Receiver, Sender};
pub use tracing::{debug, error, info, warn, Level};

//==============================================================================
pub struct MultiRouter<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    rc_primary: RaptorCast<ST, M, OM, SE, PD, AP>,
    rc_secondary: Option<RaptorCastSecondary<ST, M, OM, SE, PD>>,

    // raptorcast config is stored for future role change
    rc_config: RaptorCastConfig<ST>,
    self_node_id: NodeId<CertificateSignaturePubKey<ST>>,
    current_epoch: Epoch,
    epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,

    shared_pdd: Arc<Mutex<PeerDiscoveryDriver<PD>>>,

    phantom: PhantomData<(OM, SE)>,
}

const RAPTORCAST_BRIDGE_CHANNEL_CAPACITY: usize = 8 * 1024;

impl<ST, M, OM, SE, PD, AP> MultiRouter<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new<B>(
        self_node_id: NodeId<CertificateSignaturePubKey<ST>>,
        cfg: RaptorCastConfig<ST>,
        dataplane_builder: DataplaneBuilder,
        peer_discovery_builder: B,
        current_epoch: Epoch,
        epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
        auth_protocol: AP,
    ) -> Self
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>,
    {
        // Peer discovery needs to be shared among primary and secondary
        let pdd = PeerDiscoveryDriver::new(peer_discovery_builder);
        let shared_pdd = Arc::new(Mutex::new(pdd));

        let mut dp = dataplane_builder.build();
        assert!(dp.block_until_ready(Duration::from_secs(1)));

        let tcp_socket = dp.tcp_sockets.take(TcpSocketId::Raptorcast).unwrap();
        let authenticated_socket = dp.udp_sockets.take(UdpSocketId::AuthenticatedRaptorcast);
        let non_authenticated_socket = dp
            .udp_sockets
            .take(UdpSocketId::Raptorcast)
            .expect("raptorcast socket");
        let control = dp.control;

        // Create channels between primary and secondary raptorcast instances.
        // Fundamentally this is needed because, while both can send, only the
        // primary can receive data from the network.
        let (send_net_messages, recv_net_messages) =
            channel::<FullNodesGroupMessage<ST>>(RAPTORCAST_BRIDGE_CHANNEL_CAPACITY);
        let (send_group_infos, recv_group_infos) =
            channel::<Group<CertificateSignaturePubKey<ST>>>(RAPTORCAST_BRIDGE_CHANNEL_CAPACITY);
        let (send_outbound_to_primary, recv_outbound_from_secondary) =
            channel::<SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>>(
                RAPTORCAST_BRIDGE_CHANNEL_CAPACITY,
            );

        // Determine initial secondary raptorcast role
        let is_current_epoch_validator = epoch_validators
            .get(&current_epoch)
            .is_some_and(|set| set.contains(&self_node_id));
        let secondary_mode =
            if is_current_epoch_validator && cfg.secondary_instance.enable_publisher {
                SecondaryRaptorCastModeConfig::Publisher
            } else if !is_current_epoch_validator && cfg.secondary_instance.enable_client {
                SecondaryRaptorCastModeConfig::Client
            } else {
                SecondaryRaptorCastModeConfig::None
            };

        // Instantiate secondary raptorcast instance
        let rc_secondary = Self::build_secondary(
            cfg.clone(),
            secondary_mode,
            shared_pdd.clone(),
            recv_net_messages,
            send_group_infos,
            send_outbound_to_primary,
            current_epoch,
        );

        let mut rc_primary = RaptorCast::new(
            cfg.clone(),
            secondary_mode,
            tcp_socket,
            authenticated_socket,
            non_authenticated_socket,
            control,
            shared_pdd.clone(),
            current_epoch,
            auth_protocol,
        );
        rc_primary.bind_channel_to_secondary_raptorcast(
            send_net_messages,
            recv_group_infos,
            recv_outbound_from_secondary,
        );

        Self {
            rc_primary,
            rc_secondary,
            rc_config: cfg,
            current_epoch,
            epoch_validators,
            self_node_id,
            shared_pdd,
            phantom: PhantomData,
        }
    }

    fn update_role(&mut self, current_epoch: Epoch, new_role: SecondaryRaptorCastModeConfig) {
        debug!(
            ?new_role,
            ?current_epoch,
            "Updating secondary raptorcast role"
        );

        // create new channels
        let (send_net_messages, recv_net_messages) = channel(RAPTORCAST_BRIDGE_CHANNEL_CAPACITY);
        let (send_group_infos, recv_group_infos) = channel(RAPTORCAST_BRIDGE_CHANNEL_CAPACITY);
        let (send_outbound_to_primary, recv_outbound_from_secondary) =
            channel(RAPTORCAST_BRIDGE_CHANNEL_CAPACITY);

        let is_dynamic = matches!(new_role, SecondaryRaptorCastModeConfig::Client);
        // we first need to update is_dynamic_full_node before binding the channels
        self.rc_primary.set_is_dynamic_full_node(is_dynamic);
        self.rc_primary.bind_channel_to_secondary_raptorcast(
            send_net_messages,
            recv_group_infos,
            recv_outbound_from_secondary,
        );

        let rc_secondary = Self::build_secondary(
            self.rc_config.clone(),
            new_role,
            self.shared_pdd.clone(),
            recv_net_messages,
            send_group_infos,
            send_outbound_to_primary,
            current_epoch,
        );
        self.rc_secondary = rc_secondary;
    }

    fn build_secondary(
        cfg: RaptorCastConfig<ST>,
        mode: SecondaryRaptorCastModeConfig,
        shared_pdd: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
        recv_net_messages: Receiver<FullNodesGroupMessage<ST>>,
        send_group_infos: Sender<Group<CertificateSignaturePubKey<ST>>>,
        channel_to_primary_outbound: Sender<
            SecondaryOutboundMessage<CertificateSignaturePubKey<ST>>,
        >,
        current_epoch: Epoch,
    ) -> Option<RaptorCastSecondary<ST, M, OM, SE, PD>> {
        let secondary_instance: RaptorCastConfigSecondary<CertificateSignaturePubKey<ST>> =
            match mode {
                SecondaryRaptorCastModeConfig::None => {
                    debug!("Configured with Secondary RaptorCast instance: None");
                    RaptorCastConfigSecondary::default()
                }
                SecondaryRaptorCastModeConfig::Client => {
                    debug!("Configured with Secondary RaptorCast instance: Client");
                    RaptorCastConfigSecondary {
                        raptor10_redundancy: cfg
                            .secondary_instance
                            .raptor10_fullnode_redundancy_factor,
                        mode: SecondaryRaptorCastMode::Client(RaptorCastConfigSecondaryClient {
                            max_num_group: cfg.secondary_instance.max_num_group,
                            max_group_size: cfg.secondary_instance.max_group_size,
                            invite_future_dist_min: cfg.secondary_instance.invite_future_dist_min,
                            invite_future_dist_max: cfg.secondary_instance.invite_future_dist_max,
                            invite_accept_heartbeat: Duration::from_millis(
                                cfg.secondary_instance.invite_accept_heartbeat_ms,
                            ),
                        }),
                    }
                }
                SecondaryRaptorCastModeConfig::Publisher => {
                    debug!("Configured with Secondary RaptorCast instance: Publisher");
                    let full_nodes_prioritized: Vec<NodeId<CertificateSignaturePubKey<ST>>> = cfg
                        .secondary_instance
                        .full_nodes_prioritized
                        .identities
                        .iter()
                        .map(|id| NodeId::new(id.secp256k1_pubkey))
                        .collect();

                    RaptorCastConfigSecondary {
                        raptor10_redundancy: cfg
                            .secondary_instance
                            .raptor10_fullnode_redundancy_factor,
                        mode: SecondaryRaptorCastMode::Publisher(
                            RaptorCastConfigSecondaryPublisher {
                                full_nodes_prioritized,
                                group_scheduling: GroupSchedulingConfig {
                                    max_group_size: cfg.secondary_instance.max_group_size,
                                    round_span: cfg.secondary_instance.round_span,
                                    invite_lookahead: cfg.secondary_instance.invite_lookahead,
                                    max_invite_wait: cfg.secondary_instance.max_invite_wait,
                                    deadline_round_dist: cfg.secondary_instance.deadline_round_dist,
                                    init_empty_round_span: cfg
                                        .secondary_instance
                                        .init_empty_round_span,
                                },
                            },
                        ),
                    }
                }
            };

        match secondary_instance.mode {
            SecondaryRaptorCastMode::None => None,
            _ => Some(RaptorCastSecondary::new(
                cfg,
                secondary_instance.mode,
                shared_pdd,
                recv_net_messages,
                send_group_infos,
                channel_to_primary_outbound,
                current_epoch,
            )),
        }
    }
}

//==============================================================================
impl<ST, M, OM, SE, PD, AP> Executor for MultiRouter<ST, M, OM, SE, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    RaptorCast<ST, M, OM, SE, PD, AP>: Unpin,
{
    type Command = RouterCommand<ST, OM>;

    fn exec(&mut self, a_commands: Vec<Self::Command>) {
        let mut validator_cmds = Vec::new();
        let mut fullnodes_cmds = Vec::new();
        for cmd in a_commands {
            match cmd {
                RouterCommand::Publish { .. } => validator_cmds.push(cmd),
                RouterCommand::PublishWithPriority { .. } => validator_cmds.push(cmd),
                RouterCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    let cmd_cpy = RouterCommand::AddEpochValidatorSet {
                        epoch,
                        validator_set: validator_set.clone(),
                    };
                    debug!(?validator_set, "Updating validator set in multi router");
                    let validator_set = validator_set.iter().map(|(id, _)| *id).collect();
                    self.epoch_validators.insert(epoch, validator_set);

                    // if self is a validator and will fall out in the next epoch validator set
                    // switch role to Client in secondary raptorcast to start connecting to upstream
                    if epoch == self.current_epoch + Epoch(1)
                        && self
                            .epoch_validators
                            .get(&self.current_epoch)
                            .is_some_and(|set| set.contains(&self.self_node_id))
                        && !self
                            .epoch_validators
                            .get(&(self.current_epoch + Epoch(1)))
                            .is_some_and(|set| set.contains(&self.self_node_id))
                    {
                        if self.rc_config.secondary_instance.enable_client {
                            // validator demoted to full node, update role to be Client
                            self.update_role(
                                self.current_epoch,
                                SecondaryRaptorCastModeConfig::Client,
                            );
                        } else {
                            // demoted to full node but Client mode not enabled, disable secondary raptorcast
                            self.update_role(
                                self.current_epoch,
                                SecondaryRaptorCastModeConfig::None,
                            );
                        }
                    }

                    validator_cmds.push(cmd_cpy);
                }
                RouterCommand::GetPeers => validator_cmds.push(cmd),
                RouterCommand::UpdatePeers { .. } => validator_cmds.push(cmd),

                RouterCommand::PublishToFullNodes {
                    epoch,
                    round,
                    ref message,
                } => {
                    let cmd_cpy = RouterCommand::PublishToFullNodes {
                        epoch,
                        round,
                        message: message.clone(),
                    };
                    validator_cmds.push(cmd_cpy);
                    fullnodes_cmds.push(cmd);
                }
                RouterCommand::GetFullNodes => validator_cmds.push(cmd),
                RouterCommand::UpdateFullNodes {
                    ref dedicated_full_nodes,
                    ref prioritized_full_nodes,
                } => {
                    self.rc_config.primary_instance.fullnode_dedicated =
                        dedicated_full_nodes.clone();
                    self.rc_config.secondary_instance.full_nodes_prioritized = FullNodeConfig {
                        identities: prioritized_full_nodes
                            .iter()
                            .map(|id| FullNodeIdentityConfig {
                                secp256k1_pubkey: id.pubkey(),
                            })
                            .collect(),
                    };
                    let cmd_cpy = RouterCommand::UpdateFullNodes {
                        dedicated_full_nodes: dedicated_full_nodes.clone(),
                        prioritized_full_nodes: prioritized_full_nodes.clone(),
                    };
                    validator_cmds.push(cmd_cpy);
                    fullnodes_cmds.push(cmd);
                }
                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    let cmd_cpy = RouterCommand::UpdateCurrentRound(epoch, round);
                    if epoch > self.current_epoch {
                        // epoch increment
                        debug!(?self.current_epoch, ?epoch, "Epoch incremented in multi router");
                        let is_validator_before_epoch_increment = self
                            .epoch_validators
                            .get(&self.current_epoch)
                            .is_some_and(|set| set.contains(&self.self_node_id));
                        debug!(
                            ?is_validator_before_epoch_increment,
                            "Is validator before epoch increment"
                        );
                        self.current_epoch = epoch;

                        let is_validator_after_epoch_increment = self
                            .epoch_validators
                            .get(&self.current_epoch)
                            .is_some_and(|set| set.contains(&self.self_node_id));
                        debug!(
                            ?is_validator_after_epoch_increment,
                            "Is validator after epoch increment"
                        );

                        // check if secondary raptorcast role needs to be updated
                        if !is_validator_before_epoch_increment
                            && is_validator_after_epoch_increment
                        {
                            if self.rc_config.secondary_instance.enable_publisher {
                                // full node promoted to validator, update role to be Publisher
                                self.update_role(epoch, SecondaryRaptorCastModeConfig::Publisher);
                            } else {
                                // promoted to validator but Publisher mode not enabled, disable secondary raptorcast
                                self.update_role(epoch, SecondaryRaptorCastModeConfig::None);
                            }
                        }

                        // clean old epoch validators
                        self.epoch_validators
                            .retain(|epoch, _| *epoch + Epoch(1) >= self.current_epoch);
                    }
                    validator_cmds.push(cmd);
                    fullnodes_cmds.push(cmd_cpy);
                }
            }
        }
        self.rc_primary.exec(validator_cmds);
        if self.rc_secondary.is_some() {
            self.rc_secondary.as_mut().unwrap().exec(fullnodes_cmds);
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        let m1 = self.rc_primary.metrics();
        let res: ExecutorMetricsChain = if self.rc_secondary.is_some() {
            let m2 = self.rc_secondary.as_ref().unwrap().metrics();
            m1.chain(m2)
        } else {
            m1
        };
        res
    }
}

//==============================================================================
impl<ST, M, OM, E, PD, AP> Stream for MultiRouter<ST, M, OM, E, PD, AP>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, ST>>,
    Self: Unpin,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    RaptorCast<ST, M, OM, E, PD, AP>: Unpin,
    RaptorCastSecondary<ST, M, OM, E, PD>: Unpin,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    PeerDiscoveryDriver<PD>: Unpin,
{
    type Item = E;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let pinned_this = self.as_mut().get_mut();

        // Primary RC instance polls for inbound TCP, UDP raptorcast
        // and FullNodesGroupMessage intended for the secondary RC instance.
        match pinned_this.rc_primary.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
            Poll::Ready(None) => {
                error!("Primary RaptorCast stream ended unexpectedly.");
            }
            Poll::Pending => {}
        }

        // Secondary RC instance polls for FullNodesGroupMessage coming in from
        // the Channel Primary->Secondary.
        if self.rc_secondary.is_some() {
            let fn_stream = self.rc_secondary.as_mut().unwrap();
            match fn_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
                Poll::Ready(None) => {
                    error!("Secondary RaptorCast stream ended unexpectedly.");
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}
