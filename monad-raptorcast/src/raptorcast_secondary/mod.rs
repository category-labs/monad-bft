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

use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use client::Client;
use futures::Stream;
use group_message::FullNodesGroupMessage;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{udp::segment_size_for_mtu, Dataplane, UnicastMsg};
// use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_types::{Deserializable, DropTimer, Epoch, NodeId, Serializable};
use publisher::Publisher;

use super::{
    config::RaptorCastConfig,
    message::OutboundRouterMessage,
    udp,
    util::{BuildTarget, FullNodes, Group},
    RaptorCastEvent,
};
use crate::metrics::RaptorCastMetrics;

mod client;
pub mod group_message;
mod publisher;

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

pub struct RaptorCastSecondary<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Into<M> + Clone + Encodable,
{
    // Our id, regardless of role as publisher (validator) or client (full-node)
    node_id: NodeId<CertificateSignaturePubKey<ST>>,

    // Main state machine, depending on wether we are playing the publisher role
    // (i.e. we are a validator) or a client role (full-node raptor-casted to)
    // Represents only the group logic, excluding everything network related.
    role: Role<ST>,

    // Args for encoding outbound (validator -> full-node) messages
    signing_key: Arc<ST::KeyPairType>, // for re-signing app messages
    raptor10_redundancy: u8,
    curr_epoch: Epoch,

    // These are populated from inbound group confirmation messages.
    // Self SockAddr is removed.
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,

    mtu: u16,
    dataplane: Arc<Mutex<Dataplane>>,
    pending_events: VecDeque<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
    channel_from_primary: Receiver<FullNodesGroupMessage<ST>>,
    waker: Option<Waker>,
    metrics: RaptorCastMetrics,
    _phantom: PhantomData<(OM, SE)>,
}

impl<ST, M, OM, SE> RaptorCastSecondary<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Into<M> + Clone + Encodable,
{
    pub fn new(
        config: &RaptorCastConfig<ST>,
        shared_dataplane: Arc<Mutex<Dataplane>>,
        channel_from_primary: Receiver<FullNodesGroupMessage<ST>>,
        channel_to_primary: Sender<Group<ST>>,
    ) -> Self {
        let node_id = NodeId::new(config.shared_key.pubkey());
        let sec_config = config.secondary_instance.clone();
        let mut raptor10_redundancy = 0;

        // Instantiate either publisher or client state machine
        let role = match sec_config {
            super::config::SecondaryRaptorCastModeConfig::Publisher(publisher_cfg) => {
                raptor10_redundancy = publisher_cfg.raptor10_redundancy;
                let publisher = Publisher::new(node_id, publisher_cfg);
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

        Self {
            node_id,
            role,
            signing_key: config.shared_key.clone(),
            raptor10_redundancy,
            curr_epoch: Epoch(0),
            known_addresses: config.known_addresses.clone(),
            mtu: config.mtu,
            dataplane: shared_dataplane,
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
        app_message: Bytes,
        mtu: u16,
        signing_key: &Arc<ST::KeyPairType>,
        redundancy: u8,
        known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    ) -> UnicastMsg {
        let app_message_len = app_message.len();
        let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
            tracing::warn!(?elapsed, app_message_len, "long time to udp_build")
        });
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

    fn send_single_msg(
        &self,
        group_msg: FullNodesGroupMessage<ST>,
        dest_node: &NodeId<CertificateSignaturePubKey<ST>>,
    ) {
        let router_msg: OutboundRouterMessage<OM, ST> =
            OutboundRouterMessage::FullNodesGroup(group_msg);
        let msg_bytes = match router_msg.try_serialize() {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(
                    "RaptorCastSecondary failed to serialize during send_single_msg: {:?}",
                    e
                );
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
            &self.known_addresses,
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
    ) {
        let _timer = DropTimer::start(Duration::from_millis(100), |elapsed| {
            tracing::warn!(?elapsed, "long time to send_group_msg")
        });
        let group_msg = self.try_fill_name_records(group_msg);
        // Can udp_write_broadcast() be used? Optimize later
        for nid in dest_node_ids.list {
            self.send_single_msg(group_msg.clone(), &nid);
        }
    }

    fn try_fill_name_records(
        &self,
        group_msg: FullNodesGroupMessage<ST>,
    ) -> FullNodesGroupMessage<ST> {
        group_msg
        // TODO: ask peer recovery for ConfirmGroup::name_records
    }
}

impl<ST, M, OM, SE> Executor for RaptorCastSecondary<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Into<M> + Clone + Encodable,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;
    type Metrics = RaptorCastMetrics;

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
                        client.enter_round(round);
                    }
                    Role::Publisher(publisher) => {
                        self.curr_epoch = epoch;
                        if let Some((group_msg, fn_set)) =
                            publisher.enter_round_and_step_until(round)
                        {
                            self.send_group_msg(group_msg, fn_set);
                        }
                    }
                },

                Self::Command::PublishToFullNodes { epoch, message } => {
                    let app_message = message.serialize();
                    let app_message_len = app_message.len();

                    let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                        tracing::warn!(?elapsed, app_message_len, "long time to publish message")
                    });

                    let curr_group: &Group<ST> = match &mut self.role {
                        Role::Client(_) => {
                            continue;
                        }
                        Role::Publisher(publisher) => {
                            match publisher.get_current_raptorcast_group() {
                                Some(group) => group,
                                None => continue,
                            }
                        }
                    };

                    let build_target = BuildTarget::FullNodeRaptorCast(curr_group);

                    // Split app_message into raptorcast chunks that we can
                    // send to full nodes.
                    let rc_chunks = {
                        let segment_size = segment_size_for_mtu(self.mtu);
                        let unix_ts_ms = std::time::UNIX_EPOCH
                            .elapsed()
                            .expect("time went backwards")
                            .as_millis()
                            .try_into()
                            .expect("unix epoch doesn't fit in u64");
                        let messages = udp::build_messages::<ST>(
                            &self.signing_key,
                            segment_size,
                            app_message,
                            self.raptor10_redundancy,
                            epoch.0, // gets embedded into raptorcast message
                            unix_ts_ms,
                            build_target,
                            &self.known_addresses,
                        );

                        UnicastMsg {
                            msgs: messages,
                            stride: segment_size,
                        }
                    };

                    // Send the raptorcast chunks via UDP to all peers in group
                    self.dataplane.lock().unwrap().udp_write_unicast(rc_chunks);
                }
            }
        }
    }

    fn metrics(&self) -> &Self::Metrics {
        &self.metrics
    }
}
impl<ST, M, OM, E> Stream for RaptorCastSecondary<ST, M, OM, E>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes> + Decodable,
    OM: Serializable<Bytes> + Into<M> + Clone + Encodable,
    E: From<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
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
                    // Received group message from validator
                    if let Some((response_msg, validator_id)) =
                        client.on_receive_group_message(inbound_grp_msg)
                    {
                        // Send back a response to the validator
                        this.send_single_msg(response_msg, &validator_id);
                    }
                }
            },

            Err(err) => {
                if let std::sync::mpsc::TryRecvError::Disconnected = err {
                    tracing::error!("RaptorCastSecondary channel disconnected.");
                }
            }
        }

        Poll::Pending
    }
}
