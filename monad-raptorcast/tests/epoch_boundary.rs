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

// Epoch-boundary behavior of deterministic (v1) raptorcast.
//
// Receivers validate v1 primary chunks against an ElectedProposerSchedule
// fed through RouterCommand::AddEpochValidatorSet / UpdateCurrentRound, so
// delivery depends on the receiver's knowledge of epoch starts, the round
// leader, and the round window. This exercises the production wiring that
// the unit tests stub out.

use std::{
    collections::{BTreeMap, HashMap},
    net::{Ipv4Addr, SocketAddrV4},
    num::ParseIntError,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{
    driver::PeerDiscoveryDriver,
    mock::{NopDiscovery, NopDiscoveryBuilder},
    MonadNameRecord, NameRecord,
};
use monad_raptorcast::{
    create_dataplane_for_tests, v1_rollout::DeterministicProtocolRolloutStage, DataplaneHandles,
    RaptorCastEvent,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Deserializable, Epoch, NodeId, Round, Serializable, Stake};
use monad_validator::{
    leader_election::LeaderElection,
    proposer_schedule::{BoxedProposerSchedule, ElectedProposerSchedule},
    simple_round_robin::SimpleRoundRobin,
};
use tracing_subscriber::EnvFilter;

const NUM_NODES: usize = 4;
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(10);
const MESSAGE_LEN: usize = 10_000;

const EPOCH_1: Epoch = Epoch(1);
const EPOCH_2: Epoch = Epoch(2);
const EPOCH_1_START: Round = Round(0);
const EPOCH_2_START: Round = Round(100);

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

fn keypair(seed: u8) -> KeyPair {
    KeyPair::from_bytes(&mut [seed; 32]).unwrap()
}

#[derive(Clone, Copy, RlpEncodable, RlpDecodable)]
struct MockMessage {
    id: u32,
    message_len: usize,
}

impl MockMessage {
    fn new(id: u32, message_len: usize) -> Self {
        Self { id, message_len }
    }
}

impl Message for MockMessage {
    type NodeIdPubKey = CertificateSignaturePubKey<SecpSignature>;
    type Event = MockEvent<Self::NodeIdPubKey>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        MockEvent((from, self.id))
    }
}

impl Serializable<Bytes> for MockMessage {
    fn serialize(&self) -> Bytes {
        let mut message = BytesMut::zeroed(self.message_len);
        message[..4].copy_from_slice(&self.id.to_le_bytes());
        message.into()
    }
}

impl Deserializable<Bytes> for MockMessage {
    type ReadError = ParseIntError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        Ok(Self::new(
            u32::from_le_bytes(message[..4].try_into().unwrap()),
            message.len(),
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct MockEvent<P: PubKey>((NodeId<P>, u32));

impl<ST> From<RaptorCastEvent<MockEvent<CertificateSignaturePubKey<ST>>, ST>>
    for MockEvent<CertificateSignaturePubKey<ST>>
where
    ST: CertificateSignatureRecoverable,
{
    fn from(value: RaptorCastEvent<MockEvent<CertificateSignaturePubKey<ST>>, ST>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(_) => unimplemented!(),
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate { .. } => unimplemented!(),
        }
    }
}

type PubKeyType = CertificateSignaturePubKey<SecpSignature>;

#[derive(Clone)]
struct ValidatorInfo {
    keypair: Arc<KeyPair>,
    nodeid: NodeId<PubKeyType>,
}

impl ValidatorInfo {
    fn new(seed: u8) -> Self {
        let kp = keypair(seed);
        let nodeid = NodeId::new(kp.pubkey());
        Self {
            keypair: Arc::new(kp),
            nodeid,
        }
    }

    fn create_name_record(&self, dp: &DataplaneHandles) -> MonadNameRecord<SecpSignature> {
        let name_record = NameRecord::new_with_ports(
            Ipv4Addr::new(127, 0, 0, 1),
            dp.tcp_addr.port(),
            Some(dp.non_auth_addr.port()),
            dp.auth_addr.port(),
            None,
            1,
        );
        MonadNameRecord::new(name_record, &*self.keypair)
    }
}

// Records every event a node emits, so that positive assertions can
// interleave with negative (must-have-been-dropped) assertions.
struct EventLog {
    rx: tokio::sync::mpsc::UnboundedReceiver<MockEvent<PubKeyType>>,
    seen: Vec<(NodeId<PubKeyType>, u32)>,
}

impl EventLog {
    async fn wait_for(&mut self, from: NodeId<PubKeyType>, id: u32) {
        if self.seen.contains(&(from, id)) {
            return;
        }
        loop {
            let event = tokio::time::timeout(MESSAGE_TIMEOUT, self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for message {}", id))
                .expect("event channel closed");
            let MockEvent(seen) = event;
            self.seen.push(seen);
            if seen == (from, id) {
                return;
            }
        }
    }

    fn assert_never_saw(&mut self, id: u32) {
        while let Ok(event) = self.rx.try_recv() {
            let MockEvent(seen) = event;
            self.seen.push(seen);
        }
        assert!(
            !self.seen.iter().any(|(_, seen_id)| *seen_id == id),
            "message {} should have been dropped",
            id
        );
    }
}

fn spawn_validator(
    keypair: Arc<KeyPair>,
    dataplane: DataplaneHandles,
    known_addresses: HashMap<NodeId<PubKeyType>, SocketAddrV4>,
    name_records: HashMap<NodeId<PubKeyType>, MonadNameRecord<SecpSignature>>,
) -> (
    tokio::sync::mpsc::UnboundedSender<RouterCommand<SecpSignature, MockMessage>>,
    EventLog,
) {
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_local(async move {
        let config = monad_raptorcast::config::RaptorCastConfig::<SecpSignature> {
            shared_key: keypair,
            mtu: monad_dataplane::udp::DEFAULT_MTU,
            udp_message_max_age_ms: u64::MAX,
            sig_verification_rate_limit: 10_000,
            primary_instance: Default::default(),
            secondary_instance: monad_node_config::FullNodeRaptorCastConfig {
                enable_publisher: false,
                enable_client: false,
                raptor10_fullnode_redundancy_factor: 2f32,
                full_nodes_prioritized: monad_node_config::FullNodeConfig { identities: vec![] },
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
            // pin v1 semantics regardless of the current rollout stage
            deterministic_protocol_rollout: DeterministicProtocolRolloutStage::AlwaysV1,
        };

        let shared_pd = Arc::new(Mutex::new(PeerDiscoveryDriver::new(NopDiscoveryBuilder {
            known_addresses,
            name_records,
            ..Default::default()
        })));

        let proposer_schedule: BoxedProposerSchedule<PubKeyType> = Box::new(
            ElectedProposerSchedule::new(SimpleRoundRobin::<PubKeyType>::default()),
        );

        let mut validator_rc = monad_raptorcast::RaptorCast::<
            SecpSignature,
            MockMessage,
            MockMessage,
            MockEvent<PubKeyType>,
            NopDiscovery<SecpSignature>,
            monad_raptorcast::auth::NoopAuthProtocol<PubKeyType>,
            monad_raptorcast::auth::NopScore<NodeId<PubKeyType>>,
        >::new(
            config,
            monad_raptorcast::raptorcast_secondary::SecondaryRaptorCastModeConfig::None,
            dataplane.tcp_socket,
            (
                dataplane.authenticated_socket,
                monad_raptorcast::auth::NoopAuthProtocol::new(),
            ),
            None,
            Some(dataplane.non_authenticated_socket),
            dataplane.control,
            shared_pd,
            EPOCH_1,
            proposer_schedule,
        );

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    validator_rc.exec(vec![cmd]);
                }
                Some(event) = validator_rc.next() => {
                    if event_tx.send(event).is_err() {
                        break;
                    }
                }
            }
        }
    });

    (
        cmd_tx,
        EventLog {
            rx: event_rx,
            seen: Vec::new(),
        },
    )
}

fn leader_index(infos: &[ValidatorInfo], round: Round) -> usize {
    let members: BTreeMap<_, _> = infos.iter().map(|v| (v.nodeid, Stake::ONE)).collect();
    let leader = SimpleRoundRobin::default().get_leader(round, &members);
    infos.iter().position(|v| v.nodeid == leader).unwrap()
}

async fn run_epoch_boundary_scenario() {
    let infos: Vec<_> = (1..=NUM_NODES as u8).map(ValidatorInfo::new).collect();
    let dataplanes: Vec<_> = (0..NUM_NODES)
        .map(|_| create_dataplane_for_tests(false))
        .collect();

    let name_records: HashMap<_, _> = infos
        .iter()
        .zip(dataplanes.iter())
        .map(|(v, dp)| (v.nodeid, v.create_name_record(dp)))
        .collect();
    let known_addresses: HashMap<_, _> = infos
        .iter()
        .zip(dataplanes.iter())
        .map(|(v, dp)| (v.nodeid, dp.non_auth_addr))
        .collect();

    let (cmd_txs, mut logs): (Vec<_>, Vec<_>) = infos
        .iter()
        .zip(dataplanes)
        .map(|(v, dp)| {
            spawn_validator(
                v.keypair.clone(),
                dp,
                known_addresses.clone(),
                name_records.clone(),
            )
        })
        .unzip();

    let validator_set: Vec<_> = infos.iter().map(|v| (v.nodeid, Stake::ONE)).collect();
    let add_epoch = |node: usize, epoch: Epoch, epoch_start: Round| {
        cmd_txs[node]
            .send(RouterCommand::AddEpochValidatorSet {
                epoch,
                epoch_start,
                validator_set: validator_set.clone(),
            })
            .unwrap();
    };
    let publish = |from: usize, round: Round, epoch: Epoch, id: u32| {
        cmd_txs[from]
            .send(RouterCommand::Publish {
                target: monad_types::RouterTarget::Raptorcast { round, epoch },
                message: MockMessage::new(id, MESSAGE_LEN),
            })
            .unwrap();
    };

    // wait_all/assert_dropped skip the sender: the router loops every
    // published message back to its author locally.
    async fn wait_all(logs: &mut [EventLog], infos: &[ValidatorInfo], sender: usize, id: u32) {
        for (i, log) in logs.iter_mut().enumerate() {
            if i != sender {
                log.wait_for(infos[sender].nodeid, id).await;
            }
        }
    }
    fn assert_dropped(logs: &mut [EventLog], sender: usize, id: u32) {
        for (i, log) in logs.iter_mut().enumerate() {
            if i != sender {
                log.assert_never_saw(id);
            }
        }
    }

    for node in 0..NUM_NODES {
        add_epoch(node, EPOCH_1, EPOCH_1_START);
    }

    // The epoch-1 leader of a round reaches everyone.
    let sender = leader_index(&infos, Round(5));
    publish(sender, Round(5), EPOCH_1, 1);
    wait_all(&mut logs, &infos, sender, 1).await;

    // Chunks from a non-leader are dropped by check_proposer.
    let bad_sender = (leader_index(&infos, Round(6)) + 1) % NUM_NODES;
    publish(bad_sender, Round(6), EPOCH_1, 2);
    let sender = leader_index(&infos, Round(7));
    publish(sender, Round(7), EPOCH_1, 3);
    wait_all(&mut logs, &infos, sender, 3).await;
    assert_dropped(&mut logs, bad_sender, 2);

    // Chunks for an epoch the receiver has not installed yet are lost:
    // only the sender knows epoch 2 at this point.
    let sender_110 = leader_index(&infos, Round(110));
    add_epoch(sender_110, EPOCH_2, EPOCH_2_START);
    publish(sender_110, Round(110), EPOCH_2, 4);
    let sender = leader_index(&infos, Round(8));
    publish(sender, Round(8), EPOCH_1, 5);
    wait_all(&mut logs, &infos, sender, 5).await;
    assert_dropped(&mut logs, sender_110, 4);

    // Once AddEpochValidatorSet arrives, epoch-2 rounds deliver.
    for node in 0..NUM_NODES {
        if node != sender_110 {
            add_epoch(node, EPOCH_2, EPOCH_2_START);
        }
    }
    let sender = leader_index(&infos, Round(111));
    publish(sender, Round(111), EPOCH_2, 6);
    wait_all(&mut logs, &infos, sender, 6).await;

    // Epoch/round pairing is enforced across the boundary: a round on
    // the wrong side of the epoch start is dropped by check_epoch.
    let sender_95 = leader_index(&infos, Round(95));
    publish(sender_95, Round(95), EPOCH_2, 7); // round belongs to epoch 1
    let sender_112 = leader_index(&infos, Round(112));
    publish(sender_112, Round(112), EPOCH_1, 8); // round belongs to epoch 2
    let sender = leader_index(&infos, Round(96));
    publish(sender, Round(96), EPOCH_1, 9);
    wait_all(&mut logs, &infos, sender, 9).await;
    let sender = leader_index(&infos, Round(113));
    publish(sender, Round(113), EPOCH_2, 10);
    wait_all(&mut logs, &infos, sender, 10).await;
    assert_dropped(&mut logs, sender_95, 7);
    assert_dropped(&mut logs, sender_112, 8);

    // Rounds outside the receive window around the current round are
    // dropped once UpdateCurrentRound is delivered.
    for cmd_tx in &cmd_txs {
        cmd_tx
            .send(RouterCommand::UpdateCurrentRound(EPOCH_2, Round(120)))
            .unwrap();
    }
    let sender = leader_index(&infos, Round(130));
    publish(sender, Round(130), EPOCH_2, 11);
    wait_all(&mut logs, &infos, sender, 11).await;

    let sender_250 = leader_index(&infos, Round(250));
    publish(sender_250, Round(250), EPOCH_2, 12); // > 120 + 100
    let sender = leader_index(&infos, Round(150));
    publish(sender, Round(150), EPOCH_2, 13);
    wait_all(&mut logs, &infos, sender, 13).await;
    assert_dropped(&mut logs, sender_250, 12);
}

#[tokio::test(flavor = "current_thread")]
async fn deterministic_raptorcast_across_epoch_boundary() {
    init_tracing();

    tokio::task::LocalSet::new()
        .run_until(run_epoch_boundary_scenario())
        .await;
}
