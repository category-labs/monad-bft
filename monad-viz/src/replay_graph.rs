use std::{collections::BTreeMap, fmt::Debug, time::Duration, vec};

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature, quorum_certificate::genesis_vote_info,
    signature_collection::SignatureCollection, transaction_validator::MockValidator,
    validation::Sha256Hash,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_eth_types::EthAddress;
use monad_executor::{replay_nodes::ReplayNodes, timed_event::TimedEvent, State};
use monad_executor_glue::{Identifiable, MonadEvent, PeerId};
use monad_state::MonadConfig;
use monad_testutil::{signing::get_genesis_config, validators::create_keys_w_validators};
use monad_types::NodeId;

use crate::{
    graph::{Graph, NodeEvent, NodeState, ReplayConfig},
    SignatureCollectionType, TransactionValidatorType, MS,
};

#[derive(Debug, Clone)]
pub struct RepConfig {
    pub num_nodes: u32,
    pub max_tick: Duration,
    pub delta: Duration,
}

impl ReplayConfig<MS> for RepConfig {
    fn nodes(&self) -> Vec<(PubKey, <MS as State>::Config)> {
        let (keys, cert_keys, _validators, validator_mapping) =
            create_keys_w_validators::<SignatureCollectionType>(self.num_nodes);
        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();

        let voting_keys = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(cert_keys.iter())
            .collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) =
            get_genesis_config::<Sha256Hash, SignatureCollectionType, TransactionValidatorType>(
                voting_keys.iter(),
                &validator_mapping,
                &TransactionValidatorType::default(),
            );

        let state_configs = keys
            .into_iter()
            .zip(cert_keys)
            .map(|(key, certkey)| MonadConfig {
                transaction_validator: MockValidator,
                key,
                certkey,
                beneficiary: EthAddress::default(),
                validators: validator_mapping
                    .map
                    .iter()
                    .map(|(node_id, sctpubkey)| (node_id.0, *sctpubkey))
                    .collect::<Vec<_>>(),
                delta: self.delta,
                consensus_config: ConsensusConfig {
                    proposal_size: 5000,
                    state_root_delay: 0,
                    propose_with_missing_blocks: false,
                },
                genesis_block: genesis_block.clone(),
                genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
                genesis_signatures: genesis_sigs.clone(),
            })
            .collect::<Vec<_>>();
        pubkeys.into_iter().zip(state_configs).collect::<Vec<_>>()
    }

    fn max_tick(&self) -> Duration {
        self.max_tick
    }
}

pub struct ReplayNodesSimulation<S, C, ST, SCT>
where
    S: monad_executor::State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    C: ReplayConfig<S>,
{
    pub nodes: ReplayNodes<S>,
    pub current_tick: Duration,
    config: C,
    pub replay_events: BTreeMap<PeerId, Vec<TimedEvent<S::Event>>>,
}

impl<S, C, ST, SCT> ReplayNodesSimulation<S, C, ST, SCT>
where
    S: monad_executor::State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    // S::Event: Serializable<Vec<u8>> + Deserializable<[u8]> + Debug + Eq,
    C: ReplayConfig<S>,
{
    pub fn new(config: C, replay_events: BTreeMap<PeerId, Vec<TimedEvent<S::Event>>>) -> Self {
        Self {
            nodes: ReplayNodes::new(config.nodes()),
            current_tick: Duration::ZERO,
            replay_events,
            config,
        }
    }

    pub fn config(&self) -> &C {
        &self.config
    }

    fn reset(&mut self) {
        self.nodes = ReplayNodes::new(self.config.nodes());
        self.current_tick = self.min_tick();
    }

    fn get_pending_events(
        &self,
        node_id: &PeerId,
    ) -> Vec<NodeEvent<PeerId, <S as State>::Message, S::Event>> {
        let mut nes = vec![];
        for pmsg in self
            .nodes
            .replay_nodes_info
            .get(node_id)
            .unwrap()
            .pending_messages()
        {
            let tx_id = &pmsg.send_id;
            let tx_time = pmsg.send_tick;
            let msg = &pmsg.message;
            let event = pmsg.event.clone();
            for evt in self.replay_events.get(node_id).unwrap() {
                if self.tick() > tx_time && self.tick() < evt.timestamp && evt.event == event {
                    nes.push(NodeEvent::Message {
                        tx_time,
                        rx_time: evt.timestamp,
                        tx_peer: tx_id,
                        message: msg,
                    });
                }
            }
        }
        nes
    }
}

impl<S, C, ST, SCT> Graph for ReplayNodesSimulation<S, C, ST, SCT>
where
    S: monad_executor::State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    // S::Event: Serializable<Vec<u8>> + Deserializable<[u8]> + Eq + Debug,
    C: ReplayConfig<S>,
{
    type State = S;
    type Message = S::Message;
    type MessageId = <S::Message as Identifiable>::Id;
    type Event = S::Event;
    type NodeId = PeerId;

    fn state(&self) -> Vec<NodeState<Self::NodeId, S, S::Message, S::Event>> {
        let mut state = self
            .nodes
            .replay_nodes_info
            .iter()
            .map(|(peer_id, replay_node_info)| NodeState {
                id: peer_id,
                state: replay_node_info.state(),
                pending_events: self.get_pending_events(peer_id),
            })
            .collect::<Vec<_>>();
        state.sort_by_key(|node| node.id);
        state
    }

    fn tick(&self) -> Duration {
        self.current_tick
    }

    fn min_tick(&self) -> Duration {
        Duration::from_secs(0)
    }

    fn max_tick(&self) -> Duration {
        self.config.max_tick()
    }

    fn set_tick(&mut self, tick: Duration) {
        self.reset();
        for (pid, tevents) in self.replay_events.iter() {
            for (i, tevent) in tevents.iter().enumerate() {
                if (i < tevents.len() - 1) && (tevents[i].timestamp > tick) {
                    break;
                }
                self.nodes.step(pid, tevent.event.clone(), tevent.timestamp);
            }
        }
        self.current_tick = tick
    }
}
