use std::{collections::BTreeMap, fmt::Debug, time::Duration, vec};

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::BlockType, quorum_certificate::genesis_vote_info, transaction_validator::MockValidator,
};
use monad_crypto::{
    hasher::Sha256Hash,
    secp256k1::{KeyPair, PubKey},
};
use monad_eth_types::EthAddress;
use monad_executor::{replay_nodes::ReplayNodes, timed_event::TimedEvent, State};
use monad_executor_glue::PeerId;
use monad_mock_swarm::swarm_relation::SwarmRelation;
use monad_state::MonadConfig;
use monad_testutil::{signing::get_genesis_config, validators::create_keys_w_validators};
use monad_types::{NodeId, Round};

use crate::{
    graph::{Graph, NodeEvent, NodeState, ReplayConfig},
    VizSwarm,
};

#[derive(Debug, Clone)]
pub struct RepConfig {
    pub num_nodes: u32,
    pub max_tick: Duration,
    pub delta: Duration,
}

type MS = <VizSwarm as SwarmRelation>::State;

impl ReplayConfig<MS> for RepConfig {
    fn nodes(&self) -> Vec<(PubKey, <MS as State>::Config)> {
        let (keys, cert_keys, _validators, validator_mapping) = create_keys_w_validators::<
            <VizSwarm as SwarmRelation>::SignatureCollectionType,
        >(self.num_nodes);
        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();

        let voting_keys = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(cert_keys.iter())
            .collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<
            Sha256Hash,
            <VizSwarm as SwarmRelation>::SignatureCollectionType,
            <VizSwarm as SwarmRelation>::TransactionValidator,
        >(
            voting_keys.iter(),
            &validator_mapping,
            &MockValidator::default(),
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
                    epoch_length: Round(100),
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

pub struct ReplayNodesSimulation<S, C>
where
    S: SwarmRelation,
    C: ReplayConfig<S::State>,
{
    pub nodes: ReplayNodes<S::State>,
    pub current_tick: Duration,
    config: C,
    pub replay_events: BTreeMap<PeerId, Vec<TimedEvent<<S::State as State>::Event>>>,
}

impl<S, C> ReplayNodesSimulation<S, C>
where
    S: SwarmRelation,
    C: ReplayConfig<S::State>,
{
    pub fn new(
        config: C,
        replay_events: BTreeMap<PeerId, Vec<TimedEvent<<S::State as State>::Event>>>,
    ) -> Self {
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
    ) -> Vec<NodeEvent<PeerId, <S::State as State>::Message, <S::State as State>::Event>> {
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

impl<S, C> Graph for ReplayNodesSimulation<S, C>
where
    S: SwarmRelation,
    C: ReplayConfig<S::State>,
{
    type State = S::State;
    type InboundMessage = S::InboundMessage;
    type NodeId = PeerId;
    type Swarm = S;

    fn state(&self) -> Vec<NodeState<Self::NodeId, Self::Swarm, Self::InboundMessage>> {
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
