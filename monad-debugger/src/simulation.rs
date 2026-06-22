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
    collections::{BTreeSet, VecDeque},
    time::Duration,
};

use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema, ServerError};
use monad_crypto::{certificate_signature::PubKey, NopPubKey};
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock_swarm::{Nodes, SwarmBuilder},
    swarm_relation::{DebugSwarmRelation, SwarmRelation},
    terminator::{NodesTerminator, UntilTerminator},
};
use monad_transformer::ID;
use monad_types::NodeId;

use crate::{
    graphql::{GraphQLRoot, GraphQLSimulation},
    network::{
        NetworkCommand, NetworkCommandAction, NetworkConfig, NetworkPipeline,
        DEFAULT_LINK_LATENCY_MS,
    },
};

pub struct Simulation {
    pub(crate) current_tick: Duration,
    pub(crate) swarm: Nodes<DebugSwarmRelation>,
    pub(crate) event_log: VecDeque<(
        Duration,
        ID<NopPubKey>,
        MonadEvent<
            <DebugSwarmRelation as SwarmRelation>::SignatureType,
            <DebugSwarmRelation as SwarmRelation>::SignatureCollectionType,
            <DebugSwarmRelation as SwarmRelation>::ExecutionProtocolType,
        >,
    )>,
    event_cache_size: usize,
    pub(crate) network_config: NetworkConfig,
    pub(crate) network_command_log: Vec<NetworkCommand>,
    applied_network_commands: BTreeSet<u64>,
    next_network_command_sequence: u64,
    config: Box<dyn Fn() -> SwarmBuilder<DebugSwarmRelation>>,
    schema: Schema<GraphQLRoot, EmptyMutation, EmptySubscription>,
}

impl Simulation {
    pub fn new(config: Box<dyn Fn() -> SwarmBuilder<DebugSwarmRelation>>) -> Self {
        let mut swarm = config().build();
        let network_config = Self::default_network_config(&swarm);
        Self::install_network_pipeline(&mut swarm, &network_config);

        Self {
            current_tick: Duration::ZERO,
            swarm,
            event_log: Default::default(),
            event_cache_size: 100,
            network_config,
            network_command_log: Vec::new(),
            applied_network_commands: BTreeSet::new(),
            next_network_command_sequence: 0,
            config,
            schema: Schema::new(GraphQLRoot, EmptyMutation, EmptySubscription),
        }
    }

    pub fn schema(&self) -> String {
        self.schema.sdl()
    }

    pub fn execute_query(&self, query: &str) -> Result<serde_json::Value, Vec<ServerError>> {
        let state = GraphQLSimulation(self);
        let request = Request::new(query).data(state);
        let response = futures::executor::block_on(self.schema.execute(request)).into_result()?;
        Ok(response.data.into_json().unwrap())
    }

    pub fn set_tick(&mut self, tick: Duration) {
        if tick < self.current_tick {
            self.replay_to_tick(tick);
            return;
        }
        assert!(tick >= self.current_tick);
        self.advance_to_tick(tick);
    }

    pub fn reset(&mut self) {
        self.network_command_log.clear();
        self.applied_network_commands.clear();
        self.next_network_command_sequence = 0;
        self.rebuild_swarm();
    }

    pub fn set_default_latency(&mut self, latency_ms: i32) -> Result<(), String> {
        let latency = duration_from_latency_ms(latency_ms)?;
        self.record_network_command(NetworkCommandAction::SetDefaultLatency { latency })
    }

    pub fn set_link_latency(
        &mut self,
        from: &str,
        to: &str,
        latency_ms: i32,
    ) -> Result<(), String> {
        let from = parse_node_id(from)?;
        let to = parse_node_id(to)?;
        let latency = duration_from_latency_ms(latency_ms)?;
        self.record_network_command(NetworkCommandAction::SetLinkLatency { from, to, latency })
    }

    pub fn set_link_dropped(&mut self, from: &str, to: &str, dropped: bool) -> Result<(), String> {
        let from = parse_node_id(from)?;
        let to = parse_node_id(to)?;
        self.record_network_command(NetworkCommandAction::SetLinkDropped { from, to, dropped })
    }

    pub fn clear_link_rule(&mut self, from: &str, to: &str) -> Result<(), String> {
        let from = parse_node_id(from)?;
        let to = parse_node_id(to)?;
        self.record_network_command(NetworkCommandAction::ClearLinkRule { from, to })
    }

    pub fn set_node_position(&mut self, node: &str, x: i32, y: i32) -> Result<(), String> {
        let node = parse_node_id(node)?;
        self.record_network_command(NetworkCommandAction::SetNodePosition { node, x, y })
    }

    pub fn set_node_online(&mut self, node: &str, online: bool) -> Result<(), String> {
        let node = parse_node_id(node)?;
        self.record_network_command(NetworkCommandAction::SetNodeOnline { node, online })
    }

    fn advance_to_tick(&mut self, tick: Duration) {
        while let Some(command) = self.next_unapplied_network_command_until(tick) {
            if command.tick > self.current_tick {
                self.step_swarm_until(UntilTerminator::new().until_tick(command.tick));
                self.current_tick = command.tick;
            }
            self.apply_network_command(&command)
                .expect("recorded network command must replay successfully");
            self.applied_network_commands.insert(command.sequence);
        }

        let term = UntilTerminator::new().until_tick(tick);
        self.step_swarm_until(term);
        self.current_tick = tick;
    }

    pub fn step(&mut self) {
        let next_event_tick = self.swarm.peek_tick();
        let next_command = self.next_unapplied_network_command();

        match (next_event_tick, next_command) {
            (Some(event_tick), Some(command)) if command.tick <= event_tick => {
                self.advance_to_tick(command.tick);
            }
            (None, Some(command)) => {
                self.advance_to_tick(command.tick);
            }
            _ => {
                let term = UntilTerminator::new().until_step(1);
                self.step_swarm_until(term);
            }
        }
    }

    fn step_swarm_until(&mut self, mut terminator: impl NodesTerminator<DebugSwarmRelation>) {
        while let Some((tick, id, event)) = self.swarm.step_until(&mut terminator) {
            self.event_log.push_back((tick, id, event));
            if self.event_log.len() > self.event_cache_size {
                self.event_log.pop_front();
            }

            self.current_tick = tick;
        }
    }

    fn replay_to_tick(&mut self, tick: Duration) {
        self.rebuild_swarm();
        self.advance_to_tick(tick);
    }

    fn rebuild_swarm(&mut self) {
        self.event_log.clear();
        self.swarm = (self.config)().build();
        self.network_config = Self::default_network_config(&self.swarm);
        self.applied_network_commands.clear();
        Self::install_network_pipeline(&mut self.swarm, &self.network_config);
        self.current_tick = Duration::ZERO;
    }

    fn record_network_command(&mut self, action: NetworkCommandAction) -> Result<(), String> {
        let command = NetworkCommand {
            tick: self.current_tick,
            sequence: self.next_network_command_sequence,
            action,
        };
        self.apply_network_command(&command)?;
        self.network_command_log.push(command.clone());
        self.applied_network_commands.insert(command.sequence);
        self.next_network_command_sequence += 1;
        Ok(())
    }

    fn apply_network_command(&mut self, command: &NetworkCommand) -> Result<(), String> {
        self.network_config.apply(&command.action)?;
        Self::install_network_pipeline(&mut self.swarm, &self.network_config);
        Ok(())
    }

    fn next_unapplied_network_command_until(&self, tick: Duration) -> Option<NetworkCommand> {
        self.network_command_log
            .iter()
            .filter(|command| {
                command.tick <= tick && !self.applied_network_commands.contains(&command.sequence)
            })
            .min_by_key(|command| (command.tick, command.sequence))
            .cloned()
    }

    fn next_unapplied_network_command(&self) -> Option<NetworkCommand> {
        self.network_command_log
            .iter()
            .filter(|command| !self.applied_network_commands.contains(&command.sequence))
            .min_by_key(|command| (command.tick, command.sequence))
            .cloned()
    }

    fn default_network_config(swarm: &Nodes<DebugSwarmRelation>) -> NetworkConfig {
        let peers = swarm
            .states()
            .values()
            .map(|node| *node.id.get_peer_id())
            .collect();
        NetworkConfig::new(Duration::from_millis(DEFAULT_LINK_LATENCY_MS), peers)
    }

    fn install_network_pipeline(
        swarm: &mut Nodes<DebugSwarmRelation>,
        network_config: &NetworkConfig,
    ) {
        for node in swarm.mut_states().values_mut() {
            node.outbound_pipeline = Box::new(NetworkPipeline::new(network_config.clone()));
        }
    }
}

fn duration_from_latency_ms(latency_ms: i32) -> Result<Duration, String> {
    if latency_ms <= 0 {
        return Err("latency must be greater than zero".to_owned());
    }
    Ok(Duration::from_millis(
        latency_ms
            .try_into()
            .map_err(|_| "invalid latency".to_owned())?,
    ))
}

fn parse_node_id(node_id: &str) -> Result<NodeId<NopPubKey>, String> {
    let bytes = hex::decode(node_id).map_err(|_| "failed to parse node id hex".to_owned())?;
    Ok(NodeId::new(
        NopPubKey::from_bytes(&bytes).map_err(|_| "invalid node id".to_owned())?,
    ))
}

#[cfg(test)]
mod tests {
    use monad_crypto::certificate_signature::PubKey;

    use super::{parse_node_id, Simulation};
    use crate::default_swarm_config;

    fn node_ids(simulation: &Simulation) -> Vec<String> {
        simulation
            .network_config
            .peers()
            .iter()
            .map(|node_id| hex::encode(node_id.pubkey().bytes()))
            .collect()
    }

    #[test]
    fn invalid_node_id_returns_error() {
        let mut simulation = Simulation::new(Box::new(default_swarm_config));
        let ids = node_ids(&simulation);

        let err = simulation
            .set_link_latency("not hex", &ids[1], 10)
            .unwrap_err();
        assert!(err.contains("hex"));
    }

    #[test]
    fn rewind_replays_network_command_log() {
        let mut simulation = Simulation::new(Box::new(default_swarm_config));
        let ids = node_ids(&simulation);

        simulation.set_tick(std::time::Duration::from_millis(100));
        simulation.set_default_latency(30).unwrap();
        simulation
            .set_link_latency(&ids[0], &ids[1], 11)
            .unwrap();

        simulation.set_tick(std::time::Duration::from_millis(200));
        simulation.set_default_latency(40).unwrap();

        simulation.set_tick(std::time::Duration::from_millis(150));
        assert_eq!(
            simulation.network_config.default_latency(),
            std::time::Duration::from_millis(30)
        );
        assert_eq!(
            simulation
                .network_config
                .effective_rule(
                    *simulation.network_config.peers().iter().next().unwrap(),
                    *simulation.network_config.peers().iter().nth(1).unwrap()
                )
                .latency,
            std::time::Duration::from_millis(11)
        );

        simulation.set_tick(std::time::Duration::from_millis(250));
        assert_eq!(
            simulation.network_config.default_latency(),
            std::time::Duration::from_millis(40)
        );
    }

    #[test]
    fn invalid_position_returns_error() {
        let mut simulation = Simulation::new(Box::new(default_swarm_config));
        let ids = node_ids(&simulation);

        simulation
            .set_node_position(&ids[0], -500, 1500)
            .unwrap();

        let err = simulation.set_node_position(&ids[0], -501, 500).unwrap_err();
        assert!(err.contains("x"));
    }

    #[test]
    fn rewind_replays_node_topology_commands() {
        let mut simulation = Simulation::new(Box::new(default_swarm_config));
        let ids = node_ids(&simulation);

        simulation.set_tick(std::time::Duration::from_millis(100));
        simulation.set_node_position(&ids[0], 100, 100).unwrap();
        simulation.set_node_online(&ids[0], false).unwrap();

        simulation.set_tick(std::time::Duration::from_millis(200));
        simulation.set_node_online(&ids[0], true).unwrap();

        simulation.set_tick(std::time::Duration::from_millis(150));
        let node_id = parse_node_id(&ids[0]).unwrap();
        let node = simulation
            .network_config
            .nodes()
            .into_iter()
            .find(|node| node.id == node_id)
            .unwrap();
        assert_eq!((node.x, node.y, node.online), (100, 100, false));

        simulation.set_tick(std::time::Duration::from_millis(250));
        let node = simulation
            .network_config
            .nodes()
            .into_iter()
            .find(|node| node.id == node_id)
            .unwrap();
        assert!(node.online);
    }

    #[test]
    fn pending_message_query_survives_node_restart() {
        let mut simulation = Simulation::new(Box::new(default_swarm_config));
        let ids = node_ids(&simulation);
        let query = r#"
            query {
              nodes {
                pendingMessages {
                  message {
                    __typename
                    ... on GraphQLConsensusMessage {
                      round
                      message {
                        __typename
                        ... on GraphQLProposal {
                          seqNum
                        }
                      }
                    }
                  }
                }
              }
            }
        "#;

        simulation.set_node_online(&ids[0], false).unwrap();
        simulation.set_tick(std::time::Duration::from_millis(500));
        simulation.set_node_online(&ids[0], true).unwrap();

        for tick in (500..=2000).step_by(25) {
            simulation.set_tick(std::time::Duration::from_millis(tick));
            simulation.execute_query(query).unwrap();
        }
    }
}
