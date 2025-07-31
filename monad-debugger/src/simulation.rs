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

use std::{collections::VecDeque, time::Duration};

use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema, ServerError};
use monad_crypto::NopPubKey;
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock_swarm::{Nodes, SwarmBuilder},
    swarm_relation::{DebugSwarmRelation, SwarmRelation},
    terminator::{NodesTerminator, UntilTerminator},
};
use monad_transformer::ID;

use crate::graphql::{GraphQLRoot, GraphQLSimulation};

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
    config: Box<dyn Fn() -> SwarmBuilder<DebugSwarmRelation>>,
    schema: Schema<GraphQLRoot, EmptyMutation, EmptySubscription>,
}

impl Simulation {
    pub fn new(config: Box<dyn Fn() -> SwarmBuilder<DebugSwarmRelation>>) -> Self {
        Self {
            current_tick: Duration::ZERO,
            swarm: config().build(),
            event_log: Default::default(),
            event_cache_size: 100,
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
            self.reset();
        }
        assert!(tick >= self.current_tick);
        let term = UntilTerminator::new().until_tick(tick);
        self.step_until(term);
        self.current_tick = tick;
    }

    pub fn step(&mut self) {
        let term = UntilTerminator::new().until_step(1);
        self.step_until(term);
    }

    pub fn step_until(&mut self, mut terminator: impl NodesTerminator<DebugSwarmRelation>) {
        while let Some((tick, id, event)) = self.swarm.step_until(&mut terminator) {
            self.event_log.push_back((tick, id, event));
            if self.event_log.len() > self.event_cache_size {
                self.event_log.pop_front();
            }

            self.current_tick = tick;
        }
    }

    fn reset(&mut self) {
        self.event_log.clear();
        self.swarm = (self.config)().build();
        self.current_tick = Duration::ZERO;
    }
}
