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

use std::{collections::BTreeMap, time::Duration};

use monad_peer_discovery::PeerDiscoveryAlgoBuilder;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{NodeBuilder, Nodes, PeerDiscSwarmRelation, RoutingTable};

pub struct PeerDiscSwarmBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
{
    pub builders: Vec<NodeBuilder<S, B>>,
    pub seed: u64,
}

impl<S, B> PeerDiscSwarmBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
    B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
{
    pub fn build(self) -> Nodes<S> {
        let _init_span_entered =
            tracing::trace_span!("init", tick = format!("{:?}", Duration::ZERO)).entered();

        let mut states = BTreeMap::new();
        let mut routing_table = RoutingTable::new();
        let addrs: Vec<_> = self.builders.iter().map(|b| (b.id, b.addr)).collect();
        for builder in self.builders {
            let id = builder.id;
            let _node_span_entered = tracing::trace_span!("node", id = format!("{}", id)).entered();
            routing_table.register(builder.addr, id);
            let mut node = builder.build();
            for &(other_id, other_addr) in &addrs {
                if other_id != id {
                    node.executor.id_to_addr.insert(other_id, other_addr);
                }
            }
            states.insert(id, node);
        }

        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);

        Nodes {
            states,
            routing_table,
            tick: Default::default(),
            rng: ChaCha8Rng::seed_from_u64(rng.r#gen()),
            current_seed: rng.r#gen(),
        }
    }
}
