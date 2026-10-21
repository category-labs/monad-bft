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

use std::{collections::HashMap, ops::Range, time::Duration};

use monad_mcp_chorus::stub::types::NodeId;
use monad_mcp_chorus_sim::SimMessage;
use monad_sim::Time;
use monad_sim_swarm::{Link, NetworkModel};
use rand::Rng;
use rand_chacha::ChaChaRng;

use super::{DropFn, Wire};

/// Fixed per-link latency, loss, partitions and a drop predicate. Self
/// delivery is immediate and lossless, as the node's local loopback is.
pub struct Model {
    pub latency: u64,
    pub link_latency: HashMap<(u64, u64), u64>,
    pub loss: f64,
    /// (window in ms, the nodes cut off from the rest)
    pub partitions: Vec<(Range<u64>, Vec<u64>)>,
    pub drop_if: Option<DropFn>,
}

fn at_ms(ms: u64) -> Time {
    Time(i128::from(ms) * 1_000_000)
}

impl NetworkModel<NodeId, SimMessage<Wire>> for Model {
    fn deliveries(
        &mut self,
        link: &Link<NodeId>,
        message: &SimMessage<Wire>,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        let (from, to) = (u64::from(link.from), u64::from(link.to));
        if from == to {
            return vec![Duration::ZERO];
        }
        if self
            .drop_if
            .as_ref()
            .is_some_and(|drop| drop(link, message))
        {
            return Vec::new();
        }
        let cut = self.partitions.iter().any(|(window, nodes)| {
            (at_ms(window.start)..at_ms(window.end)).contains(&link.now)
                && nodes.contains(&from) != nodes.contains(&to)
        });
        if cut || (self.loss > 0.0 && rng.gen_bool(self.loss)) {
            return Vec::new();
        }
        let ms = self
            .link_latency
            .get(&(from, to))
            .copied()
            .unwrap_or(self.latency);
        vec![Duration::from_millis(ms)]
    }
}
