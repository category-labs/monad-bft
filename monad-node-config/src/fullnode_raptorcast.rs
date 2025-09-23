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

use monad_crypto::certificate_signature::PubKey;
use monad_types::Round;
use serde::{Deserialize, Serialize};

use super::fullnode::FullNodeConfig;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FullNodeRaptorCastConfig<P: PubKey> {
    pub enable_publisher: bool,
    pub enable_client: bool,

    #[serde(bound = "P:PubKey")]
    pub full_nodes_prioritized: FullNodeConfig<P>,
    pub raptor10_fullnode_redundancy_factor: f32, // validator -> full-nodes

    // config for Publisher
    pub round_span: Round,
    pub invite_lookahead: Round,
    pub max_invite_wait: Round,
    pub deadline_round_dist: Round,
    pub init_empty_round_span: Round,

    // config for both Publisher and Client
    pub max_group_size: usize,

    // config for Client
    pub max_num_group: usize,
    pub invite_future_dist_min: Round,
    pub invite_future_dist_max: Round,
    pub invite_accept_heartbeat_ms: u64,
}
