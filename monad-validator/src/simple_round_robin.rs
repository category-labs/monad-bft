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

use std::{collections::BTreeMap, marker::PhantomData};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

use crate::leader_election::LeaderElection;

#[derive(Clone)]
pub struct SimpleRoundRobin<PT>(PhantomData<PT>);
impl<PT> Default for SimpleRoundRobin<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> LeaderElection for SimpleRoundRobin<PT> {
    type NodeIdPubKey = PT;
    fn get_leader(
        &self,
        round: Round,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        let validators: Vec<_> = validators
            .iter()
            .filter_map(|(node_id, stake)| (*stake != Stake(0)).then_some(node_id))
            .collect();
        *validators[round.0 as usize % validators.len()]
    }
}
