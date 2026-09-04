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

use std::collections::HashSet;

use monad_mcp_chorus::spec::Stake as _;

use super::{
    chunk::WireChunkId,
    types::{NodeId, Stake},
};

// a verified chunk id associated to a chunk assignment. always in
// range.
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Debug, Hash, derive_more::Into)]
#[into(usize)]
pub struct ChunkId(u16);

impl ChunkId {
    pub fn to_wire(self) -> WireChunkId {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn unchecked(wire: WireChunkId) -> Self {
        Self(wire)
    }
}

// a verified index of a node in a chunk assignment. only meaningful with the
// assignment that produced it.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, derive_more::Into)]
pub(crate) struct NodeIndex(usize);

// Resolved routing for a single chunk: the dissemination path author
// -> owner -> rebroadcast targets. The author is never a target.
pub struct ChunkRouting<'a> {
    chunk_id: ChunkId,
    target: &'a ChunkTarget,
    assignment: &'a ChunkAssignment,
}

impl<'a> ChunkRouting<'a> {
    pub fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    pub(crate) fn owner_index(&self) -> NodeIndex {
        self.target.owner_node_index
    }

    // rounding chunks in stake-proportional multicast
    pub fn partial_rebroadcast_targets(&self) -> Option<HashSet<NodeId>> {
        let indices = self.target.rebroadcast_targets.as_ref()?;
        let nodes = indices
            .iter()
            .filter(|idx| self.is_rebroadcast_target(**idx))
            .map(|idx| *self.assignment.nodes.get(*idx))
            .collect();
        Some(nodes)
    }

    // whether the node receives this chunk via rebroadcast
    fn is_rebroadcast_target(&self, node: NodeIndex) -> bool {
        if node == self.owner_index() || node == self.assignment.author {
            return false;
        }
        let Some(indices) = &self.target.rebroadcast_targets else {
            // rebroadcast to all other nodes
            return true;
        };
        indices.contains(&node)
    }

    // from whom should the receiver get this chunk? none if unrouted
    pub(crate) fn upstream(&self, receiver: NodeIndex) -> Option<Upstream> {
        if receiver == self.assignment.author {
            // the source holds every chunk
            return None;
        }
        if self.owner_index() == receiver {
            return Some(Upstream::Author);
        }
        if self.is_rebroadcast_target(receiver) {
            return Some(Upstream::Owner(self.owner_index()));
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkTarget {
    // the first-hop recipient
    owner_node_index: NodeIndex,

    // if None, rebroadcast to all other recipients. reserved for
    // stake-proportional multicast rounding chunks.
    rebroadcast_targets: Option<Vec<NodeIndex>>,
}

// A frozen ordered list of nodes, indexable by NodeIndex. Captures
// the concept of "the owner table for a ChunkAssignment".
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedNodes(Box<[NodeId]>);

impl OrderedNodes {
    fn get(&self, index: NodeIndex) -> &NodeId {
        &self.0[index.0]
    }

    // Note: O(N)
    fn index_of(&self, node: &NodeId) -> Option<NodeIndex> {
        let index = self.0.iter().position(|n| n == node)?;
        Some(NodeIndex(index))
    }

    fn iter(&self) -> impl Iterator<Item = (NodeIndex, &NodeId)> + '_ {
        self.0.iter().enumerate().map(|(i, n)| (NodeIndex(i), n))
    }
}

// A frozen assignment of chunks to nodes, identical on every
// validator.
//
// NodeIndex and ChunkId are considered validated against the
// assignment.
#[derive(Debug, PartialEq, Eq)]
pub struct ChunkAssignment {
    // mapping from NodeIndex to NodeId. The ordering is frozen on
    // assignment.
    //
    // Invariant: every target's node indices are < nodes.len().
    nodes: OrderedNodes,

    // the source of every chunk
    author: NodeIndex,

    // mapping from chunk_id to the target node.
    targets: Vec<ChunkTarget>,
}

impl ChunkAssignment {
    pub fn num_chunks(&self) -> usize {
        self.targets.len()
    }

    pub(crate) fn num_nodes(&self) -> usize {
        self.nodes.0.len()
    }

    pub(crate) fn node(&self, index: NodeIndex) -> &NodeId {
        self.nodes.get(index)
    }

    // Note: O(N). None if node is not in the assignment.
    pub(crate) fn index_of(&self, node: &NodeId) -> Option<NodeIndex> {
        self.nodes.index_of(node)
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = (NodeIndex, &NodeId)> {
        self.nodes.iter()
    }

    // Resolve the target information for a given chunk_id. Returns
    // None if chunk_id is out of range.
    pub fn resolve_chunk_id(&self, chunk_id: WireChunkId) -> Option<ChunkRouting<'_>> {
        if usize::from(chunk_id) >= self.num_chunks() {
            return None;
        }
        Some(self.routing(ChunkId(chunk_id)))
    }

    pub fn routing(&self, chunk_id: ChunkId) -> ChunkRouting<'_> {
        ChunkRouting {
            chunk_id,
            target: &self.targets[usize::from(chunk_id)],
            assignment: self,
        }
    }

    fn routings(&self) -> impl Iterator<Item = ChunkRouting<'_>> {
        self.chunk_ids().map(|chunk_id| self.routing(chunk_id))
    }

    pub(crate) fn chunk_ids(&self) -> impl Iterator<Item = ChunkId> {
        (0..self.num_chunks() as u16).map(ChunkId)
    }

    pub(crate) fn owned_chunks(&self, node: NodeIndex) -> impl Iterator<Item = ChunkRouting<'_>> {
        self.routings()
            .filter(move |routing| routing.owner_index() == node)
    }

    // the upstream of every chunk routed to the receiver
    pub(crate) fn upstreams(&self, receiver: NodeIndex) -> impl Iterator<Item = Upstream> + '_ {
        self.routings()
            .filter_map(move |routing| routing.upstream(receiver))
    }

    pub(crate) fn full_rebroadcast_targets(&self, owner: NodeIndex) -> HashSet<NodeId> {
        let mut targets = HashSet::with_capacity(self.num_nodes());
        for (index, node) in self.nodes() {
            if index == owner || index == self.author {
                continue;
            }
            targets.insert(*node);
        }
        targets
    }
}

// upstream(c, R) = bottom    if R == author
//                  author    if R == owner(c)
//                  owner(c)  if R is a rebroadcast target of c
//                  bottom    if c is unrouted to R
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Upstream {
    // the chunk is routed to the receiver in first-hop
    Author,
    // the chunk is routed to the receiver through rebroadcast via
    // this node.
    Owner(NodeIndex),
}

// assign each node to chunks according to their stake, rounded up.
pub struct StakePartition {
    nodes: OrderedNodes,
    // the stake of each node, by NodeIndex
    weights: Vec<Stake>,
}

impl StakePartition {
    pub fn new(weights: impl IntoIterator<Item = (NodeId, Stake)>) -> Self {
        let mut nodes = Vec::new();
        let mut stakes = Vec::new();
        for (node, stake) in weights {
            nodes.push(node);
            stakes.push(stake);
        }
        Self {
            nodes: OrderedNodes(nodes.into_boxed_slice()),
            weights: stakes,
        }
    }

    // the author must be one of the weighted nodes.
    pub fn assign(
        self,
        author: &NodeId,
        num_source_chunks: usize,
        // todo: use raptorcast's fixed point redundancy type
        redundancy: f32,
    ) -> ChunkAssignment {
        let scaled_num_source_chunks = (num_source_chunks as f32 * redundancy).ceil() as usize;
        let mut remaining = self.obligations(scaled_num_source_chunks);

        // chunk ids are dealt round-robin across nodes
        let mut targets = Vec::new();
        while !remaining.is_empty() {
            for (node_index, obligation) in &mut remaining {
                targets.push(ChunkTarget {
                    owner_node_index: *node_index,
                    // every chunk (including rounding) is currently
                    // rebroadcast to the whole group. todo: stake
                    // proportional rebroadcast for rounding chunks.
                    rebroadcast_targets: None,
                });
                *obligation -= 1;
            }
            remaining.retain(|(_, obligation)| *obligation > 0);
        }

        let author = self
            .nodes
            .index_of(author)
            .expect("author is a weighted node");
        ChunkAssignment {
            nodes: self.nodes,
            author,
            targets,
        }
    }

    fn obligations(&self, shares: usize) -> Vec<(NodeIndex, usize)> {
        let total = self.weights.iter().copied().sum::<Stake>();

        let mut obligations = Vec::with_capacity(self.weights.len());
        for (index, stake) in self.weights.iter().enumerate() {
            let (whole, remainder) = stake.obligation(&total, shares);
            let rounding_chunk = if remainder > 0 { 1 } else { 0 };
            let obligation = whole + rounding_chunk;
            if obligation > 0 {
                obligations.push((NodeIndex(index), obligation));
            }
        }
        obligations
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::Stake as _;

    use super::*;

    fn node(id: u64) -> NodeId {
        NodeId::dummy(id)
    }

    // author 0 with no stake, then stakes 1, 1 and 2, over 10 chunks
    fn assignment() -> ChunkAssignment {
        let weights = [
            (node(0), Stake::ZERO),
            (node(1), Stake::from(1)),
            (node(2), Stake::from(1)),
            (node(3), Stake::from(2)),
        ];
        StakePartition::new(weights).assign(&node(0), 10, 1.0)
    }

    fn owned(assignment: &ChunkAssignment, id: u64) -> Vec<WireChunkId> {
        let index = assignment.index_of(&node(id)).expect("in the assignment");
        let mut owned = Vec::new();
        for routing in assignment.owned_chunks(index) {
            owned.push(routing.chunk_id().to_wire());
        }
        owned
    }

    #[test]
    fn chunks_follow_stake_with_a_rounding_chunk_each() {
        let assignment = assignment();

        // 10 * 1/4 = 2 rem 2 -> 3; 10 * 2/4 = 5; the author owns nothing.
        // ids are dealt round-robin among the owners
        assert_eq!(assignment.num_chunks(), 11);
        assert!(owned(&assignment, 0).is_empty());
        assert_eq!(owned(&assignment, 1), [0, 3, 6]);
        assert_eq!(owned(&assignment, 2), [1, 4, 7]);
        assert_eq!(owned(&assignment, 3), [2, 5, 8, 9, 10]);
    }

    #[test]
    fn redundancy_scales_the_source_count_rounding_up() {
        // ceil(3 * 2.5) = 8 shares over 3 equal nodes: 2 rem 2 -> 3 each
        let weights = (0..3).map(|id| (node(id), Stake::from(1)));
        let assignment = StakePartition::new(weights).assign(&node(0), 3, 2.5);
        assert_eq!(assignment.num_chunks(), 9);
    }

    #[test]
    fn upstream_follows_the_dissemination_tree() {
        let assignment = assignment();
        let author = assignment.index_of(&node(0)).unwrap();
        let owner = assignment.index_of(&node(1)).unwrap();
        let other = assignment.index_of(&node(2)).unwrap();

        // chunk 0 is owned by node 1
        let routing = assignment.routing(ChunkId(0));
        assert_eq!(routing.owner_index(), owner);
        assert_eq!(routing.upstream(author), None);
        assert_eq!(routing.upstream(owner), Some(Upstream::Author));
        assert_eq!(routing.upstream(other), Some(Upstream::Owner(owner)));

        // the full rebroadcast reaches everyone but the owner and the author
        assert_eq!(routing.partial_rebroadcast_targets(), None);
        assert_eq!(
            assignment.full_rebroadcast_targets(owner),
            HashSet::from([node(2), node(3)])
        );
    }

    #[test]
    fn ids_and_nodes_are_checked_against_the_assignment() {
        let assignment = assignment();
        assert!(assignment.resolve_chunk_id(10).is_some());
        assert!(assignment.resolve_chunk_id(11).is_none());
        assert!(assignment.index_of(&node(9)).is_none());
    }

    #[test]
    fn the_same_weights_give_the_same_assignment() {
        assert_eq!(assignment(), assignment());
    }
}
