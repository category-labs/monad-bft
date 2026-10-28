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

use std::{collections::HashSet, iter::Cycle, ops::Range};

use monad_mcp_common::spec::Stake as _;

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

// an obligation of o chunks is met by ceil(o * den / num) of them, to
// tolerate packet loss
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) struct PacketLossResistance {
    num: usize,
    den: usize,
}

impl PacketLossResistance {
    pub(crate) const MAX_CHUNKS: usize = WireChunkId::MAX as usize + 1;

    pub(crate) const fn new(num: usize, den: usize) -> Self {
        assert!(0 < den && den <= num, "trimming never grows an obligation");
        assert!(
            Self::MAX_CHUNKS.checked_mul(num).is_some(),
            "trim and scale cannot overflow"
        );
        Self { num, den }
    }

    pub(crate) const fn trim(self, obligation: usize) -> usize {
        (obligation * self.den).div_ceil(self.num)
    }

    pub(crate) const fn scale(self, num_chunks: usize) -> usize {
        (num_chunks * self.num).div_ceil(self.den)
    }
}

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
            // receiver should receive the chunk from the author (first-hop)
            return Some(Upstream::Author);
        }
        if self.is_rebroadcast_target(receiver) {
            // receiver should receive the chunk from the owner (second-hop)
            return Some(Upstream::Owner(self.owner_index()));
        }

        // receiver should not receive the chunk via either first- or
        // second-hop.
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkTarget {
    // the first-hop recipient
    owner_node_index: NodeIndex,

    // if None, rebroadcast to all other recipients. Some for a partial
    // chunk under stake-proportional multicast.
    rebroadcast_targets: Option<Vec<NodeIndex>>,
}

#[derive(Copy, Clone)]
pub(crate) struct Obligation {
    pub(crate) whole: usize,
    // one more chunk, rebroadcast to (partial / total_stake) of the
    // holders.  ZERO for none.
    pub(crate) partial: Stake,
}

enum ChunkType {
    Whole,
    Fract(Stake),
}

impl Obligation {
    pub(crate) fn whole(whole: usize) -> Self {
        Self {
            whole,
            partial: Stake::ZERO,
        }
    }

    fn is_empty(&self) -> bool {
        self.whole == 0 && self.partial == Stake::ZERO
    }

    fn rounded_up(self) -> Self {
        if self.partial == Stake::ZERO {
            return self;
        }

        Self {
            whole: self.whole + 1,
            partial: Stake::ZERO,
        }
    }

    // the caller must ensure the obligation is not empty
    fn take_chunk(&mut self) -> ChunkType {
        debug_assert!(!self.is_empty());
        if self.whole > 0 {
            self.whole -= 1;
            return ChunkType::Whole;
        }

        let partial = std::mem::take(&mut self.partial);
        debug_assert!(partial > Stake::ZERO);
        ChunkType::Fract(partial)
    }
}

pub(crate) struct Obligations {
    total_stake: Stake,
    holders: Vec<(NodeId, Obligation)>,
}

impl Obligations {
    // the caller must ensure total > 0 when any partial is non-zero
    pub(crate) fn new(total_stake: Stake, holders: Vec<(NodeId, Obligation)>) -> Self {
        Self {
            total_stake,
            holders,
        }
    }

    fn rounded_up(mut self) -> Self {
        for (_, obligation) in &mut self.holders {
            *obligation = obligation.rounded_up();
        }
        self
    }
}

struct CoverageRing {
    ring: Cycle<Range<usize>>,
    num_nodes: usize,
    total_stake: Stake,
    // the sum of the fractional obligations so far, over total
    acc: Stake,
    // the holders handed out so far
    fulfilled: usize,
}

impl CoverageRing {
    fn new(num_nodes: usize, total_stake: Stake) -> Self {
        Self {
            ring: (0..num_nodes).cycle(),
            num_nodes,
            total_stake,
            acc: Stake::ZERO,
            fulfilled: 0,
        }
    }

    fn cover(&mut self, r: Stake) -> Vec<NodeIndex> {
        self.acc = self.acc + r;
        let (target, _) = self.acc.obligation(&self.total_stake, self.num_nodes);
        let n = target - self.fulfilled;
        self.fulfilled = target;
        self.ring.by_ref().take(n).map(NodeIndex).collect()
    }
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

    // nodes that are not assigned any chunks.
    unassigned_nodes: HashSet<NodeIndex>,

    packet_loss_resistance: PacketLossResistance,
}

impl ChunkAssignment {
    // chunk ids are dealt round-robin over the obligations, in holder
    // order, a partial chunk last.
    pub(crate) fn deal(
        author: &NodeId,
        obligations: Obligations,
        packet_loss_resistance: PacketLossResistance,
    ) -> Self {
        let num_holders = obligations.holders.len();
        let mut nodes = Vec::with_capacity(num_holders);
        let mut unassigned_nodes = HashSet::new();

        let mut remaining = Vec::new();
        for (node, obligation) in obligations.holders {
            let node_idx = NodeIndex(nodes.len());
            nodes.push(node);

            if obligation.is_empty() {
                unassigned_nodes.insert(node_idx);
            } else {
                remaining.push((node_idx, obligation));
            }
        }

        let nodes = OrderedNodes(nodes.into_boxed_slice());
        let author = nodes.index_of(author).expect("author is a holder");

        let mut ring = CoverageRing::new(num_holders, obligations.total_stake);
        let mut targets = Vec::new();

        while !remaining.is_empty() {
            for (owner, obligation) in &mut remaining {
                let rebroadcast_targets = match obligation.take_chunk() {
                    ChunkType::Whole => None,
                    ChunkType::Fract(r) => Some(ring.cover(r)),
                };

                targets.push(ChunkTarget {
                    owner_node_index: *owner,
                    rebroadcast_targets,
                });
            }
            remaining.retain(|(_, obligation)| !obligation.is_empty());
        }

        ChunkAssignment {
            nodes,
            author,
            targets,
            unassigned_nodes,
            packet_loss_resistance,
        }
    }

    pub fn num_chunks(&self) -> usize {
        self.targets.len()
    }

    pub(crate) fn packet_loss_resistance(&self) -> PacketLossResistance {
        self.packet_loss_resistance
    }

    pub(crate) fn author(&self) -> NodeIndex {
        self.author
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

    pub(crate) fn unassigned_nodes(&self) -> &HashSet<NodeIndex> {
        &self.unassigned_nodes
    }

    // the same for every chunk the owner rebroadcasts in full
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

// assign each node to chunks according to their stake.
pub struct StakePartition {
    weights: Vec<(NodeId, Stake)>,
}

impl StakePartition {
    pub fn new(weights: impl IntoIterator<Item = (NodeId, Stake)>) -> Self {
        Self {
            weights: weights.into_iter().collect(),
        }
    }

    // the author must be one of the weighted nodes.
    pub fn assign_rounded_up(
        self,
        author: &NodeId,
        num_source_chunks: usize,
        // todo: use raptorcast's fixed point redundancy type
        redundancy: f32,
        packet_loss_resistance: PacketLossResistance,
    ) -> ChunkAssignment {
        let shares = scale(num_source_chunks, redundancy);
        let obligations = self.obligations(shares).rounded_up();
        ChunkAssignment::deal(author, obligations, packet_loss_resistance)
    }

    // stake-proportional multicast: each fractional obligation is a
    // chunk rebroadcast to that fraction of the nodes.
    pub fn assign_spm(
        self,
        author: &NodeId,
        num_source_chunks: usize,
        redundancy: f32,
        packet_loss_resistance: PacketLossResistance,
    ) -> ChunkAssignment {
        let shares = scale(num_source_chunks, redundancy);
        let obligations = self.obligations(shares);
        ChunkAssignment::deal(author, obligations, packet_loss_resistance)
    }

    fn obligations(&self, shares: usize) -> Obligations {
        let total = self.weights.iter().map(|(_, stake)| *stake).sum::<Stake>();

        let mut holders = Vec::with_capacity(self.weights.len());
        for (node, stake) in &self.weights {
            let (whole, partial) = stake.obligation(&total, shares);
            holders.push((*node, Obligation { whole, partial }));
        }
        Obligations::new(total, holders)
    }
}

fn scale(num_source_chunks: usize, redundancy: f32) -> usize {
    (num_source_chunks as f32 * redundancy).ceil() as usize
}

#[cfg(test)]
mod tests {
    use super::{super::test_util::PACKET_LOSS_RESISTANCE, *};

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
        StakePartition::new(weights).assign_rounded_up(&node(0), 10, 1.0, PACKET_LOSS_RESISTANCE)
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
        let assignment = StakePartition::new(weights).assign_rounded_up(
            &node(0),
            3,
            2.5,
            PACKET_LOSS_RESISTANCE,
        );
        assert_eq!(assignment.num_chunks(), 9);
    }

    // the same weights under stake-proportional multicast
    fn spm_assignment() -> ChunkAssignment {
        let weights = [
            (node(0), Stake::ZERO),
            (node(1), Stake::from(1)),
            (node(2), Stake::from(1)),
            (node(3), Stake::from(2)),
        ];
        StakePartition::new(weights).assign_spm(&node(0), 10, 1.0, PACKET_LOSS_RESISTANCE)
    }

    #[test]
    fn a_partial_chunk_is_dealt_last_and_rebroadcast_to_its_share_of_nodes() {
        let assignment = spm_assignment();

        // the same chunks as rounding up, as 2.5 shares deal 3 chunks
        assert_eq!(assignment.num_chunks(), 11);
        assert_eq!(owned(&assignment, 1), [0, 3, 6]);
        assert_eq!(owned(&assignment, 2), [1, 4, 7]);
        assert_eq!(owned(&assignment, 3), [2, 5, 8, 9, 10]);

        // whole chunks reach everyone
        for id in [0, 1, 2, 3, 4, 5, 8, 9, 10] {
            let routing = assignment.routing(ChunkId(id));
            assert_eq!(routing.partial_rebroadcast_targets(), None);
        }

        // chunk 6 of node 1 is 2/4 of the 4 nodes: nodes 0 and 1, the
        // author and the owner
        let six = assignment.routing(ChunkId(6));
        assert_eq!(six.partial_rebroadcast_targets(), Some(HashSet::new()));
        // chunk 7 of node 2 picks up at node 2: the owner and node 3
        let seven = assignment.routing(ChunkId(7));
        assert_eq!(
            seven.partial_rebroadcast_targets(),
            Some(HashSet::from([node(3)]))
        );

        let owner = assignment.index_of(&node(2)).unwrap();
        let covered = assignment.index_of(&node(3)).unwrap();
        let uncovered = assignment.index_of(&node(1)).unwrap();
        assert_eq!(seven.upstream(owner), Some(Upstream::Author));
        assert_eq!(seven.upstream(covered), Some(Upstream::Owner(owner)));
        assert_eq!(seven.upstream(uncovered), None);
    }

    #[test]
    fn rounding_up_rebroadcasts_every_chunk_in_full() {
        let rounded_up = assignment();
        for id in 0..11 {
            let routing = rounded_up.routing(ChunkId(id));
            assert_eq!(routing.partial_rebroadcast_targets(), None);
        }
    }

    #[test]
    fn every_holder_is_handed_out_once_per_pass() {
        // 4 holders, total 10: partials 3, 3, 3, 3, 8 add up to 2 passes
        let mut ring = CoverageRing::new(4, Stake::from(10));
        // 1.2 nodes due so far
        assert_eq!(ring.cover(Stake::from(3)), [NodeIndex(0)]);
        // 2.4
        assert_eq!(ring.cover(Stake::from(3)), [NodeIndex(1)]);
        // 3.6
        assert_eq!(ring.cover(Stake::from(3)), [NodeIndex(2)]);
        // 4.8
        assert_eq!(ring.cover(Stake::from(3)), [NodeIndex(3)]);
        // 8.0
        let last = ring.cover(Stake::from(8));
        assert_eq!(
            last,
            [NodeIndex(0), NodeIndex(1), NodeIndex(2), NodeIndex(3)]
        );
    }

    #[test]
    fn a_dropped_fraction_carries_to_the_next_partial_chunk() {
        // 3 holders, total 4: 1/4 of 3 nodes is 0.75, so the first
        // partial chunk gets nobody and the second gets 1.5 rounded down
        let mut ring = CoverageRing::new(3, Stake::from(4));
        assert_eq!(ring.cover(Stake::from(1)), []);
        assert_eq!(ring.cover(Stake::from(1)), [NodeIndex(0)]);
        assert_eq!(ring.cover(Stake::from(2)), [NodeIndex(1), NodeIndex(2)]);
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
