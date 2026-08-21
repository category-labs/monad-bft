mod d25;

pub(crate) use d25::D25StakePartition;

use std::sync::Arc;

use monad_mcp_chorus::spec::Stake as _;

use super::{
    codec::Context,
    layout::PacketLayout,
    types::{NodeId, Stake},
};

#[derive(
    Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Debug, Hash, derive_more::From, derive_more::Into,
)]
pub struct ChunkId(u16);

impl From<ChunkId> for usize {
    fn from(chunk_id: ChunkId) -> Self {
        usize::from(u16::from(chunk_id))
    }
}

// Resolved routing for a single chunk, used to get the first-hop
// recipient (owner) and the rebroadcast targets.
pub struct ChunkRouting<'a> {
    owner: &'a NodeId,
    target: &'a ChunkTarget,
    nodes: &'a OrderedNodes,
}

impl<'a> ChunkRouting<'a> {
    // return 'a reference to allow NodeId be used after ChunkRouting
    // is dropped
    pub fn owner(&self) -> &'a NodeId {
        self.owner
    }

    pub fn rebroadcast_targets(&self) -> Vec<NodeId> {
        let owner_idx = self.target.owner_node_index;
        match &self.target.rebroadcast_targets {
            None => self
                .nodes
                .iter()
                .filter(|(idx, _)| *idx != owner_idx)
                .map(|(_, node_id)| *node_id)
                .collect(),
            Some(indices) => indices
                .iter()
                .filter(|idx| **idx != owner_idx)
                .filter_map(|idx| self.nodes.get(*idx).copied())
                .collect(),
        }
    }

    // should this chunk route to all other nodes?
    pub fn is_full_rebroadcast(&self) -> bool {
        self.target.rebroadcast_targets.is_none()
    }

    pub fn upstream(&self, receiver: &NodeId) -> Option<Upstream<&'a NodeId>> {
        if self.owner() == receiver {
            return Some(Upstream::Author);
        }

        if self.is_full_rebroadcast() || self.rebroadcast_targets().contains(receiver) {
            Some(Upstream::Owner(self.owner()))
        } else {
            // unrouted
            None
        }
    }
}

// index of a node in an OrderedNodes instance. Treat as opaque
// handle, only meaningful when used with the same OrderedNodes
// instance that produced it.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct NodeIndex(usize);

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
    fn get(&self, index: NodeIndex) -> Option<&NodeId> {
        self.0.get(index.0)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = (NodeIndex, &NodeId)> + '_ {
        self.0.iter().enumerate().map(|(i, n)| (NodeIndex(i), n))
    }
}

impl FromIterator<NodeId> for OrderedNodes {
    fn from_iter<I: IntoIterator<Item = NodeId>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

// A frozen assignment of chunks to nodes. Can be cloned cheaply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAssignment {
    // mapping from NodeIndex to NodeId. The ordering is frozen on
    // assignment.
    //
    // Invariant: every target.node_index is < nodes.len().
    nodes: Arc<OrderedNodes>,

    // mapping from chunk_id to the target node.
    targets: Arc<Vec<ChunkTarget>>,

    // the number of source chunks. used as decoding threshold.
    num_source_chunks: usize,
}

impl ChunkAssignment {
    pub fn num_chunks(&self) -> usize {
        self.targets.len()
    }

    pub fn num_source_chunks(&self) -> usize {
        self.num_source_chunks
    }

    // Note: this method is O(N)
    fn node_index(&self, node: &NodeId) -> Option<NodeIndex> {
        self.nodes
            .iter()
            .find_map(|(index, n)| (*n == *node).then_some(index))
    }

    // Resolve the target information for a given chunk_id. Returns
    // None if chunk_id is out of range.
    pub fn resolve_chunk_id(&self, chunk_id: ChunkId) -> Option<ChunkRouting<'_>> {
        let target = self.targets.get(usize::from(chunk_id))?;
        let owner = self.nodes.get(target.owner_node_index)?;
        Some(ChunkRouting {
            owner,
            target,
            nodes: &self.nodes,
        })
    }
}

// upstream(c, R) = author    if R == owner(c)
//                  owner(c)  if R is a rebroadcast target of c
//                  bottom    if c is unrouted to R
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Upstream<N = NodeId> {
    // the chunk is routed to the receiver in first-hop
    Author,
    // the chunk is routed to the receiver through rebroadcast via
    // this node.
    Owner(N),
}

// assign each node to chunks according to their stake, rounded up.
pub struct StakePartition {
    weights: Vec<(NodeId, Stake)>,
}

impl StakePartition {
    pub fn new(weights: impl IntoIterator<Item = (NodeId, Stake)>) -> Self {
        Self {
            weights: weights.into_iter().collect(),
        }
    }

    // todo: use raptorcast's fixed point redundancy type
    pub fn assign(&self, num_source_chunks: usize, redundancy: f32) -> ChunkAssignment {
        let nodes: OrderedNodes = self.weights.iter().map(|(node, _)| *node).collect();
        let mut targets = Vec::new();

        let scaled_num_source_chunks = (num_source_chunks as f32 * redundancy).ceil() as usize;
        let total = self.weights.iter().map(|(_, stake)| *stake).sum::<Stake>();
        let mut remaining = self
            .weights
            .iter()
            .enumerate()
            .map(|(index, (_, stake))| {
                let (whole, remainder) = stake.obligation(&total, scaled_num_source_chunks);
                // a fractional remainder claims one rounding chunk
                let rounding_chunk = (remainder > 0) as usize;
                (NodeIndex(index), whole + rounding_chunk)
            })
            .collect::<Vec<_>>();

        while !remaining.is_empty() {
            remaining.retain_mut(|(node_index, obligation)| {
                if *obligation == 0 {
                    return false;
                }

                targets.push(ChunkTarget {
                    owner_node_index: *node_index,
                    // every chunk (including rounding) is currently
                    // rebroadcast to the whole group. todo: stake
                    // proportional rebroadcast for rounding chunks.
                    rebroadcast_targets: None,
                });

                *obligation -= 1;
                *obligation > 0
            });
        }

        let nodes = Arc::new(nodes);
        let targets = Arc::new(targets);
        ChunkAssignment {
            nodes,
            targets,
            num_source_chunks,
        }
    }
}

pub(crate) trait Assigner {
    // q: should this be made fallible?
    fn from_layout(
        layout: &PacketLayout,
        author: &NodeId,
        context: &Context,
        // may need other environments, e.g. slot
    ) -> ChunkAssignment;
}
