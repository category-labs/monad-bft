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

use std::{collections::VecDeque, ops::Range};

use alloy_primitives::U256;
use bytes::BytesMut;
use monad_crypto::certificate_signature::PubKey;
use monad_raptor::r10::lt::MAX_TRIPLES;
use monad_types::{NodeId, Stake};
use rand::{seq::SliceRandom as _, SeedableRng as _};
use rand_chacha::ChaCha20Rng;

use super::{BuildError, Chunk, Result};
use crate::util::{ensure, PrimaryBroadcastGroup, Recipient, Redundancy, SecondaryBroadcastGroup};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct NodeIndex(usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkTarget {
    // the first-hop recipient
    node_index: NodeIndex,

    // if None, rebroadcast to all other recipients. reserved for
    // stake-proportional multicast rounding chunks.
    rebroadcast_targets: Option<Vec<NodeIndex>>,
}

impl From<NodeIndex> for ChunkTarget {
    fn from(value: NodeIndex) -> Self {
        Self {
            node_index: value,
            rebroadcast_targets: None,
        }
    }
}

pub(crate) trait OrderedNodes<PT>
where
    PT: PubKey,
{
    fn get(&self, index: NodeIndex) -> Option<&NodeId<PT>>;
    // [u8; 32] == <ChaCha20Rng as SeedableRng>::Seed
    fn shuffle(&mut self, seed: [u8; 32]);
    fn len(&self) -> usize;
}

impl<PT> OrderedNodes<PT> for NodeId<PT>
where
    PT: PubKey,
{
    fn get(&self, index: NodeIndex) -> Option<&NodeId<PT>> {
        match index.0 {
            0 => Some(self),
            _ => None,
        }
    }

    fn shuffle(&mut self, _seed: [u8; 32]) {
        // no-op since there's only one node
    }

    fn len(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChunkAssignment {
    // Invariant: targets[*].node_index < node_len
    //
    // Intuitively, treat this field as playing the role of owning a
    // &dyn OrderedNodes without the lifetime hassles. Such that the
    // chunk assignment is bound to a *single* *immutable*
    // OrderedNodes instance. This guarantees that the node indices in
    // the assignment are always valid and consistent with the nodes
    // used for materialization.
    node_len: usize,

    // mapping from chunk_id to the target node
    targets: Vec<ChunkTarget>,
}

impl ChunkAssignment {
    fn with_capacity(node_len: usize, capacity: usize) -> Self {
        Self {
            node_len,
            targets: Vec::with_capacity(capacity),
        }
    }

    pub fn unicast(num_chunks: usize) -> Self {
        let mut assignment = Self::with_capacity(1, num_chunks);
        let target = NodeIndex(0);
        assignment.push_range(target, 0..num_chunks);
        assignment
    }

    pub fn num_chunks(&self) -> usize {
        self.targets.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    fn push_range(&mut self, target: impl Into<ChunkTarget>, chunk_id_range: Range<usize>) {
        debug_assert_eq!(chunk_id_range.start, self.targets.len());

        let target = target.into();
        debug_assert!(target.node_index.0 < self.node_len);

        for _ in chunk_id_range {
            self.targets.push(target.clone());
        }
    }

    fn push(&mut self, target: impl Into<ChunkTarget>, chunk_id: usize) {
        debug_assert_eq!(chunk_id, self.targets.len());

        let target = target.into();
        debug_assert!(target.node_index.0 < self.node_len);

        self.targets.push(target);
    }

    fn iter(&self) -> impl Iterator<Item = (usize, &ChunkTarget)> {
        self.targets.iter().enumerate()
    }

    // Resolve the target information for a given chunk_id. Returns
    // None if chunk_id is out of range.
    //
    // The provided nodes must be the same OrderedNodes instance that
    // produced this assignment.
    pub fn resolve_chunk_id<'a, PT, N>(
        &'a self,
        chunk_id: usize,
        nodes: &'a N,
    ) -> Option<ChunkRouting<'a, PT, N>>
    where
        PT: PubKey,
        N: OrderedNodes<PT>,
    {
        let target = self.targets.get(chunk_id)?;
        let recipient = nodes.get(target.node_index)?;
        Some(ChunkRouting {
            recipient,
            target,
            nodes,
        })
    }

    // Caller must guarantee that the provided nodes are the same as
    // the ones used for assignment.
    pub(crate) fn materialize<PT>(
        &self,
        segment_len: usize,
        nodes: &impl OrderedNodes<PT>,
    ) -> Result<Vec<Chunk<PT>>>
    where
        PT: PubKey,
    {
        assert_eq!(self.node_len, nodes.len());

        if self.targets.is_empty() {
            return Ok(vec![]);
        }

        ensure!(self.num_chunks() <= MAX_TRIPLES, BuildError::TooManyChunks);

        let mut chunks = Vec::with_capacity(self.num_chunks());
        let mut buffer = BytesMut::zeroed(self.num_chunks() * segment_len);
        let mut recipients = vec![None; nodes.len()];

        for (chunk_id, target) in self.iter() {
            let payload = buffer.split_to(segment_len);
            let Some(node_id) = nodes.get(target.node_index) else {
                tracing::debug!("BUG: invalid target node index: {}", target.node_index.0);
                continue;
            };

            // guaranteed by the invariant on self.node_len
            debug_assert!(target.node_index.0 < recipients.len());
            let recipient = recipients[target.node_index.0]
                .get_or_insert_with(|| Recipient::new(*node_id))
                .clone();
            let chunk = Chunk::new(chunk_id, recipient, payload);
            chunks.push(chunk);
        }

        debug_assert_eq!(chunks.len(), self.num_chunks());
        debug_assert_eq!(buffer.len(), 0);

        Ok(chunks)
    }
}

// Resolved routing for a single chunk, used to get the first-hop
// recipient and the rebroadcast targets.
pub struct ChunkRouting<'a, PT: PubKey, N> {
    recipient: &'a NodeId<PT>,
    target: &'a ChunkTarget,
    nodes: &'a N,
}

impl<'a, PT: PubKey, N: OrderedNodes<PT>> ChunkRouting<'a, PT, N> {
    pub fn recipient(&self) -> &NodeId<PT> {
        self.recipient
    }

    pub fn rebroadcast_targets(&self) -> Vec<NodeId<PT>> {
        let recipient_idx = self.target.node_index;
        match &self.target.rebroadcast_targets {
            None => (0..self.nodes.len())
                .map(NodeIndex)
                .filter(|idx| *idx != recipient_idx)
                .filter_map(|idx| self.nodes.get(idx).copied())
                .collect(),
            Some(indices) => indices
                .iter()
                .filter(|idx| **idx != recipient_idx)
                .filter_map(|idx| self.nodes.get(*idx).copied())
                .collect(),
        }
    }
}

pub(crate) struct EvenPartition<PT: PubKey> {
    nodes: Vec<NodeId<PT>>,
}

impl<PT: PubKey> EvenPartition<PT> {
    #[cfg(test)]
    pub fn new(nodes: Vec<NodeId<PT>>) -> Self {
        Self { nodes }
    }

    pub fn from_group(group: &SecondaryBroadcastGroup<'_, PT>) -> Self {
        Self {
            nodes: group.iter().cloned().collect(),
        }
    }
}

impl<PT> OrderedNodes<PT> for EvenPartition<PT>
where
    PT: PubKey,
{
    fn get(&self, index: NodeIndex) -> Option<&NodeId<PT>> {
        self.nodes.get(index.0)
    }

    fn shuffle(&mut self, seed: [u8; 32]) {
        let mut rng = ChaCha20Rng::from_seed(seed);
        self.nodes.shuffle(&mut rng);
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl<PT: PubKey> EvenPartition<PT> {
    pub fn assign(
        &self,
        num_base_symbols: usize,
        redundancy: Redundancy,
    ) -> Result<ChunkAssignment> {
        let num_symbols = redundancy
            .scale(num_base_symbols)
            .ok_or(BuildError::TooManyChunks)?;
        let num_nodes = self.nodes.len();
        let mut assignment = ChunkAssignment::with_capacity(num_nodes, num_symbols);
        if num_nodes == 0 {
            tracing::warn!("no nodes specified for even partition chunk assigner");
            return Ok(assignment);
        }

        for chunk_id in 0..num_symbols {
            let target = NodeIndex(chunk_id % num_nodes);
            assignment.push(target, chunk_id);
        }

        Ok(assignment)
    }
}

// Proportional to stake, plus each validator gets an optional
// rounding chunk
pub(crate) struct StakePartition<PT: PubKey> {
    // Validator set with the publisher node excluded.
    // Invariant: all stake must be non-zero
    validators: Vec<(NodeId<PT>, Stake)>,

    // Invariant: total_stake == sum of validators' stakes
    // Invariant: validators.is_empty() iff total_stake == Stake::ZERO
    // (i.e. singleton validator set)
    total_stake: Stake,
}

impl<PT: PubKey> OrderedNodes<PT> for StakePartition<PT> {
    fn get(&self, index: NodeIndex) -> Option<&NodeId<PT>> {
        self.validators.get(index.0).map(|(node_id, _)| node_id)
    }

    // Shuffle the validator stake map for chunk assignment. This uses
    // a deterministic seed. It is required that the publisher and all
    // validators compute the shuffling using the same seed and
    // algorithm for deterministic raptorcast.
    fn shuffle(&mut self, seed: [u8; 32]) {
        let mut rng = ChaCha20Rng::from_seed(seed);
        self.validators.shuffle(&mut rng);
    }

    fn len(&self) -> usize {
        self.validators.len()
    }
}

#[inline(always)]
pub fn stake_partition_num_chunks_hint(
    num_base_symbols: usize,
    redundancy: Redundancy,
    group_size: usize,
) -> Option<usize> {
    let num_validators = group_size.checked_sub(1)?; // exclude author
    let num_scaled_symbols = redundancy.scale(num_base_symbols)?;
    num_scaled_symbols.checked_add(num_validators)
}

impl<PT: PubKey> StakePartition<PT> {
    pub fn from_group(group: &PrimaryBroadcastGroup<'_, PT>) -> Self {
        let mut total_stake = Stake::ZERO;
        let mut validators = Vec::with_capacity(group.len().into());

        for (node, stake) in group.iter() {
            if node == group.author() {
                // skip author
                continue;
            }
            // stake is guaranteed to be non-zero from PrimaryBroadcastGroup's invariant.
            debug_assert!(!stake.0.is_zero());
            validators.push((*node, *stake));
            total_stake += *stake;
        }

        Self {
            validators,
            total_stake,
        }
    }

    #[cfg(test)]
    // accepts u64/U256 as stake
    fn from_stakes<T>(validators: Vec<(NodeId<PT>, T)>) -> Self
    where
        // Use TryInto instead of Into as U256 does not implement
        // From<u64>.
        T: TryInto<U256>,
    {
        let validators: Vec<_> = validators
            .into_iter()
            .map(|(n, s)| {
                let s = s.try_into().ok().unwrap();
                assert!(!s.is_zero());
                (n, Stake(s))
            })
            .collect();
        let total_stake = validators.iter().map(|(_, s)| *s).sum::<Stake>();
        Self {
            validators,
            total_stake,
        }
    }

    pub fn assign(
        &self,
        num_base_symbols: usize,
        redundancy: Redundancy,
    ) -> Result<ChunkAssignment> {
        if self.validators.is_empty() {
            return Ok(ChunkAssignment::default());
        }
        self.assign_round_robin(num_base_symbols, redundancy)
            .ok_or(BuildError::TooManyChunks)
    }

    pub fn num_chunks_hint(
        &self,
        num_base_symbols: usize,
        redundancy: Redundancy,
    ) -> Option<usize> {
        let group_size = self.validators.len() + 1; // add back the author
        stake_partition_num_chunks_hint(num_base_symbols, redundancy, group_size)
    }

    // Compute O = num_scaled_symbols * stake / total_stake, split the
    // result into the number of whole chunks (floor(O)) and the
    // remainder (num_scaled_symbols * stake % total_stake).
    //
    // Returns None on overflow.
    fn obligation(&self, num_scaled_symbols: usize, stake: Stake) -> Option<(usize, Stake)> {
        let stake = stake.0;
        debug_assert!(!stake.is_zero());
        let prod = stake.checked_mul(U256::from(num_scaled_symbols))?;

        let total = self.total_stake.0;
        debug_assert!(!total.is_zero());

        // SAFETY: obligation getting called implies the presence of
        // at least one validator in `validators`, thus we must have
        // total_stake > 0 from the invariant.
        let (quo, rem) = prod.div_rem(total);
        let quo = quo.try_into().ok()?;
        let rem = Stake(rem);
        Some((quo, rem))
    }

    #[cfg(test)]
    // Returns None on overflow.
    fn assign_proportional(
        &self,
        num_base_symbols: usize,
        redundancy: Redundancy,
    ) -> Option<ChunkAssignment> {
        let capacity = self.num_chunks_hint(num_base_symbols, redundancy)?;
        let num_scaled_symbols = redundancy.scale(num_base_symbols)?;
        let mut assignment = ChunkAssignment::with_capacity(self.validators.len(), capacity);

        let mut curr_chunk_id = 0;
        for (i, (_node_id, stake)) in self.validators.iter().enumerate() {
            let (whole_chunks, remainder) = self.obligation(num_scaled_symbols, *stake)?;
            // 1 if there's a non-zero remainder, else 0
            let rounding_chunks = (!remainder.0.is_zero()) as usize;
            let next_chunk_id = curr_chunk_id + whole_chunks + rounding_chunks;
            // TODO(xinyuan): restrict rebroadcast targets for rounding chunks
            assignment.push_range(NodeIndex(i), curr_chunk_id..next_chunk_id);
            curr_chunk_id = next_chunk_id;
        }

        assert!(assignment.num_chunks() >= num_scaled_symbols);
        assert!(assignment.num_chunks() <= capacity);

        Some(assignment)
    }

    // Returns None on overflow.
    fn assign_round_robin(
        &self,
        num_base_symbols: usize,
        redundancy: Redundancy,
    ) -> Option<ChunkAssignment> {
        let capacity = self.num_chunks_hint(num_base_symbols, redundancy)?;
        let num_scaled_symbols = redundancy.scale(num_base_symbols)?;
        let mut assignment = ChunkAssignment::with_capacity(self.validators.len(), capacity);

        let mut remaining: VecDeque<(NodeIndex, usize)> =
            VecDeque::with_capacity(self.validators.len());
        for (i, (_node_id, stake)) in self.validators.iter().enumerate() {
            let (whole_chunks, remainder) = self.obligation(num_scaled_symbols, *stake)?;
            // 1 if there's a non-zero remainder, else 0
            let rounding_chunks = (!remainder.0.is_zero()) as usize;
            let obligation = whole_chunks + rounding_chunks;
            // TODO(xinyuan): restrict rebroadcast targets for rounding chunks
            remaining.push_back((NodeIndex(i), obligation));
        }

        let mut chunk_id = 0;
        while !remaining.is_empty() {
            // optimization to avoid iterating over the whole list
            // when only one validator has remaining obligation
            if remaining.len() == 1 {
                let (node_idx, rem) = remaining.pop_front().unwrap();
                assignment.push_range(node_idx, chunk_id..(chunk_id + rem));
                break;
            }

            remaining.retain_mut(|(node_idx, rem)| {
                if *rem == 0 {
                    return false;
                }
                assignment.push(*node_idx, chunk_id);
                *rem -= 1;
                chunk_id += 1;
                *rem > 0
            })
        }

        assert!(assignment.num_chunks() >= num_scaled_symbols);
        assert!(assignment.num_chunks() <= capacity);

        Some(assignment)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_primitives::{utils::parse_ether, U256};
    use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{NodeId, Stake};
    use monad_validator::validator_set::MAX_VALIDATOR_SET_SIZE;
    use rand::{seq::SliceRandom as _, SeedableRng as _};
    use rand_chacha::ChaCha20Rng;
    use rstest::rstest;

    use super::{ChunkAssignment, EvenPartition, NodeIndex, OrderedNodes, StakePartition};
    use crate::{
        packet::{assigner::stake_partition_num_chunks_hint, BuildError, Result},
        util::Redundancy,
    };

    const R3: Redundancy = Redundancy::from_u8(3);

    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    type NodeNum = u64;
    fn node_id(seed: NodeNum) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        NodeId::new(key_pair.pubkey())
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    // Count how many chunks each NodeIndex receives in an assignment.
    fn chunk_counts(assignment: &ChunkAssignment) -> HashMap<usize, usize> {
        let mut counts = HashMap::new();
        for (_, target) in assignment.iter() {
            *counts.entry(target.node_index.0).or_default() += 1;
        }
        counts
    }

    // Assert chunk_ids are contiguous from 0..num_chunks.
    fn assert_contiguous(assignment: &ChunkAssignment) {
        for (i, (chunk_id, _)) in assignment.iter().enumerate() {
            assert_eq!(chunk_id, i, "chunk_ids must be contiguous from 0");
        }
    }

    // Assert all node indices in an assignment are within bounds.
    fn assert_indices_valid(assignment: &ChunkAssignment, num_nodes: usize) {
        for (_, target) in assignment.iter() {
            assert!(
                target.node_index.0 < num_nodes,
                "node_index {} out of bounds (num_nodes={})",
                target.node_index.0,
                num_nodes,
            );
        }
    }

    // ---------------------------------------------------------------
    // ChunkAssignment::unicast
    // ---------------------------------------------------------------

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(100)]
    fn test_unicast(#[case] num_chunks: usize) {
        let assignment = ChunkAssignment::unicast(num_chunks);
        assert_eq!(assignment.num_chunks(), num_chunks);
        assert_contiguous(&assignment);
        // all chunks target NodeIndex(0)
        for (_, target) in assignment.iter() {
            assert_eq!(target.node_index, NodeIndex(0));
        }
    }

    // ---------------------------------------------------------------
    // ChunkAssignment::materialize
    // ---------------------------------------------------------------

    #[test]
    fn test_materialize_single_node() {
        let node = node_id(1);
        let assignment = ChunkAssignment::unicast(3);
        let chunks = assignment.materialize(64, &node).unwrap();

        assert_eq!(chunks.len(), 3);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_id(), i);
            assert_eq!(chunk.recipient().node_id(), &node);
            assert_eq!(chunk.payload().len(), 64);
        }
    }

    #[test]
    fn test_materialize_multiple_nodes() {
        let partition = EvenPartition::new(vec![node_id(1), node_id(2), node_id(3)]);
        // 2 base symbols * 3 redundancy = 6 symbols
        let assignment = partition.assign(2, R3).unwrap();
        let chunks = assignment.materialize(32, &partition).unwrap();

        assert_eq!(chunks.len(), 6);
        // chunks should round-robin: 0->n1, 1->n2, 2->n3, 3->n1, ...
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_id(), i);
            let expected_node = node_id((i % 3 + 1) as u64);
            assert_eq!(chunk.recipient().node_id(), &expected_node);
        }
    }

    // ---------------------------------------------------------------
    // ChunkAssignment::target (AssignedTarget)
    // ---------------------------------------------------------------

    #[test]
    fn test_target_recipient_lookup() {
        let partition = EvenPartition::new(vec![node_id(1), node_id(2), node_id(3)]);
        // 2 base * 3 redundancy = 6 symbols
        let assignment = partition.assign(2, R3).unwrap();

        // round-robin: 0->n1, 1->n2, 2->n3, 3->n1, 4->n2, 5->n3
        let expected = [1, 2, 3, 1, 2, 3];
        for (chunk_id, &n) in expected.iter().enumerate() {
            let t = assignment.resolve_chunk_id(chunk_id, &partition).unwrap();
            assert_eq!(t.recipient(), &node_id(n));
        }

        // out of range
        assert!(assignment.resolve_chunk_id(6, &partition).is_none());
        assert!(assignment
            .resolve_chunk_id(usize::MAX, &partition)
            .is_none());
    }

    #[test]
    fn test_target_single_node() {
        let node = node_id(42);
        let assignment = ChunkAssignment::unicast(3);

        let t = assignment.resolve_chunk_id(0, &node).unwrap();
        assert_eq!(t.recipient(), &node);
        assert!(t.rebroadcast_targets().is_empty());

        assert!(assignment.resolve_chunk_id(3, &node).is_none());
    }

    #[test]
    fn test_target_rebroadcast_excludes_recipient() {
        let partition = EvenPartition::new(vec![node_id(1), node_id(2), node_id(3)]);
        // 1 base * 3 redundancy = 3 symbols
        let assignment = partition.assign(1, R3).unwrap();

        // chunk 0 -> recipient n1, rebroadcast to [n2, n3]
        let t = assignment.resolve_chunk_id(0, &partition).unwrap();
        assert_eq!(t.recipient(), &node_id(1));
        assert_eq!(t.rebroadcast_targets(), vec![node_id(2), node_id(3)]);

        // chunk 1 -> recipient n2, rebroadcast to [n1, n3]
        let t = assignment.resolve_chunk_id(1, &partition).unwrap();
        assert_eq!(t.recipient(), &node_id(2));
        assert_eq!(t.rebroadcast_targets(), vec![node_id(1), node_id(3)]);

        // chunk 2 -> recipient n3, rebroadcast to [n1, n2]
        let t = assignment.resolve_chunk_id(2, &partition).unwrap();
        assert_eq!(t.recipient(), &node_id(3));
        assert_eq!(t.rebroadcast_targets(), vec![node_id(1), node_id(2)]);
    }

    #[test]
    fn test_target_out_of_range() {
        let partition = EvenPartition::new(vec![node_id(1), node_id(2)]);
        let assignment = partition.assign(1, R3).unwrap();

        assert!(assignment.resolve_chunk_id(99, &partition).is_none());
    }

    // ---------------------------------------------------------------
    // EvenPartition
    // ---------------------------------------------------------------

    #[test]
    fn test_even_partition_empty_nodes() {
        let partition = EvenPartition::<PT>::new(vec![]);
        let assignment = partition.assign(10, R3).unwrap();
        assert!(assignment.is_empty());
    }

    #[rstest]
    #[case(1, 0)]
    #[case(1, 1)]
    #[case(1, 7)]
    #[case(3, 1)]
    #[case(3, 3)]
    #[case(3, 10)]
    #[case(100, 1)]
    #[case(100, 99)]
    #[case(100, 100)]
    #[case(100, 101)]
    #[case(100, 1000)]
    fn test_even_partition_distribution(#[case] num_nodes: usize, #[case] num_base_symbols: usize) {
        let nodes: Vec<_> = (1..=num_nodes as u64).map(node_id).collect();
        let partition = EvenPartition::new(nodes);
        let assignment = partition.assign(num_base_symbols, R3).unwrap();

        let num_symbols = num_base_symbols * 3;
        assert_eq!(assignment.num_chunks(), num_symbols);
        assert_contiguous(&assignment);
        assert_indices_valid(&assignment, num_nodes);

        // each node gets floor(S/N) or ceil(S/N) chunks
        let counts = chunk_counts(&assignment);
        let floor = num_symbols / num_nodes;
        let ceil = num_symbols.div_ceil(num_nodes);
        for &count in counts.values() {
            assert!(
                count == floor || count == ceil,
                "count={count} not in [{floor}, {ceil}]"
            );
        }
    }

    #[test]
    fn test_even_partition_round_robin_order() {
        // 3 nodes, 3 base * 3 redundancy = 9 symbols -> 0,1,2,0,1,2,0,1,2
        let partition = EvenPartition::new(vec![node_id(1), node_id(2), node_id(3)]);
        let assignment = partition.assign(3, R3).unwrap();

        let expected = [0, 1, 2, 0, 1, 2, 0, 1, 2];
        for (i, (_, target)) in assignment.iter().enumerate() {
            assert_eq!(target.node_index.0, expected[i]);
        }
    }

    #[test]
    fn test_even_partition_shuffle() {
        let mut partition = EvenPartition::new(vec![node_id(1), node_id(2), node_id(3)]);
        let before: Vec<_> = (0..3)
            .map(|i| *partition.get(NodeIndex(i)).unwrap())
            .collect();

        partition.shuffle([42u8; 32]);
        let after: Vec<_> = (0..3)
            .map(|i| *partition.get(NodeIndex(i)).unwrap())
            .collect();

        // same elements, different order (with overwhelming probability)
        assert_eq!(before.len(), after.len());
        for node in &before {
            assert!(after.contains(node));
        }
    }

    // ---------------------------------------------------------------
    // StakePartition
    // ---------------------------------------------------------------

    // Reference implementation of stake partitioning using f64 for
    // shares.
    pub(super) struct F64StakePartition<PT: PubKey> {
        // Publisher node excluded.
        //
        // Invariant: validators.map(.1).sum() == 1.0
        validators: Vec<(NodeId<PT>, f64)>,
    }

    impl<PT: PubKey> OrderedNodes<PT> for F64StakePartition<PT> {
        fn get(&self, index: NodeIndex) -> Option<&NodeId<PT>> {
            self.validators.get(index.0).map(|(node_id, _)| node_id)
        }

        fn shuffle(&mut self, seed: [u8; 32]) {
            let mut rng = ChaCha20Rng::from_seed(seed);
            self.validators.shuffle(&mut rng);
        }

        fn len(&self) -> usize {
            self.validators.len()
        }
    }

    impl<PT: PubKey> F64StakePartition<PT> {
        pub(super) fn from_shares(validators: Vec<(NodeId<PT>, f64)>) -> Self {
            Self { validators }
        }

        pub(super) fn assign(
            &self,
            num_base_symbols: usize,
            redundancy: Redundancy,
        ) -> Result<ChunkAssignment> {
            if self.validators.is_empty() {
                return Ok(ChunkAssignment::default());
            }
            self.assign_round_robin(num_base_symbols, redundancy)
        }

        pub(super) fn num_chunks_hint(
            &self,
            num_base_symbols: usize,
            redundancy: Redundancy,
        ) -> Option<usize> {
            let group_size = self.validators.len() + 1;
            stake_partition_num_chunks_hint(num_base_symbols, redundancy, group_size)
        }

        #[expect(unused)] // reference implementation
        fn assign_proportional(
            &self,
            num_base_symbols: usize,
            redundancy: Redundancy,
        ) -> Result<ChunkAssignment> {
            let capacity = self
                .num_chunks_hint(num_base_symbols, redundancy)
                .ok_or(BuildError::TooManyChunks)?;
            let num_scaled_symbols = redundancy
                .scale(num_base_symbols)
                .ok_or(BuildError::TooManyChunks)?;
            let mut assignment = ChunkAssignment::with_capacity(self.validators.len(), capacity);

            let mut curr_chunk_id = 0;
            for (i, (_node_id, share)) in self.validators.iter().enumerate() {
                let obligation = num_scaled_symbols as f64 * share;
                let next_chunk_id: usize = curr_chunk_id + obligation.ceil() as usize;
                assignment.push_range(NodeIndex(i), curr_chunk_id..next_chunk_id);
                curr_chunk_id = next_chunk_id;
            }

            assert!(assignment.num_chunks() >= num_scaled_symbols);
            assert!(assignment.num_chunks() <= capacity);

            Ok(assignment)
        }

        fn assign_round_robin(
            &self,
            num_base_symbols: usize,
            redundancy: Redundancy,
        ) -> Result<ChunkAssignment> {
            use std::collections::VecDeque;

            let capacity = self
                .num_chunks_hint(num_base_symbols, redundancy)
                .ok_or(BuildError::TooManyChunks)?;
            let num_scaled_symbols = redundancy
                .scale(num_base_symbols)
                .ok_or(BuildError::TooManyChunks)?;
            let mut assignment = ChunkAssignment::with_capacity(self.validators.len(), capacity);

            let mut remaining: VecDeque<_> = self
                .validators
                .iter()
                .enumerate()
                .map(|(i, (_node_id, share))| {
                    let obligation = share * num_scaled_symbols as f64;
                    (NodeIndex(i), obligation.ceil() as usize)
                })
                .collect();

            let mut chunk_id = 0;
            while !remaining.is_empty() {
                if remaining.len() == 1 {
                    let (node_idx, rem) = remaining.pop_front().unwrap();
                    assignment.push_range(node_idx, chunk_id..(chunk_id + rem));
                    break;
                }

                remaining.retain_mut(|(node_idx, rem)| {
                    if *rem == 0 {
                        return false;
                    }
                    assignment.push(*node_idx, chunk_id);
                    *rem -= 1;
                    chunk_id += 1;
                    *rem > 0
                })
            }

            assert!(assignment.num_chunks() >= num_scaled_symbols);
            assert!(assignment.num_chunks() <= capacity);

            Ok(assignment)
        }
    }

    // Build matching f64-share and integer-stake partition algorithms
    // from a single list of (node_seed, raw_stake) entries for
    // differential testing.
    fn make_paired_partitions(
        stakes: &[(NodeNum, U256)],
    ) -> (F64StakePartition<PT>, StakePartition<PT>) {
        let total: U256 = stakes
            .iter()
            .map(|(_, s)| *s)
            .fold(U256::ZERO, |a, b| a + b);
        let f64_validators: Vec<_> = stakes
            .iter()
            .map(|(n, s)| {
                let share = Stake::from(*s) / Stake::from(total);
                (node_id(*n), share)
            })
            .collect();
        let int_validators: Vec<_> = stakes.iter().map(|(n, s)| (node_id(*n), *s)).collect();
        (
            F64StakePartition::from_shares(f64_validators),
            StakePartition::from_stakes(int_validators),
        )
    }

    #[test]
    fn test_stake_partition_single_validator() {
        let partition = StakePartition::from_stakes(vec![(node_id(1), 7)]);
        // 10 base * 3 redundancy = 30 symbols
        let assignment = partition.assign(10, R3).unwrap();
        assert_eq!(assignment.num_chunks(), 30);
        assert_contiguous(&assignment);
        for (_, target) in assignment.iter() {
            assert_eq!(target.node_index, NodeIndex(0));
        }
    }

    #[rstest]
    // all assuming redundancy=3
    // equal stakes: each gets ceil(15 * 1/2) = 8 -> 16 total
    #[case(vec![(1, 1u64), (2, 1)], 5, vec![(0, 8), (1, 8)])]
    // 1:2 ratio: ceil(12 * 1/3) = 4, ceil(12 * 2/3) = 8 -> 12 total
    #[case(vec![(1, 1), (2, 2)], 4, vec![(0, 4), (1, 8)])]
    // 1:2:3 ratio: ceil(36 * 1/6) = 6, ceil(36 * 2/6) = 12, ceil(36 * 3/6) = 18
    #[case(vec![(1, 1), (2, 2), (3, 3)], 12, vec![(0, 6), (1, 12), (2, 18)])]
    fn test_stake_partition_chunk_counts(
        #[case] stakes: Vec<(u64, u64)>,
        #[case] num_base_symbols: usize,
        #[case] expected_counts: Vec<(usize, usize)>,
    ) {
        let validators: Vec<_> = stakes.into_iter().map(|(n, s)| (node_id(n), s)).collect();
        let partition = StakePartition::from_stakes(validators);
        let assignment = partition.assign(num_base_symbols, R3).unwrap();

        assert_contiguous(&assignment);
        let counts = chunk_counts(&assignment);
        for (node_idx, expected) in expected_counts {
            assert_eq!(
                counts.get(&node_idx).copied().unwrap_or(0),
                expected,
                "node_idx={node_idx}"
            );
        }
    }

    #[test]
    fn test_stake_partition_round_robin_order() {
        // 1:2 ratio with 2 base * 3 redundancy = 6 symbols
        // obligations: ceil(6*1/3)=2, ceil(6*2/3)=4 -> total 6
        // round-robin: 0,1,0,1,1,1
        let partition = StakePartition::from_stakes(vec![(node_id(1), 1u64), (node_id(2), 2u64)]);
        let assignment = partition.assign(2, R3).unwrap();

        assert_eq!(assignment.num_chunks(), 6);
        let node_indices: Vec<usize> = assignment.iter().map(|(_, t)| t.node_index.0).collect();
        assert_eq!(node_indices, vec![0, 1, 0, 1, 1, 1]);
    }

    #[test]
    fn test_stake_partition_rounding_bounds() {
        // With N validators, rounding can add at most N extra chunks.
        let stakes = vec![
            (node_id(1), 1u64),
            (node_id(2), 2u64),
            (node_id(3), 3u64),
            (node_id(4), 4u64),
        ];
        let partition = StakePartition::from_stakes(stakes);
        let num_base_symbols = 100;
        let assignment = partition.assign(num_base_symbols, R3).unwrap();

        let num_symbols = num_base_symbols * 3;
        assert!(assignment.num_chunks() >= num_symbols);
        assert!(assignment.num_chunks() <= num_symbols + 4);
        assert_contiguous(&assignment);
        assert_indices_valid(&assignment, 4);
    }

    #[rstest]
    // simple 1:2 stake ratio
    #[case(vec![(1, 1), (2, 2)], 10)]
    // three validators with unequal stake
    #[case(vec![(1, 1), (2, 3), (3, 6)], 50)]
    // four validators with small differences
    #[case(vec![(1, 20), (2, 25), (3, 25), (4, 30)], 100)]
    // extreme: one validator has almost all stake
    #[case(vec![(1, 1), (2, 999)], 100)]
    // large validator set
    #[case({
        let n = MAX_VALIDATOR_SET_SIZE as u64;
        (1..=n).map(|i| (i, n)).collect::<Vec<_>>()
    }, 5000)]
    fn test_proportional_vs_round_robin_same_counts(
        #[case] stakes: Vec<(u64, u64)>,
        #[case] num_symbols: usize,
    ) {
        let validators: Vec<_> = stakes.into_iter().map(|(n, s)| (node_id(n), s)).collect();
        let partition = StakePartition::from_stakes(validators);

        let proportional = partition.assign_proportional(num_symbols, R3).unwrap();
        let round_robin = partition.assign_round_robin(num_symbols, R3).unwrap();

        assert_eq!(proportional.num_chunks(), round_robin.num_chunks());
        assert_eq!(chunk_counts(&proportional), chunk_counts(&round_robin));
        assert_contiguous(&proportional);
        assert_contiguous(&round_robin);
    }

    #[test]
    fn test_stake_partition_numerical_stability() {
        use crate::packet::regular;

        const DEFAULT_SEGMENT_LEN: usize = 1400;
        const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;
        const DEFAULT_LAYOUT: regular::PacketLayout =
            regular::PacketLayout::new(DEFAULT_SEGMENT_LEN, DEFAULT_MERKLE_TREE_DEPTH);
        let symbol_len = DEFAULT_LAYOUT.symbol_len();

        for scale in [
            U256::from(1),
            U256::from(u64::MAX),
            parse_ether("100_000_000_000").unwrap(), // 100B
        ] {
            // message_len chosen to produce 10 base symbols, * 2 redundancy = 20
            let message_len = symbol_len * 10;
            let redundancy = Redundancy::from_u8(2);
            let num_base_symbols = DEFAULT_LAYOUT.num_base_symbols(message_len);
            assert_eq!(num_base_symbols, 10);

            // total stake = 16*scale
            let stakes = [
                (1u64, U256::from(1) * scale), // 1/16 -> ceil(20/16) = 2
                (2, U256::from(4) * scale),    // 4/16 -> ceil(80/16) = 5
                (3, U256::from(5) * scale),    // 5/16 -> ceil(100/16) = 7
                (4, U256::from(6) * scale),    // 6/16 -> ceil(120/16) = 8
            ];

            let validators: Vec<_> = stakes.iter().map(|(n, s)| (node_id(*n), *s)).collect();
            let partition = StakePartition::from_stakes(validators);
            let assignment = partition.assign(num_base_symbols, redundancy).unwrap();

            let counts = chunk_counts(&assignment);
            assert_eq!(counts[&0], 2, "scale={scale}");
            assert_eq!(counts[&1], 5, "scale={scale}");
            assert_eq!(counts[&2], 7, "scale={scale}");
            assert_eq!(counts[&3], 8, "scale={scale}");

            // total should be 22 (20 symbols + 2 rounding chunks)
            assert_eq!(assignment.num_chunks(), 22, "scale={scale}");
        }
    }

    #[test]
    fn test_stake_partition_shuffle() {
        let mut partition = StakePartition::from_stakes(vec![
            (node_id(1), 1u64),
            (node_id(2), 1),
            (node_id(3), 1),
            (node_id(4), 1),
        ]);
        let before: Vec<_> = (0..4)
            .map(|i| *partition.get(NodeIndex(i)).unwrap())
            .collect();

        partition.shuffle([7u8; 32]);
        let after: Vec<_> = (0..4)
            .map(|i| *partition.get(NodeIndex(i)).unwrap())
            .collect();

        assert_eq!(before.len(), after.len());
        for node in &before {
            assert!(after.contains(node));
        }
    }

    // ---------------------------------------------------------------
    // Differential: F64StakePartition vs integer StakePartition
    // ---------------------------------------------------------------

    // Assignments are produced from the same (NodeId, stake) input
    // and must agree both in per-node chunk counts and in the
    // per-chunk recipient sequence. The order match relies on both
    // implementations using the same VecDeque round-robin walk.
    fn assert_assignments_match(
        f64_partition: &F64StakePartition<PT>,
        int_partition: &StakePartition<PT>,
        num_base_symbols: usize,
        redundancy: Redundancy,
        ctx: &str,
    ) {
        let f64_assignment = f64_partition.assign(num_base_symbols, redundancy).unwrap();
        let int_assignment = int_partition.assign(num_base_symbols, redundancy).unwrap();

        assert_eq!(
            f64_assignment.num_chunks(),
            int_assignment.num_chunks(),
            "num_chunks mismatch [{ctx}]"
        );
        assert_eq!(
            chunk_counts(&f64_assignment),
            chunk_counts(&int_assignment),
            "chunk_counts mismatch [{ctx}]"
        );
        let f64_indices: Vec<usize> = f64_assignment.iter().map(|(_, t)| t.node_index.0).collect();
        let int_indices: Vec<usize> = int_assignment.iter().map(|(_, t)| t.node_index.0).collect();
        assert_eq!(
            f64_indices, int_indices,
            "per-chunk node_index mismatch [{ctx}]"
        );
    }

    // Deterministic case set chosen so both implementations agree
    // exactly: integer stakes well inside f64 precision and totals
    // that don't trigger sub-ulp rounding boundaries.
    #[rstest]
    #[case(vec![(1, 1u64), (2, 1)], 10)]
    #[case(vec![(1, 1), (2, 2)], 10)]
    #[case(vec![(1, 1), (2, 2), (3, 3)], 30)]
    #[case(vec![(1, 1), (2, 3), (3, 6)], 50)]
    #[case(vec![(1, 4), (2, 5), (3, 5), (4, 6)], 100)]
    #[case(vec![(1, 10), (2, 20), (3, 30), (4, 40)], 1000)]
    fn test_diff_simple_inputs_agree(
        #[case] stakes: Vec<(u64, u64)>,
        #[case] num_base_symbols: usize,
    ) {
        let stakes_u256: Vec<_> = stakes.iter().map(|(n, s)| (*n, U256::from(*s))).collect();
        let (f64_partition, int_partition) = make_paired_partitions(&stakes_u256);
        let ctx = format!("stakes={stakes:?} num_base_symbols={num_base_symbols}");
        assert_assignments_match(&f64_partition, &int_partition, num_base_symbols, R3, &ctx);
    }

    // Random stake distributions for stakes up to 100B.
    #[test]
    fn test_diff_random_stakes_agree() {
        use rand::{Rng as _, SeedableRng as _};

        let mut rng = rand_chacha::ChaCha20Rng::from_seed([42u8; 32]);
        let ether: U256 = parse_ether("1").unwrap(); // 10^18

        for trial in 0..200 {
            let num_validators = rng.gen_range(2..=64);
            let stakes: Vec<(u64, U256)> = (0..num_validators)
                .map(|i| {
                    let a: u64 = rng.gen_range(1..=100_000);
                    let b: u64 = rng.gen_range(1..=1_000_000);
                    let stake = U256::from(a) * U256::from(b) * ether;
                    (i as u64 + 1, stake)
                })
                .collect();

            let num_base_symbols = rng.gen_range(1..=512);
            let (f64_partition, int_partition) = make_paired_partitions(&stakes);
            let ctx = format!("trial={trial} n={num_validators} m={num_base_symbols}");
            assert_assignments_match(&f64_partition, &int_partition, num_base_symbols, R3, &ctx);
        }
    }

    // Large validator set (MAX_VALIDATOR_SET_SIZE) with small
    // integer stakes; both implementations should still agree.
    #[test]
    fn test_diff_large_set_small_stakes_agree() {
        use alloy_primitives::U256;
        use rand::{Rng as _, SeedableRng as _};

        let mut rng = rand_chacha::ChaCha20Rng::from_seed([7u8; 32]);
        let stakes: Vec<_> = (1..=MAX_VALIDATOR_SET_SIZE as u64)
            .map(|i| {
                let s: u32 = rng.gen_range(1..=1_000_000);
                (i, U256::from(s))
            })
            .collect();
        let (f64_partition, int_partition) = make_paired_partitions(&stakes);
        assert_assignments_match(
            &f64_partition,
            &int_partition,
            2048,
            R3,
            "large_set_small_stakes",
        );
    }

    // ---------------------------------------------------------------
    // Broadcast (replicated) pattern
    // ---------------------------------------------------------------

    #[test]
    fn test_broadcast_pattern() {
        // Simulates the broadcast code path: unicast assignment
        // materialized once per recipient.
        let recipients = vec![node_id(1), node_id(2), node_id(3)];
        let num_symbols = 5;
        let segment_len = 32;

        let assignment = ChunkAssignment::unicast(num_symbols);
        let mut all_chunks = Vec::new();
        for recipient in &recipients {
            all_chunks.extend(assignment.materialize(segment_len, recipient).unwrap());
        }

        // 3 recipients * 5 symbols = 15 chunks
        assert_eq!(all_chunks.len(), 15);

        // each recipient gets chunks 0..5
        for (r_idx, recipient) in recipients.iter().enumerate() {
            let start = r_idx * num_symbols;
            for i in 0..num_symbols {
                assert_eq!(all_chunks[start + i].chunk_id(), i);
                assert_eq!(all_chunks[start + i].recipient().node_id(), recipient);
            }
        }
    }

    #[test]
    fn test_broadcast_empty_recipients() {
        let assignment = ChunkAssignment::unicast(5);
        let all_chunks: Vec<super::Chunk<PT>> = Vec::new();
        // no recipients -> no chunks
        assert!(all_chunks.is_empty());
        // but the assignment itself is valid
        assert_eq!(assignment.num_chunks(), 5);
    }
}
