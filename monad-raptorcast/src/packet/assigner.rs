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

use std::{collections::HashMap, ops::Range};

use bytes::BytesMut;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{NodeId, Stake};
use rand::{rngs::StdRng, seq::SliceRandom as _, SeedableRng as _};

use super::{BuildError, Chunk, PacketLayout, Recipient, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkOrder {
    // A roughly GSO-concatenation friendly ordering, such that each
    // recipients' chunks are continuous
    GsoFriendly,

    // Each recipient receives chunks in round-robin order
    RoundRobin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSlice<'a, PT: PubKey> {
    recipient: &'a Recipient<PT>,
    chunk_id_range: Range<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAssignment<'a, PT: PubKey> {
    // The number of chunks (=packets) to be generated
    total_chunks: usize,
    assignments: Vec<ChunkSlice<'a, PT>>,

    // The following fields are hints for performance optimization
    // purposes. Unspecified or inaccurate hints will not result in
    // faulty chunk generation.

    // the order of the chunk slices, used in reordering
    order: Option<ChunkOrder>,

    // used in reordering allocation
    num_recipients: Option<usize>,

    // used in reused symbol encoding optimization
    unique_symbol_id: Option<bool>,
}

type NodeHash<'a> = &'a [u8; 20];

impl<'a, PT: PubKey> ChunkAssignment<'a, PT> {
    fn empty() -> Self {
        Self {
            total_chunks: 0,
            assignments: Vec::new(),

            num_recipients: None,
            order: None,
            unique_symbol_id: None,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            total_chunks: 0,
            assignments: Vec::with_capacity(capacity),

            num_recipients: None,
            order: None,
            unique_symbol_id: None,
        }
    }

    fn hint_num_recipients(&mut self, num_recipients: usize) {
        self.num_recipients = Some(num_recipients);
    }

    fn hint_unique_symbol_id(&mut self, unique: bool) {
        self.unique_symbol_id = Some(unique);
    }

    fn hint_order(&mut self, order: ChunkOrder) {
        self.order = Some(order);
    }

    pub fn total_chunks(&self) -> usize {
        self.total_chunks
    }

    // Return true if the generated chunk each has unique chunk id.
    // Used to provide hint for optimized symbol encoding.
    pub fn unique_chunk_id(&self) -> bool {
        self.unique_symbol_id.unwrap_or(false)
    }

    pub fn is_empty(&self) -> bool {
        self.total_chunks == 0
    }

    fn push(&mut self, recipient: &'a Recipient<PT>, chunk_id_range: Range<usize>) {
        let slice = ChunkSlice {
            recipient,
            chunk_id_range,
        };
        self.push_slice(slice);
    }

    fn push_slice(&mut self, slice: ChunkSlice<'a, PT>) {
        if slice.chunk_id_range.is_empty() {
            return;
        }

        self.total_chunks += slice.chunk_id_range.len();

        // we expect no allocation to occur for performance reasons.
        debug_assert!(self.assignments.len() < self.assignments.capacity());
        self.assignments.push(slice);
    }

    pub fn ensure_order(&mut self, expected_order: Option<ChunkOrder>) {
        let Some(expected_order) = expected_order else {
            // no order specified, nothing to do.
            return;
        };

        if self.order.is_some_and(|o| o == expected_order) {
            // already in the expected order, nothing to do.
            return;
        }

        if self.assignments.is_empty() {
            // empty assignment is compatible with any ordering
            self.order = Some(expected_order);
            return;
        }

        match expected_order {
            ChunkOrder::GsoFriendly => {
                self.assignments = Self::reorder_to_gso(
                    std::mem::take(&mut self.assignments),
                    self.num_recipients,
                );
            }
            ChunkOrder::RoundRobin => {
                self.assignments = Self::reorder_to_round_robin(
                    std::mem::take(&mut self.assignments),
                    self.total_chunks,
                );
            }
        }
    }

    // This method is best effort, as it may not always produce the
    // optimal GSO grouping if the chunk slices's chunk range are not
    // ordered.
    fn reorder_to_gso(
        chunk_slices: Vec<ChunkSlice<'a, PT>>,
        hint_num_recipients: Option<usize>,
    ) -> Vec<ChunkSlice<'a, PT>> {
        use std::collections::hash_map::Entry;

        let num_recipients = hint_num_recipients.unwrap_or(chunk_slices.len());
        let mut messages: HashMap<NodeHash, (&'a Recipient<_>, Vec<Range<usize>>)> =
            HashMap::with_capacity(num_recipients);

        let mut out_slices_count = 0;

        for chunk_slice in chunk_slices {
            let recipient = chunk_slice.recipient;
            let key = recipient.node_hash();

            match messages.entry(key) {
                Entry::Vacant(e) => {
                    out_slices_count += 1;
                    e.insert((recipient, vec![chunk_slice.chunk_id_range]));
                }
                Entry::Occupied(mut e) => {
                    let (_recipient, ranges) = e.get_mut();
                    let last = ranges.last_mut().expect("occupied entry never empty");
                    if last.end + 1 == chunk_slice.chunk_id_range.start {
                        last.end = chunk_slice.chunk_id_range.end;
                        continue;
                    }

                    out_slices_count += 1;
                    ranges.push(chunk_slice.chunk_id_range);
                }
            }
        }

        let mut reordered = Vec::with_capacity(out_slices_count);
        for (recipient, ranges) in messages.into_values() {
            for range in ranges {
                reordered.push(ChunkSlice {
                    recipient,
                    chunk_id_range: range,
                });
            }
        }
        reordered
    }

    fn reorder_to_round_robin(
        chunk_slices: Vec<ChunkSlice<'a, PT>>,
        total_chunks: usize,
    ) -> Vec<ChunkSlice<'a, PT>> {
        use std::collections::{BTreeMap, VecDeque};

        let mut buckets: BTreeMap<NodeHash, VecDeque<ChunkSlice<_>>> = BTreeMap::new();

        // Group by recipients
        for slice in chunk_slices {
            buckets
                .entry(slice.recipient.node_hash())
                .or_default()
                .push_back(slice);
        }

        // Each recipient get their own queue of chunks.
        let mut queues: Vec<VecDeque<ChunkSlice<_>>> = buckets.into_values().collect();

        // Optimized algorithm:
        //
        // 1. go through each recipient's queue in order
        // 2. if the queue is not empty, add the front chunk into the output
        // 3. otherwise, delete the empty queue from the Vec of queues
        // 4. repeat until there is no queue left
        let mut output = Vec::with_capacity(total_chunks);

        while !queues.is_empty() {
            queues.retain_mut(|queue| {
                let Some(front) = queue.front_mut() else {
                    return false; // drop the empty queue
                };

                if front.chunk_id_range.is_empty() {
                    queue.pop_front();
                    return !queue.is_empty(); // drop queue if empty
                }

                let start = front.chunk_id_range.start;
                let chunk_id_range = start..(start + 1);
                front.chunk_id_range.start = start + 1;

                output.push(ChunkSlice {
                    recipient: front.recipient,
                    chunk_id_range,
                });

                true // keep the non-empty queue
            });
        }

        output
    }

    pub fn generate(&self, layout: PacketLayout) -> Vec<Chunk<PT>> {
        let mut buffer = BytesMut::zeroed(self.total_chunks * layout.segment_len());
        let mut all_chunks = Vec::with_capacity(self.total_chunks);

        for slice in &self.assignments {
            split_off_chunks_into(
                &mut all_chunks,
                &mut buffer,
                slice.recipient,
                slice.chunk_id_range.clone(),
                layout.segment_len(),
            );
        }

        debug_assert_eq!(all_chunks.len(), self.total_chunks);
        debug_assert!(buffer.is_empty());

        all_chunks
    }

    // Two assignments are equivalent if their normalized chunks are
    // equal. Used in testing.
    //
    // Internally, it breaks down the underlying chunk slices into a
    // single-chunk slices and sorted in a consistent order.
    #[cfg(test)]
    pub fn normalized_chunks(&self) -> Vec<ChunkSlice<'a, PT>> {
        let mut slices = Vec::with_capacity(self.total_chunks);
        for slice in &self.assignments {
            for chunk_id in slice.chunk_id_range.clone() {
                slices.push(ChunkSlice {
                    recipient: slice.recipient,
                    chunk_id_range: chunk_id..(chunk_id + 1),
                })
            }
        }
        slices.sort_unstable_by_key(|s| (s.chunk_id_range.start, s.recipient.node_hash()));
        slices
    }
}

pub(crate) struct Replicated<PT: PubKey> {
    // each recipient receives all the same chunks, used by broadcast
    // target and point-to-point target
    recipients: Vec<Recipient<PT>>,
}

impl<PT: PubKey> Replicated<PT> {
    pub fn from_unicast(node_id: NodeId<PT>) -> Self {
        Self {
            recipients: vec![Recipient::new(node_id)],
        }
    }

    pub fn from_broadcast(recipients: Vec<NodeId<PT>>) -> Self {
        Self {
            recipients: recipients.into_iter().map(Recipient::new).collect(),
        }
    }
}

pub(crate) trait ChunkAssigner<PT: PubKey> {
    fn assign_chunks(
        &self,
        num_symbols: usize,
        preferred_order: Option<ChunkOrder>,
    ) -> Result<ChunkAssignment<PT>>;
}

impl<PT: PubKey> ChunkAssigner<PT> for Replicated<PT> {
    fn assign_chunks(
        &self,
        num_symbols: usize,
        preferred_order: Option<ChunkOrder>,
    ) -> Result<ChunkAssignment<PT>> {
        if self.recipients.is_empty() {
            tracing::warn!("no recipients specified for chunk assigner");
            return Ok(ChunkAssignment::empty());
        }

        let total_chunks = num_symbols * self.recipients.len();
        let mut assignment;

        match preferred_order {
            None | Some(ChunkOrder::GsoFriendly) => {
                assignment = ChunkAssignment::with_capacity(self.recipients.len());
                assignment.hint_order(ChunkOrder::GsoFriendly);
                for recipient in &self.recipients {
                    assignment.push(recipient, 0..num_symbols);
                }
            }
            Some(ChunkOrder::RoundRobin) => {
                assignment = ChunkAssignment::with_capacity(total_chunks);
                assignment.hint_order(ChunkOrder::RoundRobin);
                for chunk_id in 0..num_symbols {
                    for recipient in &self.recipients {
                        assignment.push(recipient, chunk_id..(chunk_id + 1));
                    }
                }
            }
        };

        debug_assert_eq!(assignment.total_chunks(), total_chunks);
        assignment.hint_num_recipients(self.recipients.len());
        assignment.hint_unique_symbol_id(self.recipients.len() <= 1);
        Ok(assignment)
    }
}

pub(crate) struct Partitioned<PT: PubKey> {
    weighted_nodes: Vec<(Recipient<PT>, Stake)>,
    total_stake: Stake,
}

impl<PT: PubKey> Partitioned<PT> {
    // This assigner is only used for full-node raptorcast, which is
    // based on homogeneous peers. RaptorCast between validators has
    // switched to StakeBasedWithRC assigner.
    #[cfg_attr(not(test), expect(unused))]
    pub fn from_validator_set(validator_set: Vec<(NodeId<PT>, Stake)>) -> Self {
        let mut total_stake = Stake::ZERO;
        let weighted_nodes = validator_set
            .into_iter()
            .map(|(nid, stake)| {
                total_stake += stake;
                (Recipient::new(nid), stake)
            })
            .collect();

        Self {
            weighted_nodes,
            total_stake,
        }
    }

    pub fn from_homogeneous_peers(peers: Vec<NodeId<PT>>) -> Self {
        let weighted_nodes: Vec<_> = peers
            .into_iter()
            .map(|p| (Recipient::new(p), Stake::ONE))
            .collect();
        let total_stake = Stake::from(weighted_nodes.len() as u64);
        Self {
            weighted_nodes,
            total_stake,
        }
    }

    fn assign_gso(&self, num_symbols: usize) -> ChunkAssignment<PT> {
        let num_nodes = self.weighted_nodes.len();
        let mut assignment = ChunkAssignment::with_capacity(num_nodes);
        assignment.hint_order(ChunkOrder::GsoFriendly);
        assignment.hint_num_recipients(num_nodes);
        assignment.hint_unique_symbol_id(true);

        let mut running_stake = Stake::ZERO;
        for (recipient, stake) in &self.weighted_nodes {
            let start_id = (num_symbols as f64 * (running_stake / self.total_stake)) as usize;
            running_stake += *stake;
            let end_id = (num_symbols as f64 * (running_stake / self.total_stake)) as usize;
            assignment.push(recipient, start_id..end_id);
        }

        assignment
    }
}

impl<PT: PubKey> ChunkAssigner<PT> for Partitioned<PT> {
    fn assign_chunks(
        &self,
        num_symbols: usize,
        _preferred_order: Option<ChunkOrder>,
    ) -> Result<ChunkAssignment<PT>> {
        if self.weighted_nodes.is_empty() {
            tracing::warn!("no nodes specified for partitioned chunk assigner");
            return Ok(ChunkAssignment::empty());
        }
        if self.total_stake == Stake::ZERO {
            return Err(BuildError::ZeroTotalStake);
        }

        Ok(self.assign_gso(num_symbols))
    }
}

// each validator gets an additional rounding chunk
pub(crate) struct StakeBasedWithRC<PT: PubKey> {
    validator_set: Vec<(Recipient<PT>, Stake)>,
    total_stake: Stake,
}

impl<PT: PubKey> StakeBasedWithRC<PT> {
    pub fn seed_from_app_message_hash(app_message_hash: &[u8; 20]) -> [u8; 32] {
        let mut padded_seed = [0u8; 32];
        padded_seed[..20].copy_from_slice(app_message_hash);
        padded_seed
    }

    // Shuffle the validator stake map for chunk assignment. This uses
    // a deterministic seed, as in the future, it will be required
    // that the leader and all validators compute the shuffling in the
    // same way (for features not yet implemented).  In the future,
    // this should be done using known shuffling algorithm to allow
    // for easy implementation in other languages, e.g., using Mt19937
    // and Fisher Yates shuffle.
    pub fn shuffle_validators<ST>(
        view: &crate::util::ValidatorsView<ST>,
        seed: [u8; 32],
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Stake)>
    where
        ST: CertificateSignatureRecoverable,
    {
        let mut validator_set = view
            .iter()
            .map(|(node_id, stake)| (*node_id, stake))
            .collect::<std::collections::BinaryHeap<_>>()
            .into_sorted_vec();
        let mut rng = StdRng::from_seed(seed);
        validator_set.shuffle(&mut rng);
        validator_set
    }

    pub fn from_validator_set(validator_set: Vec<(NodeId<PT>, Stake)>) -> Self {
        let mut total_stake = Stake::ZERO;
        let validator_set: Vec<_> = validator_set
            .into_iter()
            .map(|(nid, stake)| {
                total_stake += stake;
                (Recipient::new(nid), stake)
            })
            .collect();

        Self {
            validator_set,
            total_stake,
        }
    }
}

impl<PT: PubKey> ChunkAssigner<PT> for StakeBasedWithRC<PT> {
    fn assign_chunks(
        &self,
        num_symbols: usize,
        _preferred_order: Option<ChunkOrder>,
    ) -> Result<ChunkAssignment<PT>> {
        if self.validator_set.is_empty() {
            tracing::warn!("no nodes specified for partitioned chunk assigner");
            return Ok(ChunkAssignment::empty());
        }
        if self.total_stake == Stake::ZERO {
            return Err(BuildError::ZeroTotalStake);
        }

        let num_validators = self.validator_set.len();
        let obligations = self
            .validator_set
            .iter()
            .map(|(_, s)| num_symbols as f64 * (*s / self.total_stake));

        let mut ic = vec![0usize; num_validators];
        let mut rc = vec![false; num_validators];

        for (i, o) in obligations.enumerate() {
            ic[i] = o as usize; // ic := floor(o)
            rc[i] = o.fract() > 0.0; // rc := ceil(o - floor(o))
        }

        let mut assignment = ChunkAssignment::with_capacity(num_validators * 2);
        assignment.hint_order(ChunkOrder::GsoFriendly);
        assignment.hint_num_recipients(num_validators);
        assignment.hint_unique_symbol_id(true);

        let mut curr_chunk_id = 0;
        for (i, (recipient, _stake)) in self.validator_set.iter().enumerate() {
            let next_chunk_id = curr_chunk_id + ic[i] + rc[i] as usize;
            assignment.push(recipient, curr_chunk_id..next_chunk_id);
            curr_chunk_id = next_chunk_id;
        }

        Ok(assignment)
    }
}

fn split_off_chunks_into<PT: PubKey>(
    output: &mut Vec<Chunk<PT>>,
    buffer: &mut BytesMut,
    recipient: &Recipient<PT>,
    chunk_ids: Range<usize>,
    segment_len: usize,
) {
    debug_assert!(
        buffer.len() >= segment_len * chunk_ids.len(),
        "insufficient buffer space"
    );

    for chunk_id in chunk_ids {
        let segment = buffer.split_to(segment_len);
        let chunk = Chunk::new(chunk_id, recipient.clone(), segment);
        output.push(chunk);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        ops::Range,
    };

    use alloy_primitives::U256;
    use itertools::Itertools as _;
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{NodeId, Stake};
    use rand::{seq::SliceRandom, Rng};
    use rand_distr::{Distribution, Normal};

    use super::{ChunkAssignment, ChunkOrder, Partitioned, StakeBasedWithRC};
    use crate::{
        packet::{assigner::Replicated, ChunkAssigner as _, PacketLayout, Recipient},
        util::Redundancy,
    };

    const DEFAULT_SEGMENT_LEN: usize = 1400;
    const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;
    const DEFAULT_LAYOUT: PacketLayout =
        PacketLayout::new(DEFAULT_SEGMENT_LEN, DEFAULT_MERKLE_TREE_DEPTH);
    const DEFAULT_SYMBOL_LEN: usize = DEFAULT_LAYOUT.symbol_len();

    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    type NodeNum = u64;
    fn node_id(seed: NodeNum) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        NodeId::new(key_pair.pubkey())
    }

    struct StaticAssigner {
        slices: Vec<(Recipient<PT>, Range<usize>)>,
    }

    impl StaticAssigner {
        fn from_template(slices: &[(NodeNum, Range<usize>)]) -> Self {
            let recipients: HashMap<NodeNum, Recipient<_>> = slices
                .iter()
                .map(|(n, _)| n)
                .unique()
                .map(|n| (*n, Recipient::new(node_id(*n))))
                .collect();
            let slices = slices
                .iter()
                .map(|(n, range)| (recipients[n].clone(), range.clone()))
                .collect();
            Self { slices }
        }

        fn assign_chunks(&self) -> ChunkAssignment<PT> {
            let mut assignment = ChunkAssignment::with_capacity(self.slices.len());
            for slice in &self.slices {
                assignment.push(&slice.0, slice.1.clone());
            }
            assignment
        }
    }

    fn rand_validator_set(rng: &mut impl rand::Rng, max_n: usize) -> Vec<(NodeId<PT>, Stake)> {
        let n: usize = rng.gen_range(1..=max_n);
        let mut validator_set = Vec::with_capacity(n);

        let mon = U256::from(1_000_000_000_000_000_000u64);
        let min_stake = mon * U256::from(100_000); // approximated
        let mean = f64::from(min_stake * U256::from(100)); // estimated
        let std_dev = f64::from(mon * U256::from(500_000)); // estimated
        let stake_distr = Normal::new(mean, std_dev).unwrap();

        loop {
            let mut total_stake = Stake::ZERO;

            for i in 1..=n {
                let stake = stake_distr.sample(rng).max(f64::from(min_stake));
                let stake = Stake::from(u256_from_f64_lossy(stake));

                // NOTE: we don't forbid individual stake to be zero,
                // as long as total stake is non-zero. we do this to
                // test the robustness of assignment algorithm.
                total_stake += stake;
                validator_set.push((node_id(i as u64), stake));
            }

            if total_stake != Stake::ZERO {
                break;
            }
        }

        validator_set.shuffle(rng);
        validator_set
    }

    fn rand_node_set(rng: &mut impl rand::Rng, max_n: usize) -> Vec<NodeId<PT>> {
        let n: usize = rng.gen_range(1..=max_n);
        let mut node_set = Vec::with_capacity(n);

        for i in 1..=n {
            node_set.push(node_id(i as u64));
        }

        node_set.shuffle(rng);
        node_set
    }

    #[test]
    fn test_replicated_assignment() {
        let rng = &mut rand::thread_rng();

        for _ in 0..100 {
            let node_set = rand_node_set(rng, 2000);
            let assigner = Replicated::from_broadcast(node_set);
            let num_symbols = rng.gen_range(0..10);

            let assignment_1 = assigner
                .assign_chunks(num_symbols, Some(ChunkOrder::GsoFriendly))
                .expect("should assign successfully");
            let assignment_2 = assigner
                .assign_chunks(num_symbols, Some(ChunkOrder::RoundRobin))
                .expect("should assign successfully");

            assert_eq!(
                assignment_1.normalized_chunks(),
                assignment_2.normalized_chunks()
            );
        }
    }

    #[test]
    fn test_partitioned_assignment() {
        let rng = &mut rand::thread_rng();

        for _ in 0..30 {
            let validator_set = rand_validator_set(rng, 2000);
            let assigner = Partitioned::from_validator_set(validator_set);
            let num_symbols = rng.gen_range(0..1000);

            let assignment_1 = assigner
                .assign_chunks(num_symbols, Some(ChunkOrder::GsoFriendly))
                .expect("should assign successfully");
            let assignment_2 = assigner
                .assign_chunks(num_symbols, Some(ChunkOrder::RoundRobin))
                .expect("should assign successfully");

            assert_eq!(
                assignment_1.normalized_chunks(),
                assignment_2.normalized_chunks()
            );
        }
    }

    #[test]
    fn test_stake_with_rc() {
        // test the numerical stability of stakes on different scales
        for scale in [
            U256::from(1),
            U256::from(u64::MAX),
            U256::MAX / U256::from(16),
        ] {
            // c_h = 20
            let message_len = DEFAULT_SYMBOL_LEN * 10;
            let redundancy = Redundancy::from_u8(2);

            // total stake = 16*scale
            let validator_set = vec![
                // get floor(1/16*20) chunks + 1 rounding chunk (total: 2)
                (node_id(1), Stake::from(U256::from(1) * scale)),
                // get floor(4/16*20) chunks (total: 5)
                (node_id(2), Stake::from(U256::from(4) * scale)),
                // get floor(5/16*20) chunks + 1 rounding chunk (total: 7)
                (node_id(3), Stake::from(U256::from(5) * scale)),
                // get floor(6/16*20) chunks + 1 rounding chunk (total: 8)
                (node_id(4), Stake::from(U256::from(6) * scale)),
            ];

            let num_symbols = DEFAULT_LAYOUT
                .calc_num_symbols(message_len, redundancy)
                .expect("should not overflow");
            let assigner = StakeBasedWithRC::from_validator_set(validator_set);
            let assignment = assigner
                .assign_chunks(num_symbols, None)
                .expect("should assign successfully");

            let expected_assigner =
                StaticAssigner::from_template(&[(1, 0..2), (2, 2..7), (3, 7..14), (4, 14..22)]);
            let expected_assignment = expected_assigner.assign_chunks();

            assert_eq!(
                assignment.normalized_chunks(),
                expected_assignment.normalized_chunks()
            );
        }
    }

    #[test]
    fn test_stake_with_rc_properties() {
        let rng = &mut rand::thread_rng();
        for _ in 0..50 {
            let validator_set = rand_validator_set(rng, 2000);
            let n_validators = validator_set.len();
            let assigner = Partitioned::from_validator_set(validator_set.clone());
            let assigner_rc = StakeBasedWithRC::from_validator_set(validator_set);

            // estimated from at most 2MB data, 1400-byte segments, 3x redundancy
            let num_symbols = rng.gen_range(1..5000);
            let assignment = assigner
                .assign_chunks(num_symbols, None)
                .expect("should assign successfully");
            let assignment_rc = assigner_rc
                .assign_chunks(num_symbols, None)
                .expect("should assign successfully");

            // assignment with rc must produce at least the same number of chunks as without rc
            assert!(assignment_rc.total_chunks() >= assignment.total_chunks());
            // the difference in total chunks must not exceed number of validators
            assert!(assignment_rc.total_chunks() - assignment.total_chunks() <= n_validators);

            let chunk_ids: BTreeSet<_> = assignment
                .assignments
                .iter()
                .flat_map(|slice| slice.chunk_id_range.clone())
                .collect();
            let chunk_ids_rc: BTreeSet<_> = assignment_rc
                .assignments
                .iter()
                .flat_map(|slice| slice.chunk_id_range.clone())
                .collect();

            // both assignments must be continuous from 0 to total_chunks - 1
            assert_eq!(chunk_ids.len(), assignment.total_chunks());
            assert_eq!(chunk_ids.first().cloned(), Some(0));
            assert_eq!(
                chunk_ids.last().cloned(),
                Some(assignment.total_chunks() - 1)
            );

            assert_eq!(chunk_ids_rc.len(), assignment_rc.total_chunks());
            assert_eq!(chunk_ids_rc.first().cloned(), Some(0));
            assert_eq!(
                chunk_ids_rc.last().cloned(),
                Some(assignment_rc.total_chunks() - 1)
            );

            let validator_to_chunks: HashMap<_, _> = assignment
                .assignments
                .iter()
                .map(|slice| (slice.recipient.node_hash(), slice.chunk_id_range.len()))
                .collect();
            let validator_to_chunks_rc: HashMap<_, _> = assignment_rc
                .assignments
                .iter()
                .map(|slice| (slice.recipient.node_hash(), slice.chunk_id_range.len()))
                .collect();

            for (validator, num_chunks) in validator_to_chunks {
                let num_chunks_rc = validator_to_chunks_rc.get(validator);
                // each validator that exist in non-rc assignment must
                // also exist in rc assignment
                assert!(num_chunks_rc.is_some());
                // each validator must get at least as many chunks in rc assignment
                assert!(*num_chunks_rc.unwrap() >= num_chunks);
            }
        }
    }

    // Ported from alloy_primitives::U256::from_f64_lossy from a newer version
    fn u256_from_f64_lossy(value: f64) -> U256 {
        if value >= 1.0 {
            let bits = value.to_bits();
            let exponent = ((bits >> 52) & 0x7ff) - 1023;
            let mantissa = (bits & 0x0f_ffff_ffff_ffff) | 0x10_0000_0000_0000;
            if exponent <= 52 {
                U256::from(mantissa >> (52 - exponent))
            } else if exponent >= 256 {
                U256::MAX
            } else {
                U256::from(mantissa) << U256::from(exponent - 52)
            }
        } else {
            U256::ZERO
        }
    }
}
