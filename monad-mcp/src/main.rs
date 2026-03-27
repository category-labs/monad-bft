use std::collections::{BTreeMap, HashMap, btree_map};

trait PipelineStage {}

struct Instant(std::time::Instant);
struct MerkleRoot(u64);
struct EncryptionKey;
struct DecryptionShare;
struct Bytes;

struct Cyphertext<T> {
    data: Vec<u8>,
    _marker: std::marker::PhantomData<T>,
}

struct EncryptedProposal {
    id: ProposalIndex,
    merkle_root: MerkleRoot,
    encryption_key: Cyphertext<EncryptionKey>,
    data: Bytes,
}

impl EncryptedProposal {
    fn id(&self) -> ProposalIndex {
        self.id
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ProposalIndex(usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId;

enum ChunkVote<SCT> {
    Positive {
        proposal_index: ProposalIndex,
        signature: SCT,
    },
    Negative {
        signature: SCT,
    },
}

struct PositiveVoteCertificate;
struct NegativeVoteCertificate;
struct BottomVoteCertificate;

struct CertifiedProposal {}

struct BlockCertificate {}

struct ChunkHeader {
    merkle_root: MerkleRoot,
    encryption_key: Cyphertext<EncryptionKey>,
}

#[derive(Default)]
struct ChunkVotes<SCT> {
    header: Option<ChunkHeader>,
    votes: HashMap<NodeId, ChunkVote<SCT>>,
}

impl<SCT> ChunkVotes<SCT> {
    fn set_chunk_header(&mut self, chunk_header: ChunkHeader) {
        if self.header.is_some() {
            return;
        }
        self.header = Some(chunk_header);
    }

    fn insert_vote(&mut self, node_id: NodeId, vote: ChunkVote<SCT>) {
        if self.votes.contains_key(&node_id) {
            tracing::warn!("double vote received");
            return;
        }
        self.votes.insert(node_id, vote);
    }
}

// TODO: maybe just implement `type ProposalMap<T> = TotalProposalMap<Option<T>>?`
struct ProposalMap<T> {
    capacity: usize,
    values: BTreeMap<ProposalIndex, T>,
}

struct TotalProposalMap<T> {
    values: Box<[T]>,
}

impl<T> ProposalMap<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: BTreeMap::new(),
        }
    }

    fn get_mut_or_default(&mut self, proposal_id: ProposalIndex) -> &mut T
    where
        T: Default,
    {
        assert!(self.proposal_id_valid(&proposal_id));
        self.values.entry(proposal_id).or_insert_with(T::default)
    }

    fn get_mut(&mut self, proposal_id: &ProposalIndex) -> Option<&mut T> {
        self.values.get_mut(proposal_id)
    }

    fn proposal_id_valid(&self, _proposal_id: &ProposalIndex) -> bool {
        // e.g. validate the proposal index to be within the capacity
        true
    }

    fn try_into_total(self) -> Option<TotalProposalMap<T>> {
        assert!(self.values.len() <= self.capacity);

        if self.values.len() < self.capacity {
            return None; // partial map
        }

        let mut values = Vec::new();

        // relies on ordering of the proposal ids in the
        // BTreeMap. TODO: make this more robust?
        for (proposal_id, value) in self.values {
            assert!(proposal_id.0 < self.capacity);
            values.push(value);
        }

        assert!(values.len() == self.capacity);

        let values = values.into_boxed_slice();
        Some(TotalProposalMap { values })
    }

    fn into_total<S>(mut self) -> TotalProposalMap<S>
    where
        S: From<Option<T>>,
    {
        assert!(self.values.len() <= self.capacity);
        let mut values = Vec::new();

        for proposal_id in 0..self.capacity {
            let proposal_index = ProposalIndex(proposal_id);
            let value = self.values.remove(&proposal_index);
            values.push(S::from(value));
        }
        assert!(self.values.is_empty());

        let values = values.into_boxed_slice();
        assert!(values.len() == self.capacity);
        TotalProposalMap { values }
    }
}

pub struct BlockVote<SCT> {
    votes: TotalProposalMap<Option<ChunkVote<SCT>>>,
}

trait McpStage {
    type ProposalValue;
    type NextStage;
}

// collecting proposals and votes (<2 delta)
struct Stage0;

impl McpStage for Stage0 {
    type ProposalValue = ChunkVotes;
    type NextStage = Stage1;
}

struct McpSlice<S: McpStage> {
    data: ProposalMap<S::ProposalValue>,
}

impl McpSlice<Stage0> {
    fn new(num_proposals: usize) -> Self {
        Self {
            data: ProposalMap::new(num_proposals),
        }
    }

    fn on_chunk(&mut self, node_id: NodeId, header: ChunkHeader, vote: ChunkVote<SCT>) {
        let proposal_id = header.proposal_id();
        let chunk_votes = self.data.get_mut_or_default(proposal_id);
        chunk_votes.set_chunk_header(header);
        chunk_votes.insert_vote(node_id, vote);
    }

    fn on_timeout(self) -> (BlockVote, McpSlice<Stage1>) {
        for proposal_id in 0..self.data.capacity {
            let proposal_index = ProposalIndex(proposal_id);
            let chunk_votes = self.data.get_mut_or_default(proposal_index);
            if chunk_votes.header.is_none() {
                tracing::warn!("proposal {} did not receive a chunk header", proposal_id);
                continue;
            }
        }
    }
}

// preparing commit
struct Stage1;

impl McpStage for Stage1 {
    type ProposalValue = Proposal;
    type NextStage = Stage1;
}

fn main() {}
