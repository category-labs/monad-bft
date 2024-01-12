use std::fmt::Debug;

use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hashable, Hasher, HasherType},
};
use monad_types::{BlockId, NodeId, Round, SeqNum};
use zerocopy::AsBytes;

use crate::{
    block_validator::BlockValidator, payload::Payload, quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

/// This trait represents a consensus block
pub trait BlockType: Clone + PartialEq + Eq {
    type NodeIdPubKey: PubKey;
    /// Unique hash for the block
    fn get_id(&self) -> BlockId;

    /// Round in which this block was proposed
    fn get_round(&self) -> Round;

    /// Node which proposed this block
    fn get_author(&self) -> NodeId<Self::NodeIdPubKey>;

    /// returns the BlockId for the block referenced by
    /// the QC contained in this block
    fn get_parent_id(&self) -> BlockId;

    /// returns the Round for the block referenced by
    /// the QC contained in this block
    fn get_parent_round(&self) -> Round;

    /// Sequence number when this block was proposed
    fn get_seq_num(&self) -> SeqNum;
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone)]
pub struct Block<SCT: SignatureCollection> {
    /// proposer of this block
    pub author: NodeId<SCT::NodeIdPubKey>,

    /// round this block was proposed in
    pub round: Round,

    /// protocol agnostic data for the blockchain
    pub payload: Payload,

    /// Certificate of votes for this block
    pub qc: QuorumCertificate<SCT>,

    /// Unique hash used to identify the block
    id: BlockId,
}

impl<SCT: SignatureCollection> PartialEq for Block<SCT> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<SCT: SignatureCollection> Eq for Block<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for Block<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.author)
            .field("round", &self.round)
            .field("qc_info", &self.qc.info)
            .field("qc", &self.qc)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<SCT: SignatureCollection> Hashable for Block<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.id.hash(state);
    }
}

impl<SCT: SignatureCollection> Block<SCT> {
    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(
        author: NodeId<SCT::NodeIdPubKey>,
        round: Round,
        payload: &Payload,
        qc: &QuorumCertificate<SCT>,
    ) -> Self {
        Self {
            author,
            round,
            payload: payload.clone(),
            qc: qc.clone(),
            id: {
                let mut _block_hash_span = tracing::info_span!("block_hash_span").entered();
                let mut state = HasherType::new();
                author.hash(&mut state);
                state.update(round.as_bytes());
                payload.hash(&mut state);
                state.update(qc.get_block_id().0.as_bytes());
                state.update(qc.get_hash().as_bytes());

                BlockId(state.hash())
            },
        }
    }

    /// Try to create a Block from an UnverifiedBlock, verifying
    /// with the TransactionValidator
    pub fn try_from_unverified(
        unverified: UnverifiedBlock<SCT>,
        validator: &impl BlockValidator,
    ) -> Option<Self> {
        validator
            .validate(&unverified.0.payload.txns)
            .then_some(unverified.0)
    }
}

impl<SCT: SignatureCollection> BlockType for Block<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn get_round(&self) -> Round {
        self.round
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
        self.author
    }

    fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.payload.seq_num
    }
}

/// A block alongside the list of RLP encoded full transactions
/// The transactions have not been verified
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnverifiedBlock<SCT: SignatureCollection>(pub Block<SCT>);

impl<SCT: SignatureCollection> UnverifiedBlock<SCT> {
    pub fn new(block: Block<SCT>) -> Self {
        Self(block)
    }
}

impl<SCT: SignatureCollection> From<Block<SCT>> for UnverifiedBlock<SCT> {
    fn from(value: Block<SCT>) -> Self {
        Self(value)
    }
}

impl<SCT: SignatureCollection> Hashable for UnverifiedBlock<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.0.hash(state);
    }
}
