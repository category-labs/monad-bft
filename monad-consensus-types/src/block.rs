use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hashable, Hasher, HasherType},
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use zerocopy::AsBytes;

use crate::{
    payload::{FullTransactionList, Payload},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

/// This trait represents a consensus block
pub trait BlockType<SCT: SignatureCollection>: Clone + PartialEq + Eq {
    type NodeIdPubKey: PubKey;
    type TxnHash: PartialEq + Eq + std::hash::Hash;
    /// Unique hash for the block
    fn get_id(&self) -> BlockId;

    /// Round in which this block was proposed
    fn get_round(&self) -> Round;

    /// Epoch in which this block was proposed
    fn get_epoch(&self) -> Epoch;

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

    /// get list of all txn hashes in this block
    fn get_txn_hashes(&self) -> Vec<Self::TxnHash>;

    fn is_txn_list_empty(&self) -> bool;

    fn get_txn_list_len(&self) -> usize;

    /// get a reference to the block's QC
    fn get_qc(&self) -> &QuorumCertificate<SCT>;

    fn get_unvalidated_block(self) -> Block<SCT>;

    fn get_unvalidated_block_ref(&self) -> &Block<SCT>;
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone)]
pub struct Block<SCT: SignatureCollection> {
    /// proposer of this block
    pub author: NodeId<SCT::NodeIdPubKey>,

    /// Epoch this block was proposed in
    pub epoch: Epoch,

    /// round this block was proposed in
    pub round: Round,

    /// protocol agnostic data for the blockchain
    pub payload: Payload,

    /// Certificate of votes for the parent block
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
            .field("epoch", &self.epoch)
            .field("round", &self.round)
            .field("qc", &self.qc)
            .field("id", &self.id)
            .field("txn_payload_len", &self.payload.txns.bytes().len())
            .field("seq_num", &self.payload.seq_num)
            .field("execution_state_root", &self.payload.header.state_root)
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
        epoch: Epoch,
        round: Round,
        payload: &Payload,
        qc: &QuorumCertificate<SCT>,
    ) -> Self {
        Self {
            author,
            epoch,
            round,
            payload: payload.clone(),
            qc: qc.clone(),
            id: {
                let mut _block_hash_span = tracing::trace_span!("block_hash_span").entered();
                let mut state = HasherType::new();
                author.hash(&mut state);
                state.update(epoch.as_bytes());
                state.update(round.as_bytes());
                payload.hash(&mut state);
                state.update(qc.get_block_id().0.as_bytes());
                state.update(qc.get_hash().as_bytes());

                BlockId(state.hash())
            },
        }
    }
}

impl<SCT: SignatureCollection> BlockType<SCT> for Block<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;
    type TxnHash = ();

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn get_round(&self) -> Round {
        self.round
    }

    fn get_epoch(&self) -> Epoch {
        self.epoch
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

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        vec![]
    }

    fn is_txn_list_empty(&self) -> bool {
        self.payload.txns == FullTransactionList::empty()
    }

    fn get_txn_list_len(&self) -> usize {
        self.payload.txns.bytes().len()
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.qc
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        self
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        self
    }
}

/// Trait that represents how inner contents of a block should be validated
pub trait BlockPolicy<SCT: SignatureCollection> {
    type ValidatedBlock: Sized
        + Clone
        + PartialEq
        + Eq
        + std::fmt::Debug
        + BlockType<SCT, NodeIdPubKey = SCT::NodeIdPubKey>
        + Hashable
        + Send;
}

/// A block policy which does not validate the inner contents of the block
pub struct PassthruBlockPolicy;
impl<SCT: SignatureCollection> BlockPolicy<SCT> for PassthruBlockPolicy {
    type ValidatedBlock = Block<SCT>;
}
