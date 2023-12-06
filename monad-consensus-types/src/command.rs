use monad_crypto::hasher::Hash;
use monad_types::{BlockId, NodeId, Round, SeqNum};

use crate::{
    block::{Block, UnverifiedFullBlock},
    quorum_certificate::QuorumCertificate,
    timeout::TimeoutCertificate,
};

/// Parameters that accompany a FetchTxCommand for creating a proposal
/// the parameters come from consensus state when the proposal is
/// being created
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTxParams<SCT> {
    // some of this stuff is probably not strictly necessary
    // they're included here just to be extra safe
    pub node_id: NodeId,
    pub round: Round,
    pub seq_num: SeqNum,
    pub state_root_hash: Hash,
    pub high_qc: QuorumCertificate<SCT>,
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,
}

/// Parameters that accompany a FetchFullTxParams when handling an
/// incoming proposal.
/// the parameters come from the proposal
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchFullTxParams<SCT> {
    pub author: NodeId,
    pub p_block: Block<SCT>,
    pub p_last_round_tc: Option<TimeoutCertificate<SCT>>,
}

/// FetchedBlock is used to respond to BlockSync requests from other
/// nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBlock<SCT> {
    /// The node that requested this block
    pub requester: NodeId,

    /// id of the requested block
    pub block_id: BlockId,

    /// FetchedBlock results should only be used to send block data to nodes
    /// over the network so we should unverify it before sending to consensus
    /// to prevent it from being used for anything else
    pub unverified_full_block: Option<UnverifiedFullBlock<SCT>>,
}
