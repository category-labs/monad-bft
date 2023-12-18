use monad_crypto::hasher::Hash;
use monad_types::{BlockId, NodeId, Round, SeqNum};

use crate::{
    block::{Block, UnverifiedFullBlock},
    payload::TransactionHashList,
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
/// Criteria for choosing transactions during a FetchTxCommand
pub struct FetchTxsCriteria<SCT> {
    /// max number of txns to fetch
    pub max_txs: usize,
    /// limit on cumulative gas from transactions in a block
    pub block_gas_limit: u64,
    /// list of txns to avoid fetching (as they are already in pending blocks)
    pub ignore_txs: Vec<TransactionHashList>,
    /// params of the proposal of this fetch
    pub proposal_params: FetchTxParams<SCT>,
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
