use std::time::Duration;

use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncMessage, ProposalMessage},
    },
    pacemaker::{PacemakerCommand, PacemakerTimerExpire},
};
use monad_consensus_types::{
    block::Block,
    payload::{FullTransactionList, TransactionList},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    timeout::TimeoutCertificate,
};
use monad_executor::{PeerId, RouterTarget};
use monad_types::{BlockId, Epoch, Hash, NodeId, Round};

use crate::blocksync::InFlightBlockSync;

pub enum ConsensusCommand<SCT: SignatureCollection> {
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<SCT>,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    ScheduleReset,
    FetchTxs(
        usize,
        Box<dyn (FnOnce(TransactionList) -> FetchedTxs<SCT>) + Send + Sync>,
    ),
    FetchTxsReset,
    FetchFullTxs(
        TransactionList,
        Box<dyn (FnOnce(Option<FullTransactionList>) -> FetchedFullTxs<SCT>) + Send + Sync>,
    ),
    FetchFullTxsReset,
    LedgerCommit(Vec<Block<SCT>>),
    RequestSync {
        peer: NodeId,
        block_id: BlockId,
    },
    LedgerFetch(
        NodeId,
        BlockId,
        Box<dyn (FnOnce(Option<Block<SCT>>) -> FetchedBlock<SCT>) + Send + Sync>,
    ),
    LedgerFetchReset(NodeId, BlockId),
    /// Checkpoints periodically can upload/backup the ledger and garbage clean
    /// persisted events if necessary
    CheckpointSave(Checkpoint<SCT>),
    StateRootHash(Block<SCT>),
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

impl<SCT: SignatureCollection> From<PacemakerCommand<SCT>> for ConsensusCommand<SCT> {
    fn from(cmd: PacemakerCommand<SCT>) -> Self {
        match cmd {
            PacemakerCommand::PrepareTimeout(_) => unreachable!(),
            PacemakerCommand::Broadcast(message) => ConsensusCommand::Publish {
                target: RouterTarget::Broadcast,
                message: ConsensusMessage::Timeout(message),
            },
            PacemakerCommand::Schedule {
                duration,
                on_timeout,
            } => ConsensusCommand::Schedule {
                duration,
                on_timeout,
            },
            PacemakerCommand::ScheduleReset => ConsensusCommand::ScheduleReset,
        }
    }
}

impl<SCT: SignatureCollection> From<&InFlightBlockSync<SCT>> for ConsensusCommand<SCT> {
    fn from(sync: &InFlightBlockSync<SCT>) -> Self {
        ConsensusCommand::RequestSync {
            peer: sync.req_target,
            block_id: sync.qc.info.vote.id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint<SCT> {
    block: Block<SCT>,
    epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedTxs<SCT> {
    // some of this stuff is probably not strictly necessary
    // they're included here just to be extra safe
    pub node_id: NodeId,
    pub round: Round,
    pub seq_num: u64,
    pub state_root_hash: Hash,
    pub high_qc: QuorumCertificate<SCT>,
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,

    pub txns: TransactionList,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedFullTxs<SCT> {
    pub author: NodeId,
    pub p: ProposalMessage<SCT>,
    pub txns: Option<FullTransactionList>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBlock<SCT> {
    pub requester: NodeId,
    pub block_id: BlockId,
    pub block: Option<Block<SCT>>,
}

impl<SCT: SignatureCollection> From<FetchedBlock<SCT>> for ConsensusCommand<SCT> {
    fn from(fetched_b: FetchedBlock<SCT>) -> Self {
        let FetchedBlock {
            requester,
            block_id,
            block,
        } = fetched_b;
        let pid = PeerId(requester.0);
        ConsensusCommand::Publish {
            target: RouterTarget::PointToPoint(pid),
            message: ConsensusMessage::BlockSync(match block {
                Some(b) => BlockSyncMessage::BlockFound(b),
                None => BlockSyncMessage::NotAvailable(block_id),
            }),
        }
    }
}
