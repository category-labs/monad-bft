use std::time::Duration;

use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncMessage, ProposalMessage},
    },
    pacemaker::{PacemakerCommand, PacemakerTimerExpire},
    vote_state::VoteStateCommand,
};
use monad_consensus_types::{
    block::{Block, FullBlock, UnverifiedFullBlock},
    payload::{FullTransactionList, TransactionList},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    timeout::TimeoutCertificate,
};
use monad_executor::{PeerId, RouterTarget};
use monad_types::{BlockId, Epoch, Hash, NodeId, Round};

use crate::blocksync::InFlightBlockSync;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum TimeoutEvent {
    Pacemaker,
    BlockSyncTimerExpire(BlockId),
}
pub enum ConsensusCommand<SCT: SignatureCollection> {
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<SCT>,
    },
    Schedule {
        duration: Duration,
        on_timeout: TimeoutEvent,
    },
    ScheduleReset(TimeoutEvent),
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
    DrainTxs(Vec<TransactionList>),
    LedgerCommit(Vec<FullBlock<SCT>>),
    RequestSync {
        peer: NodeId,
        block_id: BlockId,
    },
    LedgerFetch(
        NodeId,
        BlockId,
        Box<dyn (FnOnce(Option<UnverifiedFullBlock<SCT>>) -> FetchedBlock<SCT>) + Send + Sync>,
    ),
    LedgerFetchReset(NodeId, BlockId),
    /// Checkpoints periodically can upload/backup the ledger and garbage clean
    /// persisted events if necessary
    CheckpointSave(Checkpoint<SCT>),
    StateRootHash(FullBlock<SCT>),
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
            PacemakerCommand::Schedule { duration } => ConsensusCommand::Schedule {
                duration,
                on_timeout: TimeoutEvent::Pacemaker,
            },
            PacemakerCommand::ScheduleReset => {
                ConsensusCommand::ScheduleReset(TimeoutEvent::Pacemaker)
            }
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

impl<SCT: SignatureCollection> From<VoteStateCommand> for ConsensusCommand<SCT> {
    fn from(value: VoteStateCommand) -> Self {
        todo!()
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

    // FetchedBlock results should only be used to send block data to peers
    // over the network so we should unverify it before sending to consensus
    // to prevent it from being used for anything else
    pub unverified_full_block: Option<UnverifiedFullBlock<SCT>>,
}

impl<SCT: SignatureCollection> From<FetchedBlock<SCT>> for ConsensusCommand<SCT> {
    fn from(fetched_b: FetchedBlock<SCT>) -> Self {
        let FetchedBlock {
            requester,
            block_id,
            unverified_full_block,
        } = fetched_b;

        ConsensusCommand::Publish {
            target: RouterTarget::PointToPoint(PeerId(requester.0)),
            message: ConsensusMessage::BlockSync(match unverified_full_block {
                Some(b) => BlockSyncMessage::BlockFound(b),
                None => BlockSyncMessage::NotAvailable(block_id),
            }),
        }
    }
}
