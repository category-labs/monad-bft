use std::time::Duration;

use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncMessage, ProposalMessage},
    },
    pacemaker::PacemakerCommand,
    vote_state::VoteStateCommand,
};
use monad_consensus_types::{
    block::{Block, FullBlock, UnverifiedFullBlock},
    command::{FetchedBlock, FetchedFullTxs, FetchedTxs},
    payload::{FullTransactionList, TransactionList},
    signature_collection::SignatureCollection,
};
use monad_executor_glue::RouterTarget;
use monad_types::{BlockId, Epoch, Hash, NodeId, Round, TimeoutVariant};

use crate::blocksync::InFlightBlockSync;

pub enum ConsensusCommand<SCT: SignatureCollection> {
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<SCT>,
    },
    Schedule {
        duration: Duration,
        on_timeout: TimeoutVariant,
    },
    ScheduleReset(TimeoutVariant),
    FetchTxs(
        usize,
        Vec<TransactionList>,
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
                on_timeout: TimeoutVariant::Pacemaker,
            },
            PacemakerCommand::ScheduleReset => {
                ConsensusCommand::ScheduleReset(TimeoutVariant::Pacemaker)
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
