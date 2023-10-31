use std::time::Duration;

use monad_consensus::{
    messages::consensus_message::ConsensusMessage, pacemaker::PacemakerCommand,
    vote_state::VoteStateCommand,
};
use monad_consensus_types::{
    block::{Block, FullBlock, UnverifiedFullBlock},
    command::{FetchFullTxParams, FetchTxParams, FetchedBlock},
    payload::TransactionHashList,
    signature_collection::SignatureCollection,
};
use monad_types::{BlockId, Epoch, NodeId, RouterTarget, TimeoutVariant};

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
    FetchTxs(usize, Vec<TransactionHashList>, FetchTxParams<SCT>),
    FetchTxsReset,
    FetchFullTxs(TransactionHashList, FetchFullTxParams<SCT>),
    FetchFullTxsReset,
    DrainTxs(Vec<TransactionHashList>),
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
    // TODO-2 add command for updating validator_set/round
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

impl<SCT: SignatureCollection> From<VoteStateCommand> for ConsensusCommand<SCT> {
    fn from(value: VoteStateCommand) -> Self {
        //TODO-3 VoteStateCommand used for evidence collection
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint<SCT> {
    block: Block<SCT>,
    epoch: Epoch,
}
