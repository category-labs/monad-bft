use std::time::Duration;

use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::TimeoutMessage},
    pacemaker::PacemakerCommand,
    vote_state::VoteStateCommand,
};
use monad_consensus_types::{
    block::{Block, FullBlock, UnverifiedFullBlock},
    command::{FetchFullTxParams, FetchTxsCriteria, FetchedBlock},
    payload::TransactionHashList,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_types::{BlockId, Epoch, NodeId, RouterTarget, TimeoutVariant};

/// Command type that the consensus state-machine outputs
/// This is converted to a monad-executor-glue::Command at the top-level monad-state
pub enum ConsensusCommand<SCT: SignatureCollection> {
    /// Attempt to send a message to RouterTarget
    /// Delivery is NOT guaranteed, retry must be handled at the state-machine level
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<SCT>,
    },
    /// Schedule a timeout event to be emitted in `duration`
    Schedule {
        duration: Duration,
        on_timeout: TimeoutVariant,
    },
    /// Cancel scheduled (if exists) timeout event
    ScheduleReset(TimeoutVariant),
    /// Fetch transaction hashes for inclusion in proposal (as proposer)
    FetchTxs(FetchTxsCriteria<SCT>),
    /// Cancel any in-progress FetchTxs commands
    /// This is only necessary to not double-emit events on replay
    FetchTxsReset,
    /// Fetch full transactions from hash list
    FetchFullTxs(TransactionHashList, FetchFullTxParams<SCT>),
    /// Cancel any in-progress FetchFullTxs commands
    /// This is only necessary to not double-emit events on replay
    FetchFullTxsReset,
    /// Drain done transactions from the mempool given hash list
    DrainTxs(Vec<TransactionHashList>),
    /// Commit blocks to ledger
    LedgerCommit(Vec<FullBlock<SCT>>),
    /// Requests BlockSync from given peer
    /// Gets converted to a RouterCommand::Publish
    /// Delivery is NOT guaranteed, retry must be handled at the state-machine level
    RequestSync { peer: NodeId, block_id: BlockId },
    /// Fetch requested block from ledger
    LedgerFetch(
        NodeId,
        BlockId,
        Box<dyn (FnOnce(Option<UnverifiedFullBlock<SCT>>) -> FetchedBlock<SCT>) + Send + Sync>,
    ),
    /// Cancel any in-progress LedgerCommit commands
    /// This is only necessary to not double-emit events on replay
    LedgerFetchReset(NodeId, BlockId),
    /// Checkpoints periodically can upload/backup the ledger and garbage collect persisted events
    /// if necessary
    CheckpointSave(Checkpoint<SCT>),
    /// Fetch the state root hash for the given block
    StateRootHash(FullBlock<SCT>),
    // TODO-2 add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

impl<SCT: SignatureCollection> ConsensusCommand<SCT> {
    pub fn from_pacemaker_command(
        cert_keypair: &SignatureCollectionKeyPairType<SCT>,
        cmd: PacemakerCommand<SCT>,
    ) -> Self {
        match cmd {
            PacemakerCommand::PrepareTimeout(tmo) => ConsensusCommand::Publish {
                target: RouterTarget::Broadcast,
                message: ConsensusMessage::Timeout(TimeoutMessage::new(tmo, cert_keypair)),
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
