use std::time::Duration;

use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::ProposalMessage},
    pacemaker::{PacemakerCommand, PacemakerTimerExpire},
};
use monad_consensus_types::{
    block::Block,
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionList},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    timeout::TimeoutCertificate,
};
use monad_executor::RouterTarget;
use monad_types::{BlockId, Epoch, NodeId, Round};

pub enum ConsensusCommand<ST, SCT: SignatureCollection> {
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<ST, SCT>,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    ScheduleReset,
    FetchTxs(Box<dyn (FnOnce(TransactionList) -> FetchedTxs<ST, SCT>) + Send + Sync>),
    FetchTxsReset,
    FetchFullTxs(
        TransactionList,
        Box<dyn (FnOnce(Option<FullTransactionList>) -> FetchedFullTxs<ST, SCT>) + Send + Sync>,
    ),
    FetchFullTxsReset,
    LedgerCommit(Block<SCT>),
    RequestSync {
        blockid: BlockId,
    },
    LedgerFetch(
        BlockId,
        Box<dyn (FnOnce(Option<Block<SCT>>) -> FetchedBlock<SCT>) + Send + Sync>,
    ),
    /// Checkpoints periodically can upload/backup the ledger and garbage clean
    /// persisted events if necessary
    CheckpointSave(Checkpoint<SCT>),
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

impl<S: MessageSignature, SC: SignatureCollection> From<PacemakerCommand<S, SC>>
    for ConsensusCommand<S, SC>
{
    fn from(cmd: PacemakerCommand<S, SC>) -> Self {
        match cmd {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint<SCT> {
    block: Block<SCT>,
    epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedTxs<ST, SCT> {
    // some of this stuff is probably not strictly necessary
    // they're included here just to be extra safe
    pub node_id: NodeId,
    pub round: Round,
    pub high_qc: QuorumCertificate<SCT>,
    pub last_round_tc: Option<TimeoutCertificate<ST>>,

    pub txns: TransactionList,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedFullTxs<ST, SCT> {
    pub author: NodeId,
    pub p: ProposalMessage<ST, SCT>,
    pub txns: Option<FullTransactionList>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBlock<SCT> {
    pub block: Option<Block<SCT>>,
}
