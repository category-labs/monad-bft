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
use monad_types::{BlockId, Epoch, Hash, NodeId, Round};

use crate::manager::InFlightBlockSync;

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
    FetchTxs(
        usize,
        Box<dyn (FnOnce(TransactionList) -> FetchedTxs<ST, SCT>) + Send + Sync>,
    ),
    FetchTxsReset,
    FetchFullTxs(
        TransactionList,
        Box<dyn (FnOnce(Option<FullTransactionList>) -> FetchedFullTxs<ST, SCT>) + Send + Sync>,
    ),
    FetchFullTxsReset,
    LedgerCommit(Vec<Block<SCT>>),
    RequestSync {
        blockid: BlockId,
    },
    LedgerFetch(
        BlockId,
        Box<dyn (FnOnce(Option<Block<SCT>>) -> FetchedBlock<SCT>) + Send + Sync>,
    ),
    LedgerFetchReset,
    /// Checkpoints periodically can upload/backup the ledger and garbage clean
    /// persisted events if necessary
    CheckpointSave(Checkpoint<SCT>),
    StateRootHash(Block<SCT>),
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

impl<S: MessageSignature, SCT: SignatureCollection> From<&InFlightBlockSync<SCT>>
    for ConsensusCommand<S, SCT>
{
    fn from(cmd: &InFlightBlockSync<SCT>) -> Self {
        ConsensusCommand::RequestSync {
            blockid: cmd.qc.info.vote.id,
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
    pub seq_num: u64,
    pub state_root_hash: Hash,
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
    pub requester: NodeId,
    pub block_id: BlockId,
    pub block: Option<Block<SCT>>,
}
