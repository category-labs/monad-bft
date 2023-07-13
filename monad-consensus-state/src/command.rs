use std::time::Duration;

use monad_consensus::{
    messages::consensus_message::ConsensusMessage,
    messages::message::ProposalMessage,
    pacemaker::{PacemakerCommand, PacemakerTimerExpire},
};
use monad_consensus_types::{
    block::{Block, TransactionList},
    quorum_certificate::QuorumCertificate,
    signature::SignatureCollection,
    timeout::TimeoutCertificate,
    transaction::TransactionCollection,
};
use monad_crypto::Signature;
use monad_executor::RouterTarget;
use monad_types::{BlockId, NodeId, Round};

pub enum ConsensusCommand<ST, SCT: SignatureCollection, TC: TransactionCollection> {
    Publish {
        target: RouterTarget,
        message: ConsensusMessage<ST, SCT>,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    ScheduleReset,
    FetchTxs(Box<dyn (FnOnce(Vec<u8>) -> FetchedTxs<ST, SCT>) + Send + Sync>),
    FetchTxsReset,
    FetchFullTxs(Box<dyn (FnOnce(TC) -> FetchedFullTxs<ST, SCT, TC>) + Send + Sync>),
    FetchFullTxsReset,
    LedgerCommit(Block<SCT>),
    RequestSync {
        blockid: BlockId,
    },
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

impl<S: Signature, SC: SignatureCollection, T: TransactionCollection> From<PacemakerCommand<S, SC>>
    for ConsensusCommand<S, SC, T>
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
pub struct FetchedFullTxs<ST, SCT, TC> {
    pub author: NodeId,
    pub p: ProposalMessage<ST, SCT>,
    pub txns: TC,
}
