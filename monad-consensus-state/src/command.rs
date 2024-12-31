use std::time::Duration;

use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::TimeoutMessage,
    },
    pacemaker::PacemakerCommand,
    validation::signing::{Validated, Verified},
    vote_state::VoteStateCommand,
};
use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader, ExecutionProtocol},
    checkpoint::Checkpoint,
    ledger::OptimisticCommit,
    quorum_certificate::{QuorumCertificate, TimestampAdjustment},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{Epoch, Round, RouterTarget, SeqNum};

/// Command type that the consensus state-machine outputs
/// This is converted to a monad-executor-glue::Command at the top-level monad-state
#[derive(Debug)]
pub enum ConsensusCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    EnterRound(Epoch, Round),
    /// Attempt to send a message to RouterTarget
    /// Delivery is NOT guaranteed, retry must be handled at the state-machine level
    Publish {
        target: RouterTarget<SCT::NodeIdPubKey>,
        message: Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>,
    },
    /// Schedule a timeout event to be emitted in `duration`
    Schedule {
        duration: Duration,
    },
    /// Cancel scheduled (if exists) timeout event
    ScheduleReset,
    /// Commit blocks to ledger
    LedgerCommit(OptimisticCommit<ST, SCT, EPT>),
    /// Requests BlockSync
    /// Serviced by block_sync in MonadState
    RequestSync(BlockRange),
    /// Cancels BlockSync request
    CancelSync(BlockRange),
    /// Too far behind, request StateSync with:
    /// 1. New blocktree root
    /// 2. New high_qc
    ///
    /// TODO we can include blocktree cache if we want
    RequestStateSync {
        root: ConsensusBlockHeader<ST, SCT, EPT>,
        high_qc: QuorumCertificate<SCT>,
    },
    /// Can only be called *once*
    StartExecution,
    /// Checkpoints periodically can upload/backup the ledger and garbage collect persisted events
    /// if necessary
    CheckpointSave {
        root_seq_num: SeqNum,
        high_qc_round: Round,
        checkpoint: Checkpoint<SCT>,
    },
    // TODO-2 add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
    /// Issue to clear mempool
    ClearMempool,
    TimestampUpdate(TimestampAdjustment),
    ScheduleVote {
        duration: Duration,
        round: Round,
    },
}

impl<ST, SCT, EPT> ConsensusCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn from_pacemaker_command(
        keypair: &ST::KeyPairType,
        cert_keypair: &SignatureCollectionKeyPairType<SCT>,
        version: u32,
        cmd: PacemakerCommand<SCT>,
    ) -> Self {
        match cmd {
            PacemakerCommand::EnterRound((epoch, round)) => {
                ConsensusCommand::EnterRound(epoch, round)
            }
            PacemakerCommand::PrepareTimeout(tmo) => ConsensusCommand::Publish {
                // TODO should this be sent to epoch of next round?
                target: RouterTarget::Broadcast(tmo.tminfo.epoch),
                message: ConsensusMessage {
                    version,
                    message: ProtocolMessage::Timeout(TimeoutMessage::new(tmo, cert_keypair)),
                }
                .sign(keypair),
            },
            PacemakerCommand::Schedule { duration } => ConsensusCommand::Schedule { duration },
            PacemakerCommand::ScheduleReset => ConsensusCommand::ScheduleReset,
        }
    }
}

impl<ST, SCT, EPT> From<VoteStateCommand> for ConsensusCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: VoteStateCommand) -> Self {
        //TODO-3 VoteStateCommand used for evidence collection
        todo!()
    }
}
