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
    block::{BlockIdRange, FullBlock},
    checkpoint::{Checkpoint, RootInfo},
    quorum_certificate::{QuorumCertificate, TimestampAdjustment},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, Round, RouterTarget};

/// Command type that the consensus state-machine outputs
/// This is converted to a monad-executor-glue::Command at the top-level monad-state
#[derive(Debug)]
pub enum ConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    EnterRound(Epoch, Round),
    /// Attempt to send a message to RouterTarget
    /// Delivery is NOT guaranteed, retry must be handled at the state-machine level
    Publish {
        target: RouterTarget<SCT::NodeIdPubKey>,
        message: Verified<ST, Validated<ConsensusMessage<SCT>>>,
    },
    /// Schedule a timeout event to be emitted in `duration`
    Schedule {
        duration: Duration,
    },
    /// Cancel scheduled (if exists) timeout event
    ScheduleReset,
    /// Commit blocks to ledger
    LedgerCommit(Vec<FullBlock<SCT>>),
    /// Requests a range of blocks through BlockSync
    /// Serviced by block_sync in MonadState
    RequestRangeSync {
        block_id_range: BlockIdRange,
    },
    /// Cancels BlockSync request
    CancelRangeSync {
        block_id_range: BlockIdRange,
    },
    /// Too far behind, request StateSync with:
    /// 1. New blocktree root
    /// 2. New high_qc
    ///
    /// TODO we can include blocktree cache if we want
    RequestStateSync {
        root: RootInfo,
        high_qc: QuorumCertificate<SCT>,
    },
    /// Can only be called *once*
    StartExecution,
    /// Checkpoints periodically can upload/backup the ledger and garbage collect persisted events
    /// if necessary
    CheckpointSave(Checkpoint<SCT>),
    // TODO-2 add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
    /// Issue to clear mempool
    ClearMempool,
    TimestampUpdate(TimestampAdjustment),
}

impl<ST, SCT> ConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn from_pacemaker_command(
        keypair: &ST::KeyPairType,
        cert_keypair: &SignatureCollectionKeyPairType<SCT>,
        version: &str,
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
                    version: version.into(),
                    message: ProtocolMessage::Timeout(TimeoutMessage::new(tmo, cert_keypair)),
                }
                .sign(keypair),
            },
            PacemakerCommand::Schedule { duration } => ConsensusCommand::Schedule { duration },
            PacemakerCommand::ScheduleReset => ConsensusCommand::ScheduleReset,
        }
    }
}

impl<ST, SCT> From<VoteStateCommand> for ConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: VoteStateCommand) -> Self {
        //TODO-3 VoteStateCommand used for evidence collection
        todo!()
    }
}
