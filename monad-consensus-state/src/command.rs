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
    block::Block,
    checkpoint::Checkpoint,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, NodeId, Round, RouterTarget, TimeoutVariant};

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
        on_timeout: TimeoutVariant,
    },
    /// Cancel scheduled (if exists) timeout event
    ScheduleReset(TimeoutVariant),
    /// Commit blocks to ledger
    LedgerCommit(Vec<Block<SCT>>),
    /// Requests BlockSync from given peer
    /// Gets converted to a RouterCommand::Publish
    /// Delivery is NOT guaranteed, retry must be handled at the state-machine level
    RequestSync {
        peer: NodeId<SCT::NodeIdPubKey>,
        block_id: BlockId,
    },
    /// Checkpoints periodically can upload/backup the ledger and garbage collect persisted events
    /// if necessary
    CheckpointSave(Checkpoint<SCT>),
    // TODO-2 add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
    /// Issue to clear mempool
    ClearMempool,
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
                target: RouterTarget::Broadcast(tmo.tminfo.epoch, tmo.tminfo.round),
                message: ConsensusMessage {
                    version: version.into(),
                    message: ProtocolMessage::Timeout(TimeoutMessage::new(tmo, cert_keypair)),
                }
                .sign(keypair),
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
