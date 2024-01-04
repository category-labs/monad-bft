use std::time::Duration;

use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::TimeoutMessage},
    pacemaker::PacemakerCommand,
    validation::signing::{Validated, Verified},
    vote_state::VoteStateCommand,
};
use monad_consensus_types::{
    block::Block,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, NodeId, RouterTarget, TimeoutVariant};

/// Command type that the consensus state-machine outputs
/// This is converted to a monad-executor-glue::Command at the top-level monad-state
pub enum ConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
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
    /// Fetch the state root hash for the given block
    StateRootHash(Block<SCT>),
    // TODO-2 add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

impl<ST, SCT> ConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn from_pacemaker_command(
        keypair: &ST::KeyPairType,
        cert_keypair: &SignatureCollectionKeyPairType<SCT>,
        cmd: PacemakerCommand<SCT>,
    ) -> Self {
        match cmd {
            PacemakerCommand::PrepareTimeout(tmo) => ConsensusCommand::Publish {
                target: RouterTarget::Broadcast,
                message: ConsensusMessage::Timeout(TimeoutMessage::new(tmo, cert_keypair))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint<SCT: SignatureCollection> {
    block: Block<SCT>,
    epoch: Epoch,
}
