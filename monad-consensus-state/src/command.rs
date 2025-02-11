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
    block::{BlockPolicy, BlockRange, ConsensusBlockHeader, OptimisticPolicyCommit},
    checkpoint::Checkpoint,
    payload::RoundSignature,
    quorum_certificate::{QuorumCertificate, TimestampAdjustment},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, ExecutionProtocol, Round, RouterTarget, SeqNum};

/// Command type that the consensus state-machine outputs
/// This is converted to a monad-executor-glue::Command at the top-level monad-state
#[derive(Debug)]
pub enum ConsensusCommand<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
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
    /// Creates a proposal
    CreateProposal {
        epoch: Epoch,
        round: Round,
        seq_num: SeqNum,
        high_qc: QuorumCertificate<SCT>,
        round_signature: RoundSignature<SCT::SignatureType>,
        last_round_tc: Option<TimeoutCertificate<SCT>>,

        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        beneficiary: [u8; 20],
        timestamp_ns: u128,

        extending_blocks: Vec<BPT::ValidatedBlock>,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
    },
    /// Commit blocks to ledger
    CommitBlocks(OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT>),
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
    /// Checkpoints periodically can upload/backup the ledger and garbage collect persisted events
    /// if necessary
    CheckpointSave {
        root_seq_num: SeqNum,
        high_qc_round: Round,
        checkpoint: Checkpoint<SCT>,
    },
    // TODO-2 add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
    TimestampUpdate(TimestampAdjustment),
    ScheduleVote {
        duration: Duration,
        round: Round,
    },
}

impl<ST, SCT, EPT, BPT, SBT> ConsensusCommand<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
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

impl<ST, SCT, EPT, BPT, SBT> From<VoteStateCommand> for ConsensusCommand<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn from(value: VoteStateCommand) -> Self {
        //TODO-3 VoteStateCommand used for evidence collection
        todo!()
    }
}
