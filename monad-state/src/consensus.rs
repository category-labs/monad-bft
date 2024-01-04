use std::marker::PhantomData;

use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::RequestBlockSyncMessage},
    validation::signing::Validated,
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusProcess,
};
use monad_consensus_types::{
    block::Block, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand, LedgerCommand, MonadEvent,
    RouterCommand, StateRootHashCommand, TimerCommand,
};
use monad_types::{NodeId, RouterTarget, TimeoutVariant};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection, validator_set::ValidatorSetType,
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};

pub(super) struct ConsensusChildState<'a, CP, ST, SCT, VT, LT, TT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType,
{
    consensus: &'a mut CP,

    /// Consensus needs these states to process messages
    epoch_manager: &'a mut EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VT, SCT>,
    leader_election: &'a LT,
    txpool: &'a mut TT,

    _phantom: PhantomData<ST>,
}

impl<'a, CP, ST, SCT, VT, LT, TT> ConsensusChildState<'a, CP, ST, SCT, VT, LT, TT>
where
    CP: ConsensusProcess<ST, SCT>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    TT: TxPool,
{
    pub(super) fn new(monad_state: &'a mut MonadState<CP, ST, SCT, VT, LT, TT>) -> Self {
        Self {
            consensus: &mut monad_state.consensus,
            epoch_manager: &mut monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,
            leader_election: &monad_state.leader_election,
            txpool: &mut monad_state.txpool,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: ConsensusEvent<ST, SCT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT>> {
        let consensus_cmds = match event {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => {
                let verified_message = match unverified_message.verify(
                    self.epoch_manager,
                    self.val_epoch_map,
                    &sender,
                ) {
                    Ok(m) => m,
                    Err(e) => {
                        handle_validation_error(e);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                let (author, _, verified_message) = verified_message.destructure();
                // Validated message according to consensus protocol spec
                let validated_mesage =
                    match verified_message.validate(self.epoch_manager, self.val_epoch_map) {
                        Ok(m) => m.into_inner(),
                        Err(e) => {
                            handle_validation_error(e);
                            // TODO-2: collect evidence
                            let evidence_cmds = vec![];
                            return evidence_cmds;
                        }
                    };

                match validated_mesage {
                    ConsensusMessage::Proposal(msg) => self.consensus.handle_proposal_message(
                        author,
                        msg,
                        self.epoch_manager,
                        self.val_epoch_map,
                        self.leader_election,
                    ),
                    ConsensusMessage::Vote(msg) => self.consensus.handle_vote_message(
                        author,
                        msg,
                        self.txpool,
                        self.epoch_manager,
                        self.val_epoch_map,
                        self.leader_election,
                    ),
                    ConsensusMessage::Timeout(msg) => self.consensus.handle_timeout_message(
                        author,
                        msg,
                        self.txpool,
                        self.epoch_manager,
                        self.val_epoch_map,
                        self.leader_election,
                    ),
                }
            }
            ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                TimeoutVariant::Pacemaker => self
                    .consensus
                    .handle_timeout_expiry()
                    .into_iter()
                    .map(|cmd| {
                        ConsensusCommand::from_pacemaker_command(
                            self.consensus.get_keypair(),
                            self.consensus.get_cert_keypair(),
                            cmd,
                        )
                    })
                    .collect(),
                TimeoutVariant::BlockSync(bid) => {
                    let current_epoch = self
                        .epoch_manager
                        .get_epoch(self.consensus.get_current_round());
                    let val_set = self
                        .val_epoch_map
                        .get_val_set(&current_epoch)
                        .expect("current validator set should be in the map");
                    self.consensus.handle_block_sync_tmo(bid, val_set)
                }
            },

            ConsensusEvent::StateUpdate((seq_num, root_hash)) => {
                self.consensus.handle_state_root_update(seq_num, root_hash);
                Vec::new()
            }

            ConsensusEvent::BlockSyncResponse {
                sender,
                unvalidated_response,
            } => {
                let validated_response =
                    match unvalidated_response.validate(self.epoch_manager, self.val_epoch_map) {
                        Ok(req) => req,
                        Err(e) => {
                            handle_validation_error(e);
                            // TODO-2: collect evidence
                            let evidence_cmds = vec![];
                            return evidence_cmds;
                        }
                    }
                    .into_inner();
                let current_epoch = self
                    .epoch_manager
                    .get_epoch(self.consensus.get_current_round());
                let val_set = self
                    .val_epoch_map
                    .get_val_set(&current_epoch)
                    .expect("current validator set should be in the map");
                self.consensus
                    .handle_block_sync(NodeId::new(sender), validated_response, val_set)
            }
        };
        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand(cmd))
            .collect::<Vec<_>>()
    }
}

pub(super) struct WrappedConsensusCommand<ST, SCT>(ConsensusCommand<ST, SCT>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>;

impl<ST, SCT> From<WrappedConsensusCommand<ST, SCT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT>,
            VerifiedMonadMessage<ST, SCT>,
            Block<SCT>,
            Checkpoint<SCT>,
            SCT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(cmd: WrappedConsensusCommand<ST, SCT>) -> Self {
        let mut parent_cmds: Vec<Command<_, _, _, _, _>> = Vec::new();

        match cmd.0 {
            ConsensusCommand::Publish { target, message } => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::Publish {
                    target,
                    message: VerifiedMonadMessage::Consensus(message),
                }))
            }
            ConsensusCommand::Schedule {
                duration,
                on_timeout,
            } => parent_cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                duration,
                variant: on_timeout,
                on_timeout: MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(on_timeout)),
            })),
            ConsensusCommand::ScheduleReset(on_timeout) => parent_cmds.push(Command::TimerCommand(
                TimerCommand::ScheduleReset(on_timeout),
            )),
            ConsensusCommand::RequestSync { peer, block_id } => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::PointToPoint(peer),
                    message: VerifiedMonadMessage::BlockSyncRequest(Validated::new(
                        RequestBlockSyncMessage { block_id },
                    )),
                }));
            }
            ConsensusCommand::LedgerCommit(block) => {
                parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                    block.clone(),
                )));
                parent_cmds.push(Command::ExecutionLedgerCommand(
                    ExecutionLedgerCommand::LedgerCommit(block),
                ))
            }
            ConsensusCommand::CheckpointSave(checkpoint) => parent_cmds.push(
                Command::CheckpointCommand(CheckpointCommand::Save(checkpoint)),
            ),
            ConsensusCommand::StateRootHash(full_block) => parent_cmds.push(
                Command::StateRootHashCommand(StateRootHashCommand::LedgerCommit(full_block)),
            ),
        }
        parent_cmds
    }
}
