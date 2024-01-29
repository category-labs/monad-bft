use std::marker::PhantomData;

use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{CascadeTxMessage, RequestBlockSyncMessage},
    },
    validation::signing::Validated,
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusState,
};
use monad_consensus_types::{
    block::Block, block_validator::BlockValidator, metrics::Metrics, payload::StateRootValidator,
    signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand, LedgerCommand, MonadEvent,
    RouterCommand, StateRootHashCommand, TimerCommand,
};
use monad_tracing_counter::inc_count;
use monad_types::{NodeId, RouterTarget, TimeoutVariant};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection,
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};

pub(super) struct ConsensusChildState<'a, ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    consensus: &'a mut ConsensusState<ST, SCT, BVT, SVT>,

    /// Consensus needs these states to process messages
    epoch_manager: &'a mut EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    leader_election: &'a LT,
    txpool: &'a mut TT,
    metrics: &'a mut Metrics,

    _phantom: PhantomData<(ST, ASVT)>,
}

impl<'a, ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
    ConsensusChildState<'a, ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            consensus: &mut monad_state.consensus,
            epoch_manager: &mut monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,
            leader_election: &monad_state.leader_election,
            txpool: &mut monad_state.txpool,
            metrics: &mut monad_state.metrics,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: ConsensusEvent<ST, SCT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT>> {
        let vec = match event {
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
                        handle_validation_error(e, self.metrics);
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
                            handle_validation_error(e, self.metrics);
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
                        self.metrics,
                    ),
                    ConsensusMessage::Vote(msg) => self.consensus.handle_vote_message(
                        author,
                        msg,
                        self.txpool,
                        self.epoch_manager,
                        self.val_epoch_map,
                        self.leader_election,
                        self.metrics,
                    ),
                    ConsensusMessage::Timeout(msg) => self.consensus.handle_timeout_message(
                        author,
                        msg,
                        self.txpool,
                        self.epoch_manager,
                        self.val_epoch_map,
                        self.leader_election,
                        self.metrics,
                    ),
                }
            }
            ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                TimeoutVariant::Pacemaker => self
                    .consensus
                    .handle_timeout_expiry(self.metrics)
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
                    self.consensus
                        .handle_block_sync_tmo(bid, val_set, self.metrics)
                }
            },

            ConsensusEvent::StateUpdate(info) => {
                inc_count!(state_root_update);
                self.consensus
                    .handle_state_root_update(info.seq_num, info.state_root_hash);
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
                            handle_validation_error(e, self.metrics);
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
                self.consensus.handle_block_sync(
                    NodeId::new(sender),
                    validated_response,
                    val_set,
                    self.metrics,
                )
            }
        };
        let consensus_cmds = vec;
        consensus_cmds
            .into_iter()
            .map(WrappedConsensusCommand)
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
            ConsensusCommand::CascadeTxns { peer, txns } => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::PointToPoint(peer),
                    message: VerifiedMonadMessage::CascadeTxns(Validated::new(CascadeTxMessage {
                        txns,
                    })),
                }))
            }
        }
        parent_cmds
    }
}
