use std::marker::PhantomData;

use monad_consensus::{
    messages::{consensus_message::ProtocolMessage, message::RequestBlockSyncMessage},
    validation::signing::Validated,
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusState, NodeState,
};
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    payload::StateRootValidator,
    signature_collection::SignatureCollection,
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand, LedgerCommand, MonadEvent,
    RouterCommand, StateRootHashCommand, TimerCommand,
};
use monad_types::{RouterTarget, TimeoutVariant};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetTypeFactory};

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};

pub(super) struct ConsensusChildState<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    consensus: &'a mut ConsensusState<ST, SCT, BPT, BVT, SVT>,
    node_state: NodeState<'a, ST, SCT, VTF, LT, TT>,

    _phantom: PhantomData<ASVT>,
}

impl<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
    ConsensusChildState<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT>,
    BVT: BlockValidator<SCT, BPT>,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            consensus: &mut monad_state.consensus,
            node_state: NodeState {
                epoch_manager: &mut monad_state.epoch_manager,
                val_epoch_map: &monad_state.val_epoch_map,
                election: &monad_state.leader_election,
                tx_pool: &mut monad_state.txpool,
                metrics: &mut monad_state.metrics,
                version: monad_state.version.protocol_version,
                _phantom: PhantomData,
            },
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
                    self.node_state.epoch_manager,
                    self.node_state.val_epoch_map,
                    &sender.pubkey(),
                ) {
                    Ok(m) => m,
                    Err(e) => {
                        handle_validation_error(e, self.node_state.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                let (author, _, verified_message) = verified_message.destructure();

                // Validated message according to consensus protocol spec
                let validated_mesage = match verified_message.validate(
                    self.node_state.epoch_manager,
                    self.node_state.val_epoch_map,
                    self.node_state.version,
                ) {
                    Ok(m) => m.into_inner(),
                    Err(e) => {
                        handle_validation_error(e, self.node_state.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                match validated_mesage {
                    ProtocolMessage::Proposal(msg) => {
                        self.consensus
                            .handle_proposal_message(author, msg, &mut self.node_state)
                    }
                    ProtocolMessage::Vote(msg) => {
                        self.consensus
                            .handle_vote_message(author, msg, &mut self.node_state)
                    }
                    ProtocolMessage::Timeout(msg) => {
                        self.consensus
                            .handle_timeout_message(author, msg, &mut self.node_state)
                    }
                }
            }
            ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                TimeoutVariant::Pacemaker => self
                    .consensus
                    .handle_timeout_expiry(self.node_state.epoch_manager, self.node_state.metrics)
                    .into_iter()
                    .map(|cmd| {
                        ConsensusCommand::from_pacemaker_command(
                            self.consensus.get_keypair(),
                            self.consensus.get_cert_keypair(),
                            self.node_state.version,
                            self.node_state.epoch_manager,
                            cmd,
                        )
                    })
                    .collect(),
                TimeoutVariant::BlockSync(bid) => {
                    let current_epoch = self
                        .node_state
                        .epoch_manager
                        .get_epoch(self.consensus.get_current_round());
                    let val_set = self
                        .node_state
                        .val_epoch_map
                        .get_val_set(&current_epoch)
                        .expect("current validator set should be in the map");
                    self.consensus
                        .handle_block_sync_tmo(bid, val_set, self.node_state.metrics)
                }
            },

            ConsensusEvent::StateUpdate(info) => {
                self.node_state.metrics.consensus_events.state_root_update += 1;
                self.consensus
                    .handle_state_root_update(info.seq_num, info.state_root_hash);
                Vec::new()
            }

            ConsensusEvent::BlockSyncResponse {
                sender,
                unvalidated_response,
            } => {
                let validated_response = match unvalidated_response
                    .validate(self.node_state.epoch_manager, self.node_state.val_epoch_map)
                {
                    Ok(req) => req,
                    Err(e) => {
                        handle_validation_error(e, self.node_state.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                }
                .into_inner();
                self.consensus
                    .handle_block_sync(sender, validated_response, &mut self.node_state)
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
            ConsensusCommand::EnterRound(epoch, round) => parent_cmds.push(Command::RouterCommand(
                RouterCommand::UpdateCurrentRound(epoch, round),
            )),
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
