use std::marker::PhantomData;

use monad_consensus::{
    messages::{consensus_message::ProtocolMessage, message::RequestBlockSyncMessage},
    validation::signing::Validated,
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusStateWrapper,
};
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    block_validator::BlockValidator,
    payload::StateRootValidator,
    signature_collection::SignatureCollection,
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand, LedgerCommand,
    LoopbackCommand, MempoolEvent, MonadEvent, RouterCommand, StateRootHashCommand, TimerCommand,
};
use monad_types::{RouterTarget, SeqNum, TimeoutVariant};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetTypeFactory};

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};

pub(super) struct ConsensusChildState<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
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
    consensus: ConsensusStateWrapper<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT>,

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
            consensus: ConsensusStateWrapper {
                consensus: &mut monad_state.consensus,

                metrics: &mut monad_state.metrics,
                tx_pool: &mut monad_state.txpool,
                epoch_manager: &mut monad_state.epoch_manager,
                block_policy: &mut monad_state.block_policy,

                val_epoch_map: &monad_state.val_epoch_map,
                election: &monad_state.leader_election,
                version: monad_state.version.protocol_version,

                state_root_validator: &monad_state.state_root_validator,
                block_validator: &monad_state.block_validator,
                beneficiary: &monad_state.beneficiary,
                nodeid: &monad_state.nodeid,
                config: &monad_state.consensus_config,

                keypair: &monad_state.keypair,
                cert_keypair: &monad_state.cert_keypair,
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
                    self.consensus.epoch_manager,
                    self.consensus.val_epoch_map,
                    &sender.pubkey(),
                ) {
                    Ok(m) => m,
                    Err(e) => {
                        handle_validation_error(e, self.consensus.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                let (author, _, verified_message) = verified_message.destructure();

                // Validated message according to consensus protocol spec
                let validated_mesage = match verified_message.validate(
                    self.consensus.epoch_manager,
                    self.consensus.val_epoch_map,
                    self.consensus.version,
                ) {
                    Ok(m) => m.into_inner(),
                    Err(e) => {
                        handle_validation_error(e, self.consensus.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                match validated_mesage {
                    ProtocolMessage::Proposal(msg) => {
                        self.consensus.handle_proposal_message(author, msg)
                    }
                    ProtocolMessage::Vote(msg) => self.consensus.handle_vote_message(author, msg),
                    ProtocolMessage::Timeout(msg) => {
                        self.consensus.handle_timeout_message(author, msg)
                    }
                }
            }
            ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                TimeoutVariant::Pacemaker => self
                    .consensus
                    .handle_timeout_expiry()
                    .into_iter()
                    .map(|cmd| {
                        ConsensusCommand::from_pacemaker_command(
                            self.consensus.keypair,
                            self.consensus.cert_keypair,
                            self.consensus.version,
                            cmd,
                        )
                    })
                    .collect(),
                TimeoutVariant::BlockSync(bid) => self.consensus.handle_block_sync_tmo(bid),
            },

            ConsensusEvent::BlockSyncResponse {
                sender,
                unvalidated_response,
            } => {
                let validated_response = match unvalidated_response
                    .validate(self.consensus.epoch_manager, self.consensus.val_epoch_map)
                {
                    Ok(req) => req,
                    Err(e) => {
                        handle_validation_error(e, self.consensus.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                }
                .into_inner();
                self.consensus.handle_block_sync(sender, validated_response)
            }
        };
        let consensus_cmds = vec;
        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand {
                state_root_delay: self.consensus.state_root_validator.get_delay(),
                command: cmd,
            })
            .collect::<Vec<_>>()
    }
}

pub(super) struct WrappedConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    state_root_delay: SeqNum,
    command: ConsensusCommand<ST, SCT>,
}

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
    fn from(wrapped: WrappedConsensusCommand<ST, SCT>) -> Self {
        let mut parent_cmds: Vec<Command<_, _, _, _, _>> = Vec::new();

        match wrapped.command {
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
            ConsensusCommand::LedgerCommit(blocks) => {
                let last_block = blocks.iter().last().expect("LedgerCommit no blocks");
                parent_cmds.extend(blocks.iter().cloned().map(|block| {
                    Command::StateRootHashCommand(StateRootHashCommand::LedgerCommit(block))
                }));
                parent_cmds.push(Command::StateRootHashCommand(
                    // upon committing block N, we no longer need state_root_N-delay
                    // therefore, we cancel below state_root_N-delay+1
                    //
                    // we'll be left with (state_root_N-delay, state_root_N] queued up, which is
                    // exactly `delay` number of roots
                    StateRootHashCommand::CancelBelow(
                        (last_block.get_seq_num() + SeqNum(1)).max(wrapped.state_root_delay)
                            - wrapped.state_root_delay,
                    ),
                ));
                parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                    blocks.clone(),
                )));
                parent_cmds.push(Command::ExecutionLedgerCommand(
                    ExecutionLedgerCommand::LedgerCommit(blocks),
                ));
            }
            ConsensusCommand::CheckpointSave(checkpoint) => parent_cmds.push(
                Command::CheckpointCommand(CheckpointCommand::Save(checkpoint)),
            ),
            ConsensusCommand::ClearMempool => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::MempoolEvent(MempoolEvent::Clear),
                )));
            }
        }
        parent_cmds
    }
}
