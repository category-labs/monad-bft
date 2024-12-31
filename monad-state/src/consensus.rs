use std::marker::PhantomData;

use monad_blocksync::blocksync::BlockSyncSelfRequester;
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::ProposalMessage,
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_state::{command::ConsensusCommand, ConsensusConfig, ConsensusStateWrapper};
use monad_consensus_types::{
    block::{BlockPolicy, ExecutionProtocol, ExecutionResult},
    block_validator::BlockValidator,
    ledger::OptimisticCommit,
    metrics::Metrics,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    BlockSyncEvent, CheckpointCommand, Command, ConsensusEvent, LedgerCommand, LoopbackCommand,
    MempoolEvent, MonadEvent, RouterCommand, StateRootHashCommand, StateSyncCommand,
    StateSyncEvent, TimeoutVariant, TimerCommand, TimestampCommand,
};
use monad_state_backend::StateBackend;
use monad_types::{NodeId, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection,
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};
use tracing::info;

use crate::{
    handle_validation_error, BlockTimestamp, ConsensusMode, MonadState, MonadVersion,
    VerifiedMonadMessage,
};

pub(super) struct ConsensusChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, TT, BVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<ST, SCT, EPT, BPT, SBT>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
{
    consensus: &'a mut ConsensusMode<ST, SCT, EPT, BPT, SBT>,

    metrics: &'a mut Metrics,
    txpool: &'a mut TT,
    epoch_manager: &'a mut EpochManager,
    block_policy: &'a mut BPT,
    state_backend: &'a SBT,

    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    leader_election: &'a LT,
    version: &'a MonadVersion,

    block_timestamp: &'a BlockTimestamp,
    block_validator: &'a BVT,
    beneficiary: &'a EthAddress,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,
    consensus_config: &'a ConsensusConfig,

    keypair: &'a ST::KeyPairType,
    cert_keypair: &'a SignatureCollectionKeyPairType<SCT>,
}

impl<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, TT, BVT>
    ConsensusChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, TT, BVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<ST, SCT, EPT, BPT, SBT>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, TT, BVT>,
    ) -> Self {
        Self {
            consensus: &mut monad_state.consensus,

            metrics: &mut monad_state.metrics,
            txpool: &mut monad_state.txpool,
            epoch_manager: &mut monad_state.epoch_manager,
            block_policy: &mut monad_state.block_policy,
            state_backend: &monad_state.state_backend,

            val_epoch_map: &monad_state.val_epoch_map,
            leader_election: &monad_state.leader_election,
            version: &monad_state.version,

            block_timestamp: &monad_state.block_timestamp,
            block_validator: &monad_state.block_validator,
            beneficiary: &monad_state.beneficiary,
            nodeid: &monad_state.nodeid,
            consensus_config: &monad_state.consensus_config,

            keypair: &monad_state.keypair,
            cert_keypair: &monad_state.cert_keypair,
        }
    }

    pub(super) fn update(
        &mut self,
        event: ConsensusEvent<ST, SCT, EPT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT>> {
        let live = match self.consensus {
            ConsensusMode::Live(live) => live,
            ConsensusMode::Sync {
                block_buffer,
                updating_target,
                ..
            } => {
                let mut cmds = Vec::new();
                if let ConsensusEvent::Message {
                    sender,
                    unverified_message,
                } = event.clone()
                {
                    if let Ok((author, ProtocolMessage::Proposal(proposal))) =
                        Self::verify_and_validate_consensus_message(
                            self.epoch_manager,
                            self.val_epoch_map,
                            self.version,
                            self.metrics,
                            sender,
                            unverified_message,
                        )
                    {
                        if let Some((new_root, new_high_qc)) =
                            block_buffer.handle_proposal(author, proposal)
                        {
                            if !*updating_target {
                                // used for deduplication, because RequestStateSync isn't synchronous
                                *updating_target = true;
                                info!(
                                    ?new_root,
                                    consensus_tip =? new_root.seq_num,
                                    "setting new statesync target",
                                );
                                cmds.push(WrappedConsensusCommand {
                                    state_root_delay: self.consensus_config.execution_delay,
                                    command: ConsensusCommand::RequestStateSync {
                                        root: new_root,
                                        high_qc: new_high_qc,
                                    },
                                });
                            }
                        }
                    }
                }
                tracing::trace!(?event, "ignoring ConsensusEvent, not live yet");
                return cmds;
            }
        };

        let mut consensus = ConsensusStateWrapper {
            consensus: live,

            metrics: self.metrics,
            tx_pool: self.txpool,
            epoch_manager: self.epoch_manager,
            block_policy: self.block_policy,
            state_backend: self.state_backend,

            val_epoch_map: self.val_epoch_map,
            election: self.leader_election,
            version: self.version.protocol_version,

            block_timestamp: self.block_timestamp,
            block_validator: self.block_validator,
            beneficiary: self.beneficiary,
            nodeid: self.nodeid,
            config: self.consensus_config,

            keypair: self.keypair,
            cert_keypair: self.cert_keypair,
        };

        let consensus_cmds = match event {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => {
                match Self::verify_and_validate_consensus_message(
                    consensus.epoch_manager,
                    consensus.val_epoch_map,
                    self.version,
                    consensus.metrics,
                    sender,
                    unverified_message,
                ) {
                    Ok((author, ProtocolMessage::Proposal(msg))) => {
                        consensus.handle_proposal_message(author, msg)
                    }
                    Ok((author, ProtocolMessage::Vote(msg))) => {
                        consensus.handle_vote_message(author, msg)
                    }
                    Ok((author, ProtocolMessage::Timeout(msg))) => {
                        consensus.handle_timeout_message(author, msg)
                    }
                    Err(evidence) => evidence,
                }
            }
            ConsensusEvent::Timeout => consensus.handle_timeout_expiry(),
            ConsensusEvent::BlockSync {
                block_range,
                full_blocks,
            } => consensus.handle_block_sync(block_range, full_blocks),
            ConsensusEvent::SendVote(round) => consensus.handle_vote_timer(round),
        };
        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand {
                state_root_delay: consensus.config.execution_delay,
                command: cmd,
            })
            .collect::<Vec<_>>()
    }

    pub(super) fn handle_execution_result(
        &mut self,
        execution_result: ExecutionResult<EPT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT>> {
        let ConsensusMode::Live(mode) = self.consensus else {
            unreachable!("handle_execution_result when not live")
        };
        let mut consensus = ConsensusStateWrapper {
            consensus: mode,

            metrics: self.metrics,
            tx_pool: self.txpool,
            epoch_manager: self.epoch_manager,
            block_policy: self.block_policy,
            state_backend: self.state_backend,

            val_epoch_map: self.val_epoch_map,
            election: self.leader_election,
            version: self.version.protocol_version,

            block_timestamp: self.block_timestamp,
            block_validator: self.block_validator,
            beneficiary: self.beneficiary,
            nodeid: self.nodeid,
            config: self.consensus_config,

            keypair: self.keypair,
            cert_keypair: self.cert_keypair,
        };

        let consensus_cmds = consensus.add_execution_result(execution_result);

        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand {
                state_root_delay: consensus.config.execution_delay,
                command: cmd,
            })
            .collect::<Vec<_>>()
    }

    pub(super) fn handle_validated_proposal(
        &mut self,
        author: NodeId<CertificateSignaturePubKey<ST>>,
        validated_proposal: ProposalMessage<ST, SCT, EPT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT>> {
        let ConsensusMode::Live(mode) = self.consensus else {
            unreachable!("handle_validated_proposal when not live")
        };

        let mut consensus = ConsensusStateWrapper {
            consensus: mode,

            metrics: self.metrics,
            tx_pool: self.txpool,
            epoch_manager: self.epoch_manager,
            block_policy: self.block_policy,
            state_backend: self.state_backend,

            val_epoch_map: self.val_epoch_map,
            election: self.leader_election,
            version: self.version.protocol_version,

            block_timestamp: self.block_timestamp,
            block_validator: self.block_validator,
            beneficiary: self.beneficiary,
            nodeid: self.nodeid,
            config: self.consensus_config,

            keypair: self.keypair,
            cert_keypair: self.cert_keypair,
        };

        let consensus_cmds = consensus.handle_proposal_message(author, validated_proposal);

        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand {
                state_root_delay: consensus.config.execution_delay,
                command: cmd,
            })
            .collect::<Vec<_>>()
    }

    fn verify_and_validate_consensus_message(
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        version: &MonadVersion,
        metrics: &mut Metrics,

        sender: NodeId<CertificateSignaturePubKey<ST>>,
        message: Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>,
    ) -> Result<
        (
            NodeId<CertificateSignaturePubKey<ST>>,
            ProtocolMessage<ST, SCT, EPT>,
        ),
        Vec<ConsensusCommand<ST, SCT, EPT>>,
    > {
        let verified_message = message
            .verify(epoch_manager, val_epoch_map, &sender.pubkey())
            .map_err(|e| {
                handle_validation_error(e, metrics);
                // TODO-2: collect evidence
                Vec::new()
            })?;

        let (author, _, verified_message) = verified_message.destructure();

        // Validated message according to consensus protocol spec
        let validated_mesage = verified_message
            .validate(epoch_manager, val_epoch_map, version.protocol_version)
            .map_err(|e| {
                handle_validation_error(e, metrics);
                // TODO-2: collect evidence
                Vec::new()
            })?;

        Ok((author, validated_mesage.into_inner()))
    }
}

pub(super) struct WrappedConsensusCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    state_root_delay: SeqNum,
    command: ConsensusCommand<ST, SCT, EPT>,
}

impl<ST, SCT, EPT> From<WrappedConsensusCommand<ST, SCT, EPT>>
    for Vec<Command<MonadEvent<ST, SCT, EPT>, VerifiedMonadMessage<ST, SCT, EPT>, ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(wrapped: WrappedConsensusCommand<ST, SCT, EPT>) -> Self {
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
            ConsensusCommand::Schedule { duration } => {
                parent_cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                    duration,
                    variant: TimeoutVariant::Pacemaker,
                    on_timeout: MonadEvent::ConsensusEvent(ConsensusEvent::Timeout),
                }))
            }
            ConsensusCommand::ScheduleReset => parent_cmds.push(Command::TimerCommand(
                TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
            )),
            ConsensusCommand::RequestSync(block_range) => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfRequest {
                        requester: BlockSyncSelfRequester::Consensus,
                        block_range,
                    }),
                )));
            }
            ConsensusCommand::CancelSync(block_range) => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfCancelRequest {
                        requester: BlockSyncSelfRequester::Consensus,
                        block_range,
                    }),
                )));
            }
            ConsensusCommand::RequestStateSync { root, high_qc } => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::StateSyncEvent(StateSyncEvent::RequestSync { root, high_qc }),
                )));
            }
            ConsensusCommand::StartExecution => {
                parent_cmds.push(Command::StateSyncCommand(StateSyncCommand::StartExecution));
            }
            ConsensusCommand::LedgerCommit(cmd) => {
                match cmd {
                    OptimisticCommit::Proposed(block) => {
                        let block_id = block.get_id();
                        let round = block.get_round();
                        let seq_num = block.get_seq_num();
                        parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                            OptimisticCommit::Proposed(block),
                        )));
                        parent_cmds.push(Command::StateRootHashCommand(
                            StateRootHashCommand::RequestProposed(block_id, seq_num, round),
                        ));
                    }
                    OptimisticCommit::Finalized(block) => {
                        let finalized_seq_num = block.get_seq_num();
                        parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                            OptimisticCommit::Finalized(block),
                        )));
                        parent_cmds.push(Command::StateRootHashCommand(
                            StateRootHashCommand::RequestFinalized(finalized_seq_num),
                        ));
                        parent_cmds.push(Command::StateRootHashCommand(
                            // upon committing block N, we no longer need state_root_N-delay
                            // therefore, we cancel below state_root_N-delay+1
                            //
                            // we'll be left with (state_root_N-delay, state_root_N] queued up, which is
                            // exactly `delay` number of roots
                            StateRootHashCommand::CancelBelow(
                                (finalized_seq_num + SeqNum(1)).max(wrapped.state_root_delay)
                                    - wrapped.state_root_delay,
                            ),
                        ));
                    }
                    OptimisticCommit::Verified(block_id) => {
                        parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                            OptimisticCommit::Verified(block_id),
                        )))
                    }
                }
            }
            ConsensusCommand::CheckpointSave {
                root_seq_num,
                high_qc_round,
                checkpoint,
            } => parent_cmds.push(Command::CheckpointCommand(CheckpointCommand {
                root_seq_num,
                high_qc_round,
                checkpoint,
            })),
            ConsensusCommand::ClearMempool => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::MempoolEvent(MempoolEvent::Clear),
                )));
            }
            ConsensusCommand::TimestampUpdate(t) => {
                parent_cmds.push(Command::TimestampCommand(TimestampCommand::AdjustDelta(t)))
            }
            ConsensusCommand::ScheduleVote { duration, round } => {
                parent_cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                    duration,
                    variant: TimeoutVariant::SendVote,
                    on_timeout: MonadEvent::ConsensusEvent(ConsensusEvent::SendVote(round)),
                }))
            }
        }
        parent_cmds
    }
}
