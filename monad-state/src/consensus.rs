use std::marker::PhantomData;

use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{BlockSyncRequestMessage, ProposalMessage},
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_state::{command::ConsensusCommand, ConsensusConfig, ConsensusStateWrapper};
use monad_consensus_types::{
    block::{BlockPolicy, BlockType},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::StateRootValidator,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    BlockSyncEvent, BlockSyncSelfRequester, CheckpointCommand, Command, ConsensusEvent, LedgerCommand, LoopbackCommand, MempoolEvent, MonadEvent, RouterCommand, StateRootHashCommand, StateSyncCommand, StateSyncEvent, TimeoutVariant, TimerCommand, TimestampCommand
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

pub(super) struct ConsensusChildState<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,
    SVT: StateRootValidator,
{
    consensus: &'a mut ConsensusMode<SCT, BPT, SBT>,

    metrics: &'a mut Metrics,
    txpool: &'a mut TT,
    epoch_manager: &'a mut EpochManager,
    block_policy: &'a mut BPT,
    state_backend: &'a SBT,

    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    leader_election: &'a LT,
    version: &'a MonadVersion,

    state_root_validator: &'a SVT,
    block_timestamp: &'a BlockTimestamp,
    block_validator: &'a BVT,
    beneficiary: &'a EthAddress,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,
    consensus_config: &'a ConsensusConfig,

    keypair: &'a ST::KeyPairType,
    cert_keypair: &'a SignatureCollectionKeyPairType<SCT>,

    _phantom: PhantomData<ASVT>,
}

impl<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
    ConsensusChildState<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    BPT: BlockPolicy<SCT, SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>,
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

            state_root_validator: &monad_state.state_root_validator,
            block_timestamp: &monad_state.block_timestamp,
            block_validator: &monad_state.block_validator,
            beneficiary: &monad_state.beneficiary,
            nodeid: &monad_state.nodeid,
            consensus_config: &monad_state.consensus_config,

            keypair: &monad_state.keypair,
            cert_keypair: &monad_state.cert_keypair,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: ConsensusEvent<ST, SCT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT>> {
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
                                    consensus_tip =? new_high_qc.get_seq_num(),
                                    "setting new statesync target",
                                );
                                cmds.push(WrappedConsensusCommand {
                                    state_root_delay: self.state_root_validator.get_delay(),
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

            state_root_validator: self.state_root_validator,
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
            ConsensusEvent::BlockSync { block_id_range, full_blocks } => {
                // TODO remove block id range ?
                let mut cmds = Vec::new();
                for full_block in full_blocks {
                    cmds.extend(consensus.handle_block_sync(full_block.block, full_block.payload))
                }
                
                cmds
            }
        };
        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand {
                state_root_delay: consensus.state_root_validator.get_delay(),
                command: cmd,
            })
            .collect::<Vec<_>>()
    }

    pub(super) fn handle_validated_proposal(
        &mut self,
        author: NodeId<CertificateSignaturePubKey<ST>>,
        validated_proposal: ProposalMessage<SCT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT>> {
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

            state_root_validator: self.state_root_validator,
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
                state_root_delay: consensus.state_root_validator.get_delay(),
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
        message: Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>,
    ) -> Result<
        (NodeId<CertificateSignaturePubKey<ST>>, ProtocolMessage<SCT>),
        Vec<ConsensusCommand<ST, SCT>>,
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

pub(super) struct WrappedConsensusCommand<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    state_root_delay: SeqNum,
    command: ConsensusCommand<ST, SCT>,
}

impl<ST, SCT> From<WrappedConsensusCommand<ST, SCT>>
    for Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(wrapped: WrappedConsensusCommand<ST, SCT>) -> Self {
        let mut parent_cmds: Vec<Command<_, _, _>> = Vec::new();

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
            ConsensusCommand::RequestRangeSync { block_id_range } => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfRequest {
                        requester: BlockSyncSelfRequester::Consensus,
                        block_id_range
                    }),
                )));
            }
            ConsensusCommand::CancelRangeSync { block_id_range } => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfCancelRequest {
                        requester: BlockSyncSelfRequester::Consensus,
                        block_id_range
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
            ConsensusCommand::LedgerCommit(blocks) => {
                let last_block = blocks.iter().last().expect("LedgerCommit no blocks");
                parent_cmds.extend(blocks.iter().map(|block| {
                    Command::StateRootHashCommand(StateRootHashCommand::Request(
                        block.get_seq_num(),
                    ))
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
                parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(blocks)));
            }
            ConsensusCommand::CheckpointSave(checkpoint) => parent_cmds.push(
                Command::CheckpointCommand(CheckpointCommand::Save(checkpoint)),
            ),
            ConsensusCommand::ClearMempool => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::MempoolEvent(MempoolEvent::Clear),
                )));
            }
            ConsensusCommand::TimestampUpdate(t) => {
                parent_cmds.push(Command::TimestampCommand(TimestampCommand::AdjustDelta(t)))
            }
        }
        parent_cmds
    }
}
