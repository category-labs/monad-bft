use itertools::Itertools;
use monad_blocksync::blocksync::BlockSyncSelfRequester;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::ProposalMessage,
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_state::{command::ConsensusCommand, ConsensusConfig, ConsensusStateWrapper};
use monad_consensus_types::{
    block::{
        BlockPolicy, ConsensusBlockHeader, ExecutionResult, OptimisticCommit,
        OptimisticPolicyCommit,
    },
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    BlockSyncEvent, CheckpointCommand, Command, ConsensusEvent, LedgerCommand, LoopbackCommand,
    MempoolEvent, MonadEvent, RouterCommand, StateRootHashCommand, StateSyncEvent, TimeoutVariant,
    TimerCommand, TimestampCommand, TxPoolCommand,
};
use monad_raptorcast::message::OutboundRouterMessage;
use monad_state_backend::StateBackend;
use monad_types::{ExecutionProtocol, NodeId, Round, RouterTarget, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use tracing::info;

use crate::{
    handle_validation_error, BlockTimestamp, ConsensusMode, MonadState, MonadVersion,
    VerifiedMonadMessage,
};

// TODO configurable
const NUM_LEADERS_FORWARD: usize = 3;

pub(super) struct ConsensusChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    consensus: &'a mut ConsensusMode<ST, SCT, EPT, BPT, SBT, CCT, CRT>,

    metrics: &'a mut Metrics,
    epoch_manager: &'a mut EpochManager,
    block_policy: &'a mut BPT,
    state_backend: &'a SBT,

    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    leader_election: &'a LT,
    version: &'a MonadVersion,

    block_timestamp: &'a BlockTimestamp,
    block_validator: &'a BVT,
    beneficiary: &'a [u8; 20],
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,
    consensus_config: &'a ConsensusConfig<CCT, CRT>,

    keypair: &'a ST::KeyPairType,
    cert_keypair: &'a SignatureCollectionKeyPairType<SCT>,
}

impl<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    ConsensusChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>,
    ) -> Self {
        Self {
            consensus: &mut monad_state.consensus,

            metrics: &mut monad_state.metrics,
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
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
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

    pub(super) fn handle_mempool_event(
        &mut self,
        event: MempoolEvent<SCT, EPT>,
    ) -> Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            OutboundRouterMessage<VerifiedMonadMessage<ST, SCT, EPT>, ST>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    > {
        let ConsensusMode::Live(consensus) = self.consensus else {
            match event {
                MempoolEvent::Proposal { .. } => {
                    unreachable!("txpool should never emit proposal while not live!")
                }
                MempoolEvent::ForwardedTxs { .. } | MempoolEvent::ForwardTxs(_) => {
                    return Vec::default()
                }
            }
        };
        let consensus = ConsensusStateWrapper {
            consensus,

            metrics: self.metrics,
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

        match event {
            MempoolEvent::Proposal {
                epoch,
                round,
                high_qc,
                seq_num,
                timestamp_ns,
                round_signature,
                delayed_execution_results,
                proposed_execution_inputs,
                last_round_tc,
            } => {
                consensus.metrics.consensus_events.creating_proposal += 1;
                let block_body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
                    execution_body: proposed_execution_inputs.body,
                });
                let block_header = ConsensusBlockHeader::new(
                    *consensus.nodeid,
                    epoch,
                    round,
                    delayed_execution_results,
                    proposed_execution_inputs.header,
                    block_body.get_id(),
                    high_qc,
                    seq_num,
                    timestamp_ns,
                    round_signature,
                );

                let p = ProposalMessage {
                    block_header,
                    block_body,
                    last_round_tc,
                };

                let msg = ConsensusMessage {
                    version: consensus.version.to_owned(),
                    message: ProtocolMessage::Proposal(p),
                }
                .sign(self.keypair);

                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::Raptorcast(epoch),
                    message: OutboundRouterMessage::AppMessage(VerifiedMonadMessage::Consensus(
                        msg,
                    )),
                })]
            }
            MempoolEvent::ForwardedTxs { sender, txs } => {
                vec![Command::TxPoolCommand(TxPoolCommand::InsertForwardedTxs {
                    sender,
                    txs,
                })]
            }
            MempoolEvent::ForwardTxs(txs) => {
                // Current round leader will only include txn in proposal if it
                // hasn't observed a TC we locally formed. In all other case,
                // current round leader has proposed and forwarded txn will not
                // get included
                //
                // Thus forwarding txn to leaders of (current round+1..)
                let round = consensus.consensus.get_current_round() + Round(1);

                (round.0..)
                    .take(NUM_LEADERS_FORWARD)
                    .map(|round| {
                        let epoch = consensus
                            .epoch_manager
                            .get_epoch(Round(round))
                            .expect("epoch for current and future rounds always exist");
                        let Some(next_validator_set) = self.val_epoch_map.get_val_set(&epoch)
                        else {
                            todo!("handle non-existent validatorset for next k round epoch");
                        };
                        let members = next_validator_set.get_members();
                        consensus.election.get_leader(Round(round), members)
                    })
                    .filter(|leader| leader != self.nodeid)
                    .unique()
                    .map(|target| {
                        // TODO ideally we could batch these all as one RouterCommand(PointToPoint) so
                        // that we can:
                        // 1. avoid cloning txns
                        // 2. avoid serializing multiple times
                        // 3. avoid raptor coding multiple times
                        // 4. use 1 sendmmsg in the router
                        Command::RouterCommand(RouterCommand::Publish {
                            target: RouterTarget::PointToPoint(target),
                            message: OutboundRouterMessage::AppMessage(
                                VerifiedMonadMessage::ForwardedTx(txs.clone()),
                            ),
                        })
                    })
                    .collect_vec()
            }
        }
    }

    pub(super) fn handle_execution_result(
        &mut self,
        execution_result: ExecutionResult<EPT>,
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let ConsensusMode::Live(mode) = self.consensus else {
            unreachable!("handle_execution_result when not live")
        };
        let mut consensus = ConsensusStateWrapper {
            consensus: mode,

            metrics: self.metrics,
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
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let ConsensusMode::Live(mode) = self.consensus else {
            unreachable!("handle_validated_proposal when not live")
        };

        let mut consensus = ConsensusStateWrapper {
            consensus: mode,

            metrics: self.metrics,
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
        Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>>,
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

pub(super) struct WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    state_root_delay: SeqNum,
    command: ConsensusCommand<ST, SCT, EPT, BPT, SBT>,
}

impl<ST, SCT, EPT, BPT, SBT> From<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            OutboundRouterMessage<VerifiedMonadMessage<ST, SCT, EPT>, ST>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn from(wrapped: WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>) -> Self {
        let mut parent_cmds: Vec<Command<_, _, _, _, _, _, _>> = Vec::new();

        match wrapped.command {
            ConsensusCommand::EnterRound(epoch, round) => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::UpdateCurrentRound(
                    epoch, round,
                )));
                parent_cmds.push(Command::TxPoolCommand(TxPoolCommand::EnterRound {
                    epoch,
                    round,
                }))
            }
            ConsensusCommand::Publish { target, message } => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::Publish {
                    target,
                    message: OutboundRouterMessage::AppMessage(VerifiedMonadMessage::Consensus(
                        message,
                    )),
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
            ConsensusCommand::CreateProposal {
                epoch,
                round,
                seq_num,
                high_qc,
                round_signature,
                last_round_tc,

                tx_limit,
                proposal_gas_limit,
                proposal_byte_limit,
                beneficiary,
                timestamp_ns,

                extending_blocks,
                delayed_execution_results,
            } => {
                parent_cmds.push(Command::TxPoolCommand(TxPoolCommand::CreateProposal {
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,

                    tx_limit,
                    proposal_gas_limit,
                    proposal_byte_limit,
                    beneficiary,
                    timestamp_ns,

                    extending_blocks,
                    delayed_execution_results,
                }));
            }
            ConsensusCommand::CommitBlocks(commit) => {
                parent_cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                    OptimisticCommit::from(&commit),
                )));

                match commit {
                    OptimisticPolicyCommit::Proposed(block) => {
                        let block_id = block.get_id();
                        let round = block.get_round();
                        let seq_num = block.get_seq_num();
                        parent_cmds.push(Command::StateRootHashCommand(
                            StateRootHashCommand::RequestProposed(block_id, seq_num, round),
                        ));
                    }
                    OptimisticPolicyCommit::Finalized(block) => {
                        let finalized_seq_num = block.get_seq_num();
                        parent_cmds.push(Command::TxPoolCommand(TxPoolCommand::BlockCommit(vec![
                            block,
                        ])));
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
                }
            }
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
            ConsensusCommand::CheckpointSave {
                root_seq_num,
                high_qc_round,
                checkpoint,
            } => parent_cmds.push(Command::CheckpointCommand(CheckpointCommand {
                root_seq_num,
                high_qc_round,
                checkpoint,
            })),
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
