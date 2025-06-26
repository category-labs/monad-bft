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
    block::{BlockPolicy, ConsensusBlockHeader, OptimisticCommit, OptimisticPolicyCommit},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    tip::ConsensusTip,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    BlockSyncEvent, CheckpointCommand, Command, ConsensusEvent, LedgerCommand, LoopbackCommand,
    MempoolEvent, MonadEvent, RouterCommand, StateRootHashCommand, StateSyncEvent, TimeoutVariant,
    TimerCommand, TimestampCommand, TxPoolCommand,
};
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
const NUM_LEADERS_FORWARD_TXS: usize = 3;
const NUM_LEADERS_UPCOMING: usize = 3;

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
                            self.leader_election,
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
                                cmds.push(self.wrap(ConsensusCommand::RequestStateSync {
                                    root: new_root,
                                    high_qc: new_high_qc,
                                }));
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

        // Return commands before being wrapped into a Vec of WrappedConsensusCommand
        let consensus_cmds = match event {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => {
                match Self::verify_and_validate_consensus_message(
                    consensus.epoch_manager,
                    consensus.val_epoch_map,
                    consensus.election,
                    self.version,
                    consensus.metrics,
                    sender,
                    unverified_message,
                ) {
                    Ok((author, ProtocolMessage::Proposal(msg))) => {
                        let mut proposal_cmds =
                            consensus.handle_proposal_message(author, msg.clone());
                        // Maybe we could skip the below command if we could somehow determine that
                        // the peer isn't publishing to full nodes at the moment?
                        let epoch = msg.tip.block_header.epoch;
                        let cons_msg = ConsensusMessage {
                            version: consensus.version.to_owned(),
                            message: ProtocolMessage::Proposal(msg),
                        }
                        .sign(self.keypair);
                        proposal_cmds.push(ConsensusCommand::PublishToFullNodes {
                            epoch,
                            message: cons_msg,
                        });
                        proposal_cmds
                    }
                    Ok((author, ProtocolMessage::Vote(msg))) => {
                        consensus.handle_vote_message(author, msg)
                    }
                    Ok((author, ProtocolMessage::Timeout(msg))) => {
                        consensus.handle_timeout_message(author, msg)
                    }
                    Ok((author, ProtocolMessage::RoundRecovery(msg))) => {
                        consensus.handle_round_recovery_message(author, msg)
                    }
                    Ok((author, ProtocolMessage::NoEndorsement(msg))) => {
                        consensus.handle_no_endorsement_message(author, msg)
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
            .map(|cmd| self.wrap(cmd))
            .collect::<Vec<_>>()
    }

    pub(super) fn handle_mempool_event(
        &mut self,
        event: MempoolEvent<ST, SCT, EPT>,
    ) -> Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
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
                fresh_proposal_certificate,
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
                    proposal_epoch: epoch,
                    proposal_round: round,
                    tip: ConsensusTip::new(
                        consensus.keypair,
                        block_header,
                        fresh_proposal_certificate,
                    ),
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
                    message: VerifiedMonadMessage::Consensus(msg),
                })]
            }
            MempoolEvent::ForwardedTxs { sender, txs } => {
                vec![Command::TxPoolCommand(TxPoolCommand::InsertForwardedTxs {
                    sender,
                    txs,
                })]
            }
            MempoolEvent::ForwardTxs(txs) => {
                self.compute_upcoming_leader_round_pairs::<false, true, NUM_LEADERS_FORWARD_TXS>()
                    .into_iter()
                    .map(|(target, _)| {
                        // TODO ideally we could batch these all as one RouterCommand(PointToPoint) so
                        // that we can:
                        // 1. avoid cloning txns
                        // 2. avoid serializing multiple times
                        // 3. avoid raptor coding multiple times
                        // 4. use 1 sendmmsg in the router
                        Command::RouterCommand(RouterCommand::Publish {
                            target: RouterTarget::PointToPoint(target),
                            message: VerifiedMonadMessage::ForwardedTx(txs.clone()),
                        })
                    })
                    .collect_vec()
            }
        }
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
            .map(|cmd| self.wrap(cmd))
            .collect::<Vec<_>>()
    }

    pub(super) fn checkpoint(&mut self) -> Option<CheckpointCommand<ST, SCT, EPT>> {
        let ConsensusMode::Live(consensus) = self.consensus else {
            return None;
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
        let checkpoint = consensus.checkpoint();
        Some(CheckpointCommand {
            root_seq_num: consensus.consensus.blocktree().root().seq_num,
            checkpoint,
        })
    }

    fn verify_and_validate_consensus_message(
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
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
            .validate(
                epoch_manager,
                val_epoch_map,
                election,
                version.protocol_version,
            )
            .map_err(|e| {
                handle_validation_error(e, metrics);
                // TODO-2: collect evidence
                Vec::new()
            })?;

        Ok((author, validated_mesage.into_inner()))
    }

    fn compute_upcoming_leader_round_pairs<
        const INCLUDE_CURRENT_ROUND: bool,
        const SKIP_SELF: bool,
        const NUM: usize,
    >(
        &self,
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Round)> {
        let ConsensusMode::Live(mode) = &self.consensus else {
            return Vec::default();
        };

        (mode.get_current_round().0 + (if INCLUDE_CURRENT_ROUND { 0 } else { 1 })..)
            .take(NUM_LEADERS_FORWARD_TXS)
            .map(Round)
            .filter_map(|round| {
                let epoch = self.epoch_manager.get_epoch(round).expect("epoch exists");

                let Some(next_validator_set) = self.val_epoch_map.get_val_set(&epoch) else {
                    todo!("handle non-existent validatorset for next k round epoch");
                };

                let leader = self
                    .leader_election
                    .get_leader(round, next_validator_set.get_members());

                if SKIP_SELF {
                    (&leader != self.nodeid).then_some((leader, round))
                } else {
                    Some((leader, round))
                }
            })
            .unique_by(|(nodeid, _)| *nodeid)
            .collect()
    }

    fn get_upcoming_leader_rounds(&self) -> Vec<Round> {
        self.compute_upcoming_leader_round_pairs::<true, false, NUM_LEADERS_UPCOMING>()
            .into_iter()
            .map(|(_, round)| round)
            .collect()
    }

    pub(super) fn wrap(
        &self,
        command: ConsensusCommand<ST, SCT, EPT, BPT, SBT>,
    ) -> WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT> {
        WrappedConsensusCommand {
            state_root_delay: self.consensus_config.execution_delay,
            upcoming_leader_rounds: self.get_upcoming_leader_rounds(),
            command,
        }
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
    upcoming_leader_rounds: Vec<Round>,
    pub command: ConsensusCommand<ST, SCT, EPT, BPT, SBT>,
}

impl<ST, SCT, EPT, BPT, SBT> From<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
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
        let WrappedConsensusCommand {
            state_root_delay,
            upcoming_leader_rounds,
            command,
        } = wrapped;

        let mut parent_cmds: Vec<Command<_, _, _, _, _, _, _>> = Vec::new();

        match command {
            ConsensusCommand::EnterRound(epoch, round) => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::UpdateCurrentRound(
                    epoch, round,
                )));
                parent_cmds.push(Command::TxPoolCommand(TxPoolCommand::EnterRound {
                    epoch,
                    round,
                    upcoming_leader_rounds,
                }))
            }
            ConsensusCommand::Publish { target, message } => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::Publish {
                    target,
                    message: VerifiedMonadMessage::Consensus(message),
                }))
            }
            ConsensusCommand::PublishToFullNodes { epoch, message } => {
                parent_cmds.push(Command::RouterCommand(RouterCommand::PublishToFullNodes {
                    epoch,
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
            ConsensusCommand::CreateProposal {
                epoch,
                round,
                seq_num,
                high_qc,
                round_signature,
                last_round_tc,
                fresh_proposal_certificate,

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
                    fresh_proposal_certificate,

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
                        let round = block.get_block_round();
                        let seq_num = block.get_seq_num();
                    }
                    OptimisticPolicyCommit::Finalized(block) => {
                        let finalized_seq_num = block.get_seq_num();
                        parent_cmds.push(Command::TxPoolCommand(TxPoolCommand::BlockCommit(vec![
                            block,
                        ])));
                        parent_cmds.push(Command::StateRootHashCommand(
                            StateRootHashCommand::NotifyFinalized(finalized_seq_num),
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
