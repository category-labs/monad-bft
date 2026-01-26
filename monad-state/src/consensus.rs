// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use itertools::Itertools;
use monad_blocksync::blocksync::BlockSyncSelfRequester;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::ProposalMessage,
    },
    validation::{
        certificate_cache::CertificateCache,
        signing::{Unvalidated, Unverified, Validated, Verified},
    },
};
use monad_consensus_state::{command::ConsensusCommand, ConsensusConfig, ConsensusStateWrapper};
use monad_consensus_types::{
    block::{BlockPolicy, ConsensusBlockHeader, OptimisticCommit, OptimisticPolicyCommit},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner},
    tip::ConsensusTip,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    BlockSyncEvent, Command, ConfigFileCommand, ConsensusEvent, LedgerCommand, LoopbackCommand,
    MempoolEvent, MonadEvent, RouterCommand, StateSyncEvent, TimeoutVariant, TimerCommand,
    TimestampCommand, TxPoolCommand, ValSetCommand,
};
use monad_state_backend::StateBackend;
use monad_types::{ExecutionProtocol, NodeId, Round, RouterTarget};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use tracing::{debug_span, info, warn};

use crate::{
    handle_validation_error, BlockTimestamp, ConsensusMode, MonadState, MonadVersion, Role,
    VerifiedMonadMessage,
};

pub(super) struct ConsensusChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    consensus: &'a mut ConsensusMode<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    certificate_cache: &'a mut CertificateCache<ST, SCT, EPT>,

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
    SBT: StateBackend<ST, SCT>,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>,
    ) -> Self {
        Self {
            consensus: &mut monad_state.consensus,
            certificate_cache: &mut monad_state.certificate_cache,

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
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>> {
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
                    // skip evidence collection in sync mode
                    if let Ok(verified_message) = Self::verify_and_validate_consensus_message(
                        self.certificate_cache,
                        self.epoch_manager,
                        self.val_epoch_map,
                        self.leader_election,
                        self.version,
                        self.metrics,
                        sender,
                        unverified_message,
                    ) {
                        let (author, protocol_message) = verified_message.into_inner();
                        if let ProtocolMessage::Proposal(proposal) = protocol_message {
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
                                    self.metrics.consensus_events.trigger_state_sync += 1;
                                    cmds.push(self.wrap(ConsensusCommand::RequestStateSync {
                                        root: new_root,
                                        high_qc: new_high_qc,
                                    }));
                                }
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
                    self.certificate_cache,
                    consensus.epoch_manager,
                    consensus.val_epoch_map,
                    consensus.election,
                    self.version,
                    consensus.metrics,
                    sender,
                    unverified_message,
                ) {
                    Ok(verified_message) => {
                        let (author, protocol_message) = verified_message.clone().into_inner();
                        match protocol_message {
                            ProtocolMessage::Proposal(msg) => {
                                let proposal_epoch = msg.proposal_epoch;
                                let proposal_round = msg.proposal_round;
                                let mut proposal_cmds =
                                    consensus.handle_proposal_message(author, msg);
                                // TODO:Maybe we could skip the below command if we could somehow determine that
                                // the peer isn't publishing to full nodes at the moment?
                                //
                                // send verified_message carrying author signature
                                proposal_cmds.push(ConsensusCommand::PublishToFullNodes {
                                    epoch: proposal_epoch,
                                    round: proposal_round,
                                    message: verified_message,
                                });
                                proposal_cmds
                            }
                            ProtocolMessage::Vote(msg) => {
                                consensus.handle_vote_message(author, msg)
                            }
                            ProtocolMessage::Timeout(msg) => {
                                consensus.handle_timeout_message(author, msg)
                            }
                            ProtocolMessage::RoundRecovery(msg) => {
                                consensus.handle_round_recovery_message(author, msg)
                            }
                            ProtocolMessage::NoEndorsement(msg) => {
                                consensus.handle_no_endorsement_message(author, msg)
                            }
                            ProtocolMessage::AdvanceRound(msg) => {
                                consensus.handle_advance_round_message(author, msg)
                            }
                        }
                    }
                    Err(evidence) => {
                        // If we have evidence, we return it as commands
                        evidence
                    }
                }
            }
            ConsensusEvent::Timeout(round) => consensus.handle_timeout_expiry(round),
            ConsensusEvent::BlockSync {
                block_range,
                full_blocks,
            } => consensus.handle_block_sync(block_range, full_blocks),
            ConsensusEvent::SendVote(round) => consensus.handle_vote_timer(round),
        };
        self.filter_cmds(consensus_cmds.into_iter())
            .map(|cmd| self.wrap(cmd))
            .collect_vec()
    }

    fn try_build_state_wrapper<'b>(
        &'b mut self,
    ) -> Option<ConsensusStateWrapper<'b, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>> {
        match self.consensus {
            ConsensusMode::Sync { .. } => None,
            ConsensusMode::Live(consensus) => Some(ConsensusStateWrapper {
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
            }),
        }
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
            CCT,
            CRT,
        >,
    > {
        let Some(consensus) = self.try_build_state_wrapper() else {
            match event {
                MempoolEvent::Proposal { .. } => {
                    unreachable!("txpool should never emit proposal while not live!")
                }
                MempoolEvent::ForwardedTxs { .. } | MempoolEvent::ForwardTxs(_) => {
                    return Vec::default()
                }
            }
        };

        match event {
            MempoolEvent::Proposal {
                epoch,
                round,
                high_qc,
                seq_num,
                timestamp_ns,
                round_signature,
                base_fee,
                base_fee_trend,
                base_fee_moment,
                delayed_execution_results,
                proposed_execution_inputs,
                last_round_tc,
                fresh_proposal_certificate,
            } => {
                let _span = debug_span!("mempool proposal").entered();
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
                    base_fee,
                    base_fee_trend,
                    base_fee_moment,
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
                consensus
                    .iter_future_other_leaders()
                    .map(|target| {
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
    ) -> Vec<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>> {
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

        self.filter_cmds(consensus_cmds.into_iter())
            .map(|cmd| self.wrap(cmd))
            .collect_vec()
    }

    pub(super) fn checkpoint(&mut self) -> Option<ConfigFileCommand<ST, SCT, EPT>> {
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
        Some(ConfigFileCommand::Checkpoint {
            root_seq_num: consensus.consensus.blocktree().root().seq_num,
            checkpoint,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn verify_and_validate_consensus_message(
        certificate_cache: &mut CertificateCache<ST, SCT, EPT>,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        version: &MonadVersion,
        metrics: &mut Metrics,

        sender: NodeId<CertificateSignaturePubKey<ST>>,
        message: Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>,
    ) -> Result<
        Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>,
        Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>>,
    > {
        let verified_message = message
            .verify(epoch_manager, val_epoch_map, &sender.pubkey())
            .map_err(|e| {
                handle_validation_error(e, metrics);
                // TODO-2: collect evidence
                Vec::new()
            })?;

        // Validated message according to consensus protocol spec
        let validated_message = verified_message
            .validate(
                certificate_cache,
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

        Ok(validated_message)
    }

    pub fn get_role(&self) -> Role {
        match self.consensus {
            ConsensusMode::Sync { .. } => Role::FullNode,
            ConsensusMode::Live(_) => {
                // - Validator if self is present in the current validator set
                // - FullNode otherwise
                let consensus_epoch = self.consensus.current_epoch();
                let validator_set = self
                    .val_epoch_map
                    .get_val_set(&consensus_epoch)
                    .unwrap_or_else(|| {
                        panic!(
                            "unknown validator set for current_epoch={:?}",
                            consensus_epoch
                        )
                    });

                if validator_set.is_member(self.nodeid) {
                    Role::Validator
                } else {
                    Role::FullNode
                }
            }
        }
    }

    fn filter_cmds(
        &self,
        consensus_cmds: impl Iterator<Item = ConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>>,
    ) -> impl Iterator<Item = ConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>> {
        let role = self.get_role();

        consensus_cmds.filter(move |cmd| {
            match role {
                Role::FullNode => match cmd {
                    // disable sending votes/timeouts for full node
                    ConsensusCommand::Publish { .. } => false,
                    // consensus state logic shouldn't trigger create proposal on a
                    // full node, but filtering it out to be safe
                    ConsensusCommand::CreateProposal { .. } => {
                        warn!("Full node emitting CreateProposal command");
                        false
                    }
                    ConsensusCommand::PublishToFullNodes { .. } => false,
                    _ => true,
                },
                Role::Validator => true,
            }
        })
    }

    pub(super) fn wrap(
        &mut self,
        command: ConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    ) -> WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT> {
        WrappedConsensusCommand {
            upcoming_leader_rounds: self
                .try_build_state_wrapper()
                .map(|consensus| consensus.iter_upcoming_self_leader_rounds().collect())
                .unwrap_or_default(),
            command,
        }
    }
}

pub(super) struct WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    upcoming_leader_rounds: Vec<Round>,
    pub command: ConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT>
    From<WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
            CCT,
            CRT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn from(wrapped: WrappedConsensusCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>) -> Self {
        let WrappedConsensusCommand {
            upcoming_leader_rounds,
            command,
        } = wrapped;

        let mut parent_cmds: Vec<Command<_, _, _, _, _, _, _, _, _>> = Vec::new();

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
                parent_cmds.push(Command::RouterCommand(RouterCommand::PublishWithPriority {
                    target,
                    message: VerifiedMonadMessage::Consensus(message),
                    priority: monad_types::UdpPriority::High,
                }))
            }
            ConsensusCommand::PublishToFullNodes {
                epoch,
                round,
                message,
            } => parent_cmds.push(Command::RouterCommand(RouterCommand::PublishToFullNodes {
                epoch,
                round,
                message: VerifiedMonadMessage::Consensus(message),
            })),
            ConsensusCommand::Schedule { round, duration } => {
                parent_cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                    duration,
                    variant: TimeoutVariant::Pacemaker,
                    on_timeout: MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(round)),
                }))
            }
            ConsensusCommand::ScheduleReset => parent_cmds.push(Command::TimerCommand(
                TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
            )),
            ConsensusCommand::CreateProposal {
                node_id,
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
                    node_id,
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
                    OptimisticPolicyCommit::Voted(block) => {
                        let _ = block.get_id();
                    }
                    OptimisticPolicyCommit::Finalized(block) => {
                        let finalized_seq_num = block.get_seq_num();
                        parent_cmds.push(Command::TxPoolCommand(TxPoolCommand::BlockCommit(vec![
                            block,
                        ])));
                        parent_cmds.push(Command::ValSetCommand(ValSetCommand::NotifyFinalized(
                            finalized_seq_num,
                        )));
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
