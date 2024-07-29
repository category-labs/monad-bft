use std::marker::PhantomData;

use monad_consensus::messages::{
    consensus_message::ProtocolMessage, message::RequestBlockSyncMessage,
};
use monad_consensus_state::{
    command::ConsensusCommand, ConsensusConfig, ConsensusState, ConsensusStateWrapper,
};
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
use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    BlockSyncEvent, CheckpointCommand, Command, ConsensusEvent, LedgerCommand, LoopbackCommand,
    MempoolEvent, MonadEvent, RouterCommand, StateRootHashCommand, TimerCommand, TimestampCommand,
};
use monad_types::{NodeId, SeqNum, TimeoutVariant};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection,
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{
    handle_validation_error, BlockTimestamp, MonadState, MonadVersion, VerifiedMonadMessage,
};

pub(super) struct ConsensusChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT, RBCT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,
    SVT: StateRootValidator,
{
    consensus: &'a mut ConsensusState<SCT, BPT, SBT, RBCT>,

    metrics: &'a mut Metrics,
    txpool: &'a mut TT,
    epoch_manager: &'a mut EpochManager,
    block_policy: &'a mut BPT,
    reserve_balance_cache: &'a mut RBCT,

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

impl<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    ConsensusChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT, RBCT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            consensus: &mut monad_state.consensus,

            metrics: &mut monad_state.metrics,
            txpool: &mut monad_state.txpool,
            epoch_manager: &mut monad_state.epoch_manager,
            block_policy: &mut monad_state.block_policy,
            reserve_balance_cache: &mut monad_state.reserve_balance_cache,

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
        let mut consensus = ConsensusStateWrapper {
            consensus: self.consensus,

            metrics: self.metrics,
            tx_pool: self.txpool,
            epoch_manager: self.epoch_manager,
            block_policy: self.block_policy,
            reserve_balance_cache: self.reserve_balance_cache,

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

        let vec = match event {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => {
                let verified_message = match unverified_message.verify(
                    consensus.epoch_manager,
                    consensus.val_epoch_map,
                    &sender.pubkey(),
                ) {
                    Ok(m) => m,
                    Err(e) => {
                        handle_validation_error(e, consensus.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                let (author, _, verified_message) = verified_message.destructure();

                // Validated message according to consensus protocol spec
                let validated_mesage = match verified_message.validate(
                    consensus.epoch_manager,
                    consensus.val_epoch_map,
                    consensus.version,
                ) {
                    Ok(m) => m.into_inner(),
                    Err(e) => {
                        handle_validation_error(e, consensus.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                match validated_mesage {
                    ProtocolMessage::Proposal(msg) => {
                        consensus.handle_proposal_message(author, msg)
                    }
                    ProtocolMessage::Vote(msg) => consensus.handle_vote_message(author, msg),
                    ProtocolMessage::Timeout(msg) => consensus.handle_timeout_message(author, msg),
                }
            }
            ConsensusEvent::Timeout => consensus
                .handle_timeout_expiry()
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        consensus.keypair,
                        consensus.cert_keypair,
                        consensus.version,
                        cmd,
                    )
                })
                .collect(),
            ConsensusEvent::BlockSync(block) => consensus.handle_block_sync(block),
        };
        let consensus_cmds = vec;
        consensus_cmds
            .into_iter()
            .map(|cmd| WrappedConsensusCommand {
                state_root_delay: consensus.state_root_validator.get_delay(),
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
            ConsensusCommand::RequestSync { block_id } => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfRequest {
                        request: RequestBlockSyncMessage { block_id },
                    }),
                )));
            }
            ConsensusCommand::CancelSync { block_id } => {
                parent_cmds.push(Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfCancelRequest {
                        request: RequestBlockSyncMessage { block_id },
                    }),
                )));
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
