use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, time::Duration};

use monad_blocktree::blocktree::BlockTree;
use monad_chain_config::{
    execution_revision::ExecutionChainParams,
    revision::{ChainParams, ChainRevision},
    ChainConfig,
};
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    pacemaker::Pacemaker,
    validation::safety::Safety,
    vote_state::VoteState,
};
use monad_consensus_types::{
    block::{BlockPolicy, BlockRange, ConsensusFullBlock, OptimisticPolicyCommit},
    block_validator::{BlockValidationError, BlockValidator},
    checkpoint::{Checkpoint, LockedEpoch, RootInfo},
    metrics::Metrics,
    payload::RoundSignature,
    quorum_certificate::{QuorumCertificate, Rank},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
    voting::Vote,
    RoundCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::StateBackend;
use monad_types::{BlockId, Epoch, ExecutionProtocol, NodeId, Round, RouterTarget, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use tracing::{debug, info, trace, warn};

use crate::{command::ConsensusCommand, timestamp::BlockTimestamp};

pub mod command;
pub mod timestamp;

/// core consensus algorithm
pub struct ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    /// Prospective blocks are stored here while they wait to be
    /// committed
    pending_block_tree: BlockTree<ST, SCT, EPT, BPT, SBT>,
    /// State machine to track collected votes for proposals
    vote_state: VoteState<SCT>,
    /// Outgoing prepared votes to send to next leader
    scheduled_vote: Option<OutgoingVoteStatus>,
    /// Tracks and updates the current round
    pacemaker: Pacemaker<ST, SCT, EPT, CCT, CRT>,
    /// Policy for upholding consensus safety when voting or extending branches
    safety: Safety<ST, SCT, EPT>,
    block_sync_requests: BTreeMap<BlockId, BlockSyncRequestStatus>,
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> PartialEq
    for ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    CCT: PartialEq,
    CRT: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.pending_block_tree.eq(&other.pending_block_tree)
            && self.vote_state.eq(&other.vote_state)
            && self.pacemaker.eq(&other.pacemaker)
            && self.safety.eq(&other.safety)
    }
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> Debug for ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    CCT: Debug,
    CRT: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusState")
            .field("pending_block_tree", &self.pending_block_tree)
            .field("vote_state", &self.vote_state)
            .field("pacemaker", &self.pacemaker)
            .field("safety", &self.safety)
            .finish()
    }
}

struct BlockSyncRequestStatus {
    range: BlockRange,
    // once a block with round >= cancel_round is committed, this request will be canceled.
    cancel_round: Round,
}

#[derive(Debug, Clone)]
enum OutgoingVoteStatus {
    TimerFired,
    VoteReady(Vote),
}

pub struct ConsensusStateWrapper<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub consensus: &'a mut ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>,

    pub metrics: &'a mut Metrics,
    pub epoch_manager: &'a mut EpochManager,
    /// Policy for validating chain extension
    /// Mutable because consensus tip will be updated
    pub block_policy: &'a mut BPT,
    pub state_backend: &'a SBT,

    pub val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    pub election: &'a LT,
    pub version: u32,

    /// Policy for validating incoming proposals
    pub block_validator: &'a BVT,
    /// Track local timestamp and validate proposal timestamps
    pub block_timestamp: &'a BlockTimestamp,

    /// Destination address for proposal payments
    pub beneficiary: &'a [u8; 20],
    /// This nodes public NodeId; what other nodes see in the validator set
    pub nodeid: &'a NodeId<SCT::NodeIdPubKey>,
    /// Parameters for consensus algorithm behaviour
    pub config: &'a ConsensusConfig<CCT, CRT>,

    // TODO-2 deprecate keypairs should probably have a different interface
    // so that users have options for securely storing their keys
    pub keypair: &'a ST::KeyPairType,
    pub cert_keypair: &'a SignatureCollectionKeyPairType<SCT>,
}

/// Consensus algorithm's configurable parameters
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct ConsensusConfig<CCT, CRT>
where
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub execution_delay: SeqNum,
    /// Duration used by consensus to determine timeout lengths
    /// delta should be approximately equal to the upper bound of message
    /// delivery during a broadcast
    pub delta: Duration,
    /// Chain config, determines vote pacing (minimum wait-time between sending
    /// votes) at a given round
    pub chain_config: CCT,
    /// If the node is lagging over state_sync_threshold blocks behind,
    /// then it should trigger statesync.
    pub live_to_statesync_threshold: SeqNum,
    /// If the node is in StateSync mode and is within this many blocks of
    /// root, then it should trigger statesync.
    ///
    /// Must be lower than live_to_statesync_threshold. The node will have
    /// at least (live_to_statesync_threshold - statesync_to_live_threshold)
    /// blocks of time to sync live_to_statesync_threshold blocks.
    pub statesync_to_live_threshold: SeqNum,
    /// Start execution if receive a high_qc within this threshold of root
    /// Must be lower than live_to_statesync_threshold
    pub start_execution_threshold: SeqNum,

    pub timestamp_latency_estimate_ns: u128,

    pub _phantom: PhantomData<CRT>,
}

/// Actions after state root validation
#[derive(Debug)]
pub enum StateRootAction {
    /// StateRoot validation is successful, proceed to next steps
    Proceed,
    /// StateRoot validation is unsuccessful - there's a mismatch of StateRoots.
    /// Reject the proposal immediately
    Reject,
    /// StateRoot validation is undecided - we haven't collect enough
    /// information. Either the state root is missing because we haven't
    /// received a majority quorum on the state root, or the state root is
    /// out-of-range. It's ok to insert to the block tree and observe if a QC
    /// forms on the block. But we shouldn't vote on the block
    Defer,
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    /// Create the core consensus state
    ///
    /// Arguments
    ///
    /// config - collection of configurable parameters for core consensus algorithm
    pub fn new(
        epoch_manager: &EpochManager,
        config: &ConsensusConfig<CCT, CRT>,
        root: RootInfo,
        high_certificate: RoundCertificate<ST, SCT, EPT>,
        high_qc: QuorumCertificate<SCT>,
    ) -> Self {
        let pacemaker = Pacemaker::new(
            config.delta,
            config.chain_config,
            config.delta, // TODO: change this to different value later
            epoch_manager,
            high_certificate,
            high_qc.clone(),
        );
        let high_qc_round = high_qc.get_round();
        ConsensusState {
            pending_block_tree: BlockTree::new(root),
            scheduled_vote: None,
            vote_state: VoteState::new(pacemaker.get_current_round()),
            pacemaker,
            safety: Safety::new(
                // ConsensusState is timed out synchronously on MonadState init, which always sets
                // highest_vote_round to consensus_round
                //
                // this gives us the guarantee that we will never vote on consensus_round
                //
                // we set this to high_qc_round here just because our consensus state machine unit
                // tests don't assume that Round(1) gets timed out
                high_qc_round, // highest_vote_round
            ),

            block_sync_requests: Default::default(),
        }
    }

    pub fn blocktree(&self) -> &BlockTree<ST, SCT, EPT, BPT, SBT> {
        &self.pending_block_tree
    }

    pub fn get_high_qc(&self) -> &QuorumCertificate<SCT> {
        self.pacemaker.high_qc()
    }

    pub fn get_current_epoch(&self) -> Epoch {
        self.pacemaker.get_current_epoch()
    }

    pub fn get_current_round(&self) -> Round {
        self.pacemaker.get_current_round()
    }
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    ConsensusStateWrapper<'_, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    /// handles the local timeout expiry event
    pub fn handle_timeout_expiry(&mut self) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let mut cmds = Vec::new();

        self.metrics.consensus_events.local_timeout += 1;
        debug!(
            round = ?self.consensus.pacemaker.get_current_round(),
            "local timeout"
        );
        cmds.extend(
            self.consensus
                .pacemaker
                .handle_event(&mut self.consensus.safety)
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        self.keypair,
                        self.cert_keypair,
                        self.version,
                        cmd,
                    )
                }),
        );
        cmds
    }

    /// handles proposal messages from other nodes
    /// validators and election are required as part of verifying the proposal certificates
    /// as well as determining the next leader
    /// Proposals can include NULL blocks which are blocks containing 0 transactions,
    /// an empty list.
    /// NULL block proposals are not required to validate the state_root field of the
    /// proposal's payload
    #[must_use]
    pub fn handle_proposal_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        p: ProposalMessage<ST, SCT, EPT>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let _handle_proposal_span =
            tracing::info_span!("handle_proposal_span", "{}", author).entered();
        info!(round = ?p.block_header.round, "received proposal");
        debug!(?author, proposal = ?p, "proposal message");
        self.metrics.consensus_events.handle_proposal += 1;

        let mut cmds = Vec::new();

        let epoch = self
            .epoch_manager
            .get_epoch(p.block_header.round)
            .expect("epoch verified");
        let validator_set = self
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("proposal message was verified");

        // a valid proposal will advance the pacemaker round so capture the original round before
        // handling the proposal certificate
        let _original_round = self.consensus.pacemaker.get_current_round();

        let process_certificate_cmds = self.process_qc(&p.block_header.qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            debug!(?last_round_tc, "Handled proposal with TC");
            self.metrics.consensus_events.proposal_with_tc += 1;
            let advance_round_cmds = self
                .consensus
                .pacemaker
                .process_certificate(
                    self.metrics,
                    self.epoch_manager,
                    RoundCertificate::Tc(last_round_tc.clone()),
                )
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        self.keypair,
                        self.cert_keypair,
                        self.version,
                        cmd,
                    )
                });
            cmds.extend(advance_round_cmds);
        }

        // author, leader, round checks
        let round = self.consensus.pacemaker.get_current_round();
        let block_round = p.block_header.round;
        let block_seq_num = p.block_header.seq_num;
        let block_round_leader = self
            .election
            .get_leader(block_round, validator_set.get_members());
        if block_round > round
            || author != block_round_leader
            || p.block_header.author != block_round_leader
        {
            debug!(
                expected_round =? round,
                round =? block_round,
                expected_leader =? block_round_leader,
                author =? author,
                block_author =? p.block_header.author,
                "Invalid Proposal"
            );
            self.metrics.consensus_events.invalid_proposal_round_leader += 1;
            return cmds;
        }

        if self.consensus.pending_block_tree.round_exists(&block_round) {
            // TODO emit evidence if this is a *different* block for the same round
            warn!(
                ?round,
                ?block_round_leader,
                ?block_seq_num,
                "dropping proposal, already received for this round"
            );
            return cmds;
        }

        let ChainParams {
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            vote_pace: _,
        } = self
            .config
            .chain_config
            .get_chain_revision(block_round)
            .chain_params();

        let ExecutionChainParams { max_code_size } = {
            // u64::MAX seconds is ~500 Billion years
            let timestamp_s: u64 = (p.block_header.timestamp_ns / 1_000_000_000)
                .try_into()
                // we don't assert because timestamp_ns is untrusted
                .unwrap_or(u64::MAX);
            self.config
                .chain_config
                .get_execution_chain_revision(timestamp_s)
                .execution_chain_params()
        };

        let author_pubkey = self
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("proposal message was verified")
            .map
            .get(&author)
            .expect("proposal author exists in validator_mapping");
        let block = match self.block_validator.validate(
            p.block_header,
            p.block_body,
            Some(author_pubkey),
            *tx_limit,
            *proposal_gas_limit,
            *proposal_byte_limit,
            *max_code_size,
        ) {
            Ok(block) => block,
            Err(BlockValidationError::TxnError) => {
                warn!(
                    ?round,
                    ?block_round_leader,
                    ?block_seq_num,
                    "dropping proposal, transaction validation failed"
                );
                self.metrics.consensus_events.failed_txn_validation += 1;
                return cmds;
            }
            Err(BlockValidationError::RandaoError) => {
                warn!(
                    ?block_round,
                    ?block_round_leader,
                    ?block_seq_num,
                    "dropping proposal, randao validation failed"
                );
                self.metrics
                    .consensus_events
                    .failed_verify_randao_reveal_sig += 1;
                return cmds;
            }
            Err(BlockValidationError::HeaderPayloadMismatchError) => {
                // TODO: this is malicious behaviour?
                warn!(
                    ?block_round,
                    ?block_round_leader,
                    ?block_seq_num,
                    "dropping proposal, header payload mismatch"
                );
                return cmds;
            }
            Err(BlockValidationError::PayloadError) => {
                warn!(
                    ?block_round,
                    ?block_round_leader,
                    ?block_seq_num,
                    "dropping proposal, payload validation failed"
                );
                return cmds;
            }
            Err(BlockValidationError::HeaderError) => {
                warn!(
                    ?block_round,
                    ?block_round_leader,
                    ?block_seq_num,
                    "dropping proposal, header validation failed"
                );
                return cmds;
            }
            Err(BlockValidationError::TimestampError) => {
                warn!(
                    ?block_round,
                    ?block_round_leader,
                    ?block_seq_num,
                    "dropping proposal, timestamp validation failed"
                );
                return cmds;
            }
        };
        let block_round = block.get_round();

        // TODO: ts adjustments are disabled anyways. this needs to be moved to
        // where timestamp validation is done, in block_policy right now.
        // block_policy doesn't have visibility on whether current round is
        // bumped. Deferring the move
        /*
        if let Some(ts_delta) = self
            .block_timestamp
            .valid_block_timestamp(block.get_qc().get_timestamp(), block.get_timestamp())
        {
            // only update timestamp if the block advanced us our round
            if block_round > original_round {
                cmds.push(ConsensusCommand::TimestampUpdate(ts_delta));
            }
        }
        */

        // at this point, block is valid and can be added to the blocktree
        let res_cmds = self.try_add_and_commit_blocktree(block);
        cmds.extend(res_cmds);

        // out-of-order proposals are possible if some round R+1 proposal arrives
        // before R because of network conditions. The proposals are still valid
        if block_round != round {
            debug!(
                expected_round = ?round,
                round = ?block_round,
                "out-of-order proposal"
            );
            self.metrics.consensus_events.out_of_order_proposals += 1;
        }

        cmds
    }

    /// collect votes from other nodes and handle at vote_state state machine
    /// When enough votes are collected, a QC is formed and broadcast to other nodes
    #[must_use]
    pub fn handle_vote_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        vote_msg: VoteMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        debug!(?author, ?vote_msg, "Vote Message");
        if vote_msg.vote.round < self.consensus.pacemaker.get_current_round() {
            self.metrics.consensus_events.old_vote_received += 1;
            return Default::default();
        }
        self.metrics.consensus_events.vote_received += 1;

        let mut cmds = Vec::new();

        let epoch = self
            .epoch_manager
            .get_epoch(vote_msg.vote.round)
            .expect("epoch verified");
        let validator_set = self
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("vote message was verified");
        let validator_mapping = self
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("vote message was verified");
        let (maybe_qc, vote_state_cmds) = self.consensus.vote_state.process_vote(
            &author,
            &vote_msg,
            validator_set,
            validator_mapping,
        );
        cmds.extend(vote_state_cmds.into_iter().map(Into::into));

        if let Some(qc) = maybe_qc {
            debug!(?qc, "Created QC");
            self.metrics.consensus_events.created_qc += 1;

            cmds.extend(self.process_qc(&qc));
            cmds.extend(self.try_propose());
        };

        cmds
    }

    /// handling remote timeout messages from other nodes
    #[must_use]
    pub fn handle_timeout_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        tmo_msg: TimeoutMessage<ST, SCT, EPT>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let tm = &tmo_msg.timeout;
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.consensus.pacemaker.get_current_round() {
            self.metrics.consensus_events.old_remote_timeout += 1;
            return cmds;
        }

        debug!(?author, timeout_msg = ?tm, "Remote timeout msg");
        self.metrics.consensus_events.remote_timeout_msg += 1;

        let epoch = self
            .epoch_manager
            .get_epoch(tm.tminfo.round)
            .expect("epoch verified");
        let validator_set = self
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("timeout message was verified");
        let validator_mapping = self
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("timeout message was verified");

        let process_certificate_cmds = self.process_qc(&tm.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            info!(?last_round_tc, "advance round from remote TC");
            self.metrics.consensus_events.remote_timeout_msg_with_tc += 1;
            let advance_round_cmds = self
                .consensus
                .pacemaker
                .process_certificate(
                    self.metrics,
                    self.epoch_manager,
                    RoundCertificate::Tc(last_round_tc.clone()),
                )
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        self.keypair,
                        self.cert_keypair,
                        self.version,
                        cmd,
                    )
                });
            cmds.extend(advance_round_cmds);
        }

        let remote_timeout_cmds = self
            .consensus
            .pacemaker
            .process_remote_timeout::<VTF::ValidatorSetType>(
                self.metrics,
                self.epoch_manager,
                validator_set,
                validator_mapping,
                &mut self.consensus.safety,
                author,
                tmo_msg,
            );

        cmds.extend(remote_timeout_cmds.into_iter().map(|cmd| {
            ConsensusCommand::from_pacemaker_command(
                self.keypair,
                self.cert_keypair,
                self.version,
                cmd,
            )
        }));
        cmds.extend(self.try_propose());

        cmds
    }

    /// invariant: handle_block_sync must only be passed blocks that were previously requested
    ///
    /// it is possible that a requested block failed to be added to the tree
    /// due to the original proposal arriving before the requested block is returned,
    /// or the requested block is no longer relevant due to prune
    #[must_use]
    pub fn handle_block_sync(
        &mut self,
        block_range: BlockRange,
        full_blocks: Vec<ConsensusFullBlock<ST, SCT, EPT>>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let mut cmds = vec![];

        let record = self
            .consensus
            .block_sync_requests
            .get(&block_range.last_block_id);
        if record.is_none() {
            // this can happen if corresponding proposal is received before blocksync response
            return cmds;
        }
        // TODO fix consensus-state unit tests so that we can enable this assertion
        // assert_eq!(block_range.num_blocks.0, full_blocks.len() as u64);

        for full_block in full_blocks {
            let (header, body) = full_block.split();
            if self
                .consensus
                .pending_block_tree
                .is_valid_to_insert(&header)
            {
                let author_pubkey = self
                    .val_epoch_map
                    .get_cert_pubkeys(&header.epoch)
                    .expect("epoch should be available for blocksync'd block")
                    .map
                    .get(&header.author)
                    .expect("blocksync'd block author should be in validator set");

                let ChainParams {
                    tx_limit,
                    proposal_gas_limit,
                    proposal_byte_limit,
                    vote_pace: _,
                } = self
                    .config
                    .chain_config
                    .get_chain_revision(header.round)
                    .chain_params();

                let ExecutionChainParams { max_code_size } = {
                    // u64::MAX seconds is ~500 Billion years
                    let timestamp_s: u64 = (header.timestamp_ns / 1_000_000_000)
                        .try_into()
                        .expect("blocksync'd block timestamp > ~500B years");
                    self.config
                        .chain_config
                        .get_execution_chain_revision(timestamp_s)
                        .execution_chain_params()
                };

                let block = self
                    .block_validator
                    .validate(
                        header,
                        body,
                        Some(author_pubkey),
                        *tx_limit,
                        *proposal_gas_limit,
                        *proposal_byte_limit,
                        *max_code_size,
                    )
                    .expect("majority extended invalid block");
                let res_cmds = self.try_add_and_commit_blocktree(block);
                cmds.extend(res_cmds);
            }
        }

        self.consensus
            .block_sync_requests
            .remove(&block_range.last_block_id);

        cmds
    }

    #[must_use]
    pub fn handle_vote_timer(
        &mut self,
        round: Round,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let Some(OutgoingVoteStatus::VoteReady(v)) = self.consensus.scheduled_vote else {
            self.consensus.scheduled_vote = Some(OutgoingVoteStatus::TimerFired);
            return vec![];
        };

        self.send_vote_and_reset_timer(round, v)
    }

    #[must_use]
    pub fn send_vote_and_reset_timer(
        &mut self,
        round: Round,
        vote: Vote,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let vote_msg = VoteMessage::<SCT>::new(vote, self.cert_keypair);

        // TODO this grouping should be enforced by epoch_manager/val_epoch_map to be less
        // error-prone
        let (next_round, next_validator_set) = {
            let next_round = round + Round(1);
            let next_epoch = self
                .epoch_manager
                .get_epoch(next_round)
                .expect("higher epoch always exists");
            let Some(next_validator_set) = self.val_epoch_map.get_val_set(&next_epoch) else {
                todo!("handle non-existent validatorset for next round epoch");
            };
            (next_round, next_validator_set.get_members())
        };
        let next_leader = self.election.get_leader(next_round, next_validator_set);
        let msg = ConsensusMessage {
            version: self.version,
            message: ProtocolMessage::Vote(vote_msg),
        }
        .sign(self.keypair);
        let send_cmd = ConsensusCommand::Publish {
            target: RouterTarget::PointToPoint(next_leader),
            message: msg,
        };
        debug!(?round, ?vote, ?next_leader, "created vote");
        self.metrics.consensus_events.created_vote += 1;

        // start the vote-timer for the next round
        let vote_timer_cmd = ConsensusCommand::ScheduleVote {
            duration: self
                .config
                .chain_config
                .get_chain_revision(round + Round(1))
                .chain_params()
                .vote_pace,
            round: round + Round(1),
        };

        self.consensus.scheduled_vote = None;
        vec![send_cmd, vote_timer_cmd]
    }

    /// If the qc has a commit_state_hash, commit the parent block and prune the
    /// block tree
    /// Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    pub fn process_qc(
        &mut self,
        qc: &QuorumCertificate<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        trace!(?qc, our_high_qc = ?self.consensus.get_high_qc(), "process qc");

        if Rank(qc.info) <= Rank(self.consensus.get_high_qc().info) {
            self.metrics.consensus_events.process_old_qc += 1;
            return Vec::new();
        }
        self.metrics.consensus_events.process_qc += 1;

        let mut cmds = Vec::new();
        cmds.extend(self.try_commit(qc));

        // cancel any obsolete blocksync requests
        let to_cancel: Vec<_> = self
            .consensus
            .block_sync_requests
            .iter()
            .filter_map(|(block_id, status)| {
                if status.cancel_round <= self.consensus.pending_block_tree.root().round {
                    Some(*block_id)
                } else {
                    None
                }
            })
            .collect();
        for block_id in &to_cancel {
            let canceled_request = self.consensus.block_sync_requests.remove(block_id).unwrap();
            cmds.push(ConsensusCommand::CancelSync(canceled_request.range));
        }

        // statesync if too far from tip
        cmds.extend(self.maybe_statesync());

        cmds.extend(
            self.consensus
                .pacemaker
                .process_certificate(
                    self.metrics,
                    self.epoch_manager,
                    RoundCertificate::Qc(qc.clone()),
                )
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        self.keypair,
                        self.cert_keypair,
                        self.version,
                        cmd,
                    )
                }),
        );

        // retrieve missing blocks if processed QC is highest and points at an
        // unknown block
        cmds.extend(self.request_blocks_if_missing_ancestor());

        // update vote_state round
        // it's ok if not leader for round; we will never propose
        self.consensus
            .vote_state
            .start_new_round(self.consensus.pacemaker.get_current_round());

        cmds
    }

    #[must_use]
    pub fn checkpoint(&self) -> Checkpoint<ST, SCT, EPT> {
        let val_set_data = |epoch: Epoch| {
            // return early if validator set isn't locked
            let _ = self.val_epoch_map.get_val_set(&epoch)?;

            let round = self.epoch_manager.epoch_starts.get(&epoch).copied();
            Some(LockedEpoch { epoch, round })
        };

        let base_epoch = self.consensus.pending_block_tree.root().epoch;
        Checkpoint {
            root: self.consensus.pending_block_tree.root().block_id,
            high_certificate: self.consensus.pacemaker.high_certificate().clone(),
            high_qc: self.consensus.get_high_qc().clone(),
            validator_sets: vec![
                val_set_data(base_epoch)
                    .expect("checkpoint: no validator set populated for base_epoch"),
                val_set_data(base_epoch + Epoch(1))
                    .expect("checkpoint: no validator set populated for base_epoch + 1"),
            ]
            .into_iter()
            .chain(
                // third val_set might not be ready
                val_set_data(base_epoch + Epoch(2)),
            )
            .collect(),
        }
    }

    /// Try commit blocks using the QC. Committing the boundary block can
    /// schedule the next epoch and bump pacemaker epoch. Call
    /// `Pacemaker::advance_epoch` to keep pacemaker in sync
    #[must_use]
    fn try_commit(
        &mut self,
        qc: &QuorumCertificate<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let mut cmds = Vec::new();
        debug!(?qc, "try committing blocks using qc");

        let Some(qc_parent) = self
            .consensus
            .pending_block_tree
            .get_block(&qc.get_block_id())
        else {
            // the block that the qc points to doesn't exist
            // or parent block is root, which
            return cmds;
        };

        let Some(committable_block_id) = qc.get_committable_id(qc_parent.header()) else {
            // qc is not committable (not consecutive rounds)
            return cmds;
        };

        if !self
            .consensus
            .pending_block_tree
            .is_coherent(&committable_block_id)
        {
            // the committable block is not (yet) coherent
            // likely because execution is lagging
            return cmds;
        }

        let blocks_to_commit = self
            .consensus
            .pending_block_tree
            .prune(&committable_block_id);

        for block in blocks_to_commit.iter() {
            debug!(
                seq_num =? block.header().seq_num,
                round =? block.header().round,
                "committing block"
            );
            // when epoch boundary block is committed, this updates
            // epoch manager records
            self.metrics.consensus_events.commit_block += 1;
            self.block_policy.update_committed_block(block);
            self.epoch_manager
                .schedule_epoch_start(block.header().seq_num, block.get_round());

            cmds.push(ConsensusCommand::CommitBlocks(
                OptimisticPolicyCommit::Finalized(block.to_owned()),
            ));
        }
        cmds
    }

    fn try_add_and_commit_blocktree(
        &mut self,
        block: BPT::ValidatedBlock,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        trace!(?block, "adding block to blocktree");
        let mut cmds = Vec::new();
        self.consensus.pending_block_tree.add(block.clone());

        cmds.extend(self.try_update_coherency(block.get_id()));

        cmds.extend(self.try_vote(&block));
        cmds.extend(self.try_propose());

        // statesync if too far from tip
        cmds.extend(self.maybe_statesync());

        cmds.extend(self.request_blocks_if_missing_ancestor());

        cmds
    }

    #[must_use]
    fn try_update_coherency(
        &mut self,
        updated_block_id: BlockId,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let mut cmds = Vec::new();
        for newly_coherent_block in self.consensus.pending_block_tree.try_update_coherency(
            self.metrics,
            updated_block_id,
            self.block_policy,
            self.state_backend,
        ) {
            // optimistically commit any block that has been added to the blocktree and is coherent
            cmds.push(ConsensusCommand::CommitBlocks(
                OptimisticPolicyCommit::Proposed(newly_coherent_block.to_owned()),
            ));
        }

        let high_commit_qc = self.consensus.pending_block_tree.get_high_committable_qc();
        if let Some(qc) = high_commit_qc {
            cmds.extend(self.try_commit(&qc));
        }

        cmds
    }

    #[must_use]
    fn maybe_statesync(&mut self) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let Some(high_qc_seq_num) = self
            .consensus
            .pending_block_tree
            .get_seq_num_of_qc(self.consensus.get_high_qc())
        else {
            return Vec::new();
        };

        if self.consensus.pending_block_tree.get_root_seq_num()
            + self.config.live_to_statesync_threshold
            > high_qc_seq_num
        {
            Vec::new()
        } else {
            panic!("high qc too far ahead of block tree root, restart client and statesync. highqc: {:?}, block-tree root {:?}", high_qc_seq_num, self.consensus.pending_block_tree.get_root_seq_num());
        }
    }

    #[must_use]
    fn try_vote(
        &mut self,
        validated_block: &BPT::ValidatedBlock,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let mut cmds = Vec::new();
        let round = validated_block.get_round();

        // only vote for proposals in the current round
        if round != self.consensus.pacemaker.get_current_round() {
            return cmds;
        }

        let Some(parent_timestamp) = self
            .consensus
            .blocktree()
            .get_timestamp_of_qc(validated_block.get_qc())
        else {
            warn!("dropping proposal, no parent timestamp");
            return cmds;
        };

        // verify timestamp here
        if self
            .block_timestamp
            .valid_block_timestamp(
                parent_timestamp,
                validated_block.get_timestamp(),
                self.config
                    .chain_config
                    .get_chain_revision(round)
                    .chain_params()
                    .vote_pace
                    .as_nanos(),
            )
            .is_none()
        {
            self.metrics.consensus_events.failed_ts_validation += 1;
            warn!(
                prev_block_ts = ?parent_timestamp,
                curr_block_ts = ?validated_block.get_timestamp(),
                local_ts = ?self.block_timestamp.get_current_time(),
                "Timestamp validation failed"
            );
            return cmds;
        }

        // check that the block is coherent
        if !self
            .consensus
            .pending_block_tree
            .is_coherent(&validated_block.get_id())
        {
            warn!("dropping proposal, is not coherent");
            return cmds;
        }

        debug!(?round, block_id = ?validated_block.get_id(), "try vote");

        if !self.consensus.safety.is_safe_to_vote(round) {
            // we've already voted or timed out this round
            return cmds;
        }
        self.consensus.safety.vote(round);

        let v = Vote {
            id: validated_block.get_id(),
            round,
            epoch: validated_block.get_epoch(),
        };

        match self.consensus.scheduled_vote {
            Some(OutgoingVoteStatus::TimerFired) => {
                // timer already fired for this round so send vote immediately
                let vote_cmd = self.send_vote_and_reset_timer(round, v);
                cmds.extend(vote_cmd);
            }
            Some(OutgoingVoteStatus::VoteReady(r)) if r.round >= v.round => {
                panic!("trying to schedule another vote in same round. scheduled vote {:?}, new vote {:?}", r, v);
            }
            Some(OutgoingVoteStatus::VoteReady(_)) | None => {
                if matches!(
                    self.consensus.scheduled_vote,
                    Some(OutgoingVoteStatus::VoteReady(_))
                ) {
                    warn!(
                        scheduled_vote = ?self.consensus.scheduled_vote,
                        new_vote = ?v,
                        "network has different vote/proposal pacing than us"
                    );
                }

                // if this is the next round after a timeout, we should vote immediately
                // otherwise, schedule the vote for later
                if self.consensus.pacemaker.last_round_tc().is_some() {
                    let vote_cmd = self.send_vote_and_reset_timer(round, v);
                    cmds.extend(vote_cmd);
                } else {
                    self.consensus.scheduled_vote = Some(OutgoingVoteStatus::VoteReady(v));
                }
            }
        }

        cmds
    }

    #[must_use]
    fn try_propose(&mut self) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let mut cmds = Vec::new();

        let (round, validator_set) = {
            let round = self.consensus.pacemaker.get_current_round();
            // TODO this grouping should be enforced by epoch_manager/val_epoch_map to be less
            // error-prone
            let epoch = self
                .epoch_manager
                .get_epoch(round)
                .expect("current epoch exists");
            let Some(validator_set) = self.val_epoch_map.get_val_set(&epoch) else {
                todo!("handle non-existent validatorset for next round epoch");
            };
            (round, validator_set)
        };

        let leader = &self.election.get_leader(round, validator_set.get_members());
        trace!(?round, ?leader, "try propose");

        // check that self is leader
        if self.nodeid != leader {
            return cmds;
        }

        // check that we have path to root and block is coherent
        cmds.extend(self.try_update_coherency(self.consensus.get_high_qc().get_block_id()));
        if !self
            .consensus
            .pending_block_tree
            .is_coherent(&self.consensus.get_high_qc().get_block_id())
        {
            return cmds;
        }

        if !self.consensus.safety.is_safe_to_propose(round) {
            return cmds;
        }
        self.consensus.safety.propose(round);

        let last_round_tc = self.consensus.pacemaker.last_round_tc().cloned();
        cmds.extend(self.process_new_round_event(last_round_tc));
        cmds
    }

    /// called when the node is entering a new round and is the leader for that round
    /// TODO this function can be folded into try_propose; it's only called there
    #[must_use]
    fn process_new_round_event(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let node_id = *self.nodeid;
        let round = self.consensus.pacemaker.get_current_round();
        let epoch = self
            .epoch_manager
            .get_epoch(round)
            .expect("higher epoch exists");
        let high_qc = self.consensus.get_high_qc().clone();

        let parent_bid = high_qc.get_block_id();
        let round_signature = RoundSignature::new(round, self.cert_keypair);

        // Propose when there's a path to root
        let pending_blocktree_blocks = self
            .consensus
            .pending_block_tree
            .get_blocks_on_path_from_root(&parent_bid)
            .expect("there should be a path to root");

        // building a proposal off the pending branch or against the root of the blocktree if
        // there is no branch
        let (try_propose_seq_num, timestamp_ns) =
            if let Some(extending_block) = pending_blocktree_blocks.last() {
                (
                    extending_block.get_seq_num() + SeqNum(1),
                    self.block_timestamp
                        .get_valid_block_timestamp(extending_block.get_timestamp()),
                )
            } else {
                (
                    self.consensus.pending_block_tree.root().seq_num + SeqNum(1),
                    self.block_timestamp.get_valid_block_timestamp(
                        self.consensus.pending_block_tree.root().timestamp_ns,
                    ),
                )
            };

        let Ok(delayed_execution_results) = self.block_policy.get_expected_execution_results(
            try_propose_seq_num,
            pending_blocktree_blocks.clone(),
            self.state_backend,
        ) else {
            warn!(
                ?node_id,
                ?round,
                ?high_qc,
                ?try_propose_seq_num,
                ?last_round_tc,
                "no eth_header found, can't propose"
            );
            self.metrics.consensus_events.rx_execution_lagging += 1;
            return Vec::new();
        };
        let _create_proposal_span = tracing::info_span!("create_proposal_span", ?round).entered();

        debug!(
            ?node_id,
            ?round,
            ?high_qc,
            ?try_propose_seq_num,
            ?last_round_tc,
            "emitting create proposal command to txpool"
        );

        vec![ConsensusCommand::CreateProposal {
            epoch,
            round,
            seq_num: try_propose_seq_num,
            high_qc,
            round_signature,
            last_round_tc,

            tx_limit: self
                .config
                .chain_config
                .get_chain_revision(round)
                .chain_params()
                .tx_limit,
            proposal_gas_limit: self
                .config
                .chain_config
                .get_chain_revision(round)
                .chain_params()
                .proposal_gas_limit,
            proposal_byte_limit: self
                .config
                .chain_config
                .get_chain_revision(round)
                .chain_params()
                .proposal_byte_limit,

            beneficiary: *self.beneficiary,
            timestamp_ns,
            extending_blocks: pending_blocktree_blocks.into_iter().cloned().collect(),
            delayed_execution_results,
        }]
    }

    #[must_use]
    fn request_blocks_if_missing_ancestor(
        &mut self,
    ) -> Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>> {
        let high_qc = self.consensus.get_high_qc();
        let root_seq_num = self.consensus.pending_block_tree.get_root_seq_num();

        let Some(range) = self
            .consensus
            .pending_block_tree
            .maybe_fill_path_to_root(high_qc)
        else {
            return Vec::new();
        };
        assert_ne!(range.num_blocks, SeqNum(0));

        if self
            .consensus
            .block_sync_requests
            .contains_key(&range.last_block_id)
        {
            return Vec::new();
        }

        if root_seq_num + self.config.live_to_statesync_threshold <= range.num_blocks {
            // crash the client to move into statesync mode and recover
            panic!(
                "blocksync request range is outside statesync threshold, range: {:?}",
                range
            );
        }

        debug!(?range, "consensus blocksyncing blocks up to root");

        self.consensus.block_sync_requests.insert(
            range.last_block_id,
            BlockSyncRequestStatus {
                range,
                // the round of last_block_id would be more precise, but it doesn't matter
                // because this is just used for garbage collection.
                cancel_round: high_qc.get_round(),
            },
        );

        vec![ConsensusCommand::RequestSync(range)]
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, ops::Deref, time::Duration};

    use alloy_consensus::{
        constants::{EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        Header, TxEnvelope, EMPTY_OMMER_ROOT_HASH,
    };
    use itertools::Itertools;
    use monad_chain_config::{
        revision::{ChainParams, MockChainRevision},
        MockChainConfig,
    };
    use monad_consensus::{
        messages::{
            consensus_message::ProtocolMessage,
            message::{ProposalMessage, TimeoutMessage, VoteMessage},
        },
        pacemaker::PacemakerCommand,
        validation::signing::Verified,
    };
    use monad_consensus_types::{
        block::{
            BlockPolicy, BlockRange, ConsensusBlockHeader, ConsensusFullBlock,
            OptimisticPolicyCommit, GENESIS_TIMESTAMP,
        },
        block_validator::BlockValidator,
        checkpoint::RootInfo,
        metrics::Metrics,
        payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        timeout::Timeout,
        voting::{ValidatorMapping, Vote},
        RoundCertificate,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
        },
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_block_validator::EthValidator;
    use monad_eth_types::{
        Balance, EthBlockBody, EthExecutionProtocol, EthHeader, ProposedEthHeader, BASE_FEE_PER_GAS,
    };
    use monad_multi_sig::MultiSig;
    use monad_state_backend::{InMemoryState, InMemoryStateInner, StateBackend, StateBackendTest};
    use monad_testutil::{
        proposal::ProposalGen,
        signing::{create_certificate_keys, create_keys, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{
        BlockId, Epoch, ExecutionProtocol, NodeId, Round, RouterTarget, SeqNum, Stake,
        GENESIS_SEQ_NUM,
    };
    use monad_validator::{
        epoch_manager::EpochManager,
        leader_election::LeaderElection,
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use test_case::test_case;
    use tracing_test::traced_test;

    use crate::{
        timestamp::BlockTimestamp, ConsensusCommand, ConsensusConfig, ConsensusState,
        ConsensusStateWrapper, OutgoingVoteStatus,
    };

    const BASE_FEE: u128 = BASE_FEE_PER_GAS as u128;
    const GAS_LIMIT: u64 = 30000;

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        vote_pace: Duration::from_millis(1000),
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type BlockPolicyType = EthBlockPolicy<SignatureType, SignatureCollectionType>;
    type StateBackendType = InMemoryState;
    type BlockValidatorType =
        EthValidator<SignatureType, SignatureCollectionType, StateBackendType>;

    struct NodeContext<ST, SCT, BPT, SBT, BVT, VTF, LT>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT>,
        SBT: StateBackend + StateBackendTest,
        BVT: BlockValidator<ST, SCT, EthExecutionProtocol, BPT, SBT>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        consensus_state: ConsensusState<
            ST,
            SCT,
            EthExecutionProtocol,
            BPT,
            SBT,
            MockChainConfig,
            MockChainRevision,
        >,

        metrics: Metrics,
        epoch_manager: EpochManager,

        val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
        election: LT,
        version: u32,

        block_validator: BVT,
        block_policy: BPT,
        state_backend: SBT,
        block_timestamp: BlockTimestamp,
        beneficiary: [u8; 20],
        nodeid: NodeId<CertificateSignaturePubKey<ST>>,
        consensus_config: ConsensusConfig<MockChainConfig, MockChainRevision>,

        keypair: ST::KeyPairType,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
    }

    impl<ST, SCT, BPT, SBT, BVT, VTF, LT> NodeContext<ST, SCT, BPT, SBT, BVT, VTF, LT>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT>,
        SBT: StateBackend + StateBackendTest,
        BVT: BlockValidator<ST, SCT, EthExecutionProtocol, BPT, SBT>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        fn wrapped_state(
            &mut self,
        ) -> ConsensusStateWrapper<
            ST,
            SCT,
            EthExecutionProtocol,
            BPT,
            SBT,
            VTF,
            LT,
            BVT,
            MockChainConfig,
            MockChainRevision,
        > {
            ConsensusStateWrapper {
                consensus: &mut self.consensus_state,

                metrics: &mut self.metrics,
                epoch_manager: &mut self.epoch_manager,

                val_epoch_map: &self.val_epoch_map,
                election: &self.election,
                version: self.version,

                block_validator: &self.block_validator,
                block_policy: &mut self.block_policy,
                state_backend: &self.state_backend,
                block_timestamp: &mut self.block_timestamp,
                beneficiary: &self.beneficiary,
                nodeid: &self.nodeid,
                config: &self.consensus_config,

                keypair: &self.keypair,
                cert_keypair: &self.cert_keypair,
            }
        }

        fn handle_proposal_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: ProposalMessage<ST, SCT, EthExecutionProtocol>,
        ) -> Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>> {
            self.wrapped_state().handle_proposal_message(author, p)
        }

        fn handle_timeout_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: TimeoutMessage<ST, SCT, EthExecutionProtocol>,
        ) -> Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>> {
            self.wrapped_state().handle_timeout_message(author, p)
        }

        fn handle_vote_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: VoteMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>> {
            self.wrapped_state().handle_vote_message(author, p)
        }

        fn handle_block_sync(
            &mut self,
            block_range: BlockRange,
            full_blocks: Vec<ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
        ) -> Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>> {
            self.wrapped_state()
                .handle_block_sync(block_range, full_blocks)
        }

        fn ledger_commit(&mut self, block: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>) {
            self.state_backend.ledger_commit(&block.get_id());
        }

        fn ledger_propose(&mut self, block: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>) {
            self.state_backend.ledger_propose(
                block.get_id(),
                block.seq_num,
                block.round,
                block.qc.get_round(),
                BTreeMap::default(),
            );
        }
    }

    struct EnvContext<ST, SCT, VTF, LT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        proposal_gen: ProposalGen<ST, SCT, EthExecutionProtocol>,
        /// malicious_proposal_gen starts with a different timestamp
        malicious_proposal_gen: ProposalGen<ST, SCT, EthExecutionProtocol>,
        keys: Vec<ST::KeyPairType>,
        cert_keys: Vec<SignatureCollectionKeyPairType<SCT>>,
        epoch_manager: EpochManager,
        val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
        election: LT,
    }

    impl<ST, SCT, VTF, LT> EnvContext<ST, SCT, VTF, LT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        /// a proposal with no execution results
        /// if execution_delay == MAX, this can always be used instead of next_proposal
        fn next_proposal_empty(
            &mut self,
        ) -> Verified<ST, ProposalMessage<ST, SCT, EthExecutionProtocol>> {
            self.proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                |seq_num, timestamp_ns, round_signature: RoundSignature<_>| ProposedEthHeader {
                    transactions_root: *EMPTY_TRANSACTIONS,
                    ommers_hash: *EMPTY_OMMER_ROOT_HASH,
                    withdrawals_root: *EMPTY_WITHDRAWALS,
                    beneficiary: Default::default(),
                    difficulty: 0,
                    number: seq_num.0,
                    gas_limit: CHAIN_PARAMS.proposal_gas_limit,
                    timestamp: (timestamp_ns / 1_000_000_000) as u64,
                    mix_hash: round_signature.get_hash().0,
                    nonce: [0_u8; 8],
                    extra_data: [0_u8; 32],
                    base_fee_per_gas: BASE_FEE_PER_GAS,
                    blob_gas_used: 0,
                    excess_blob_gas: 0,
                    parent_beacon_block_root: [0_u8; 32],
                },
                Vec::new(),
            )
        }

        fn next_proposal(
            &mut self,
            delayed_execution_results: Vec<EthHeader>,
        ) -> Verified<ST, ProposalMessage<ST, SCT, EthExecutionProtocol>> {
            self.proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                |seq_num, timestamp_ns, round_signature| ProposedEthHeader {
                    transactions_root: *EMPTY_TRANSACTIONS,
                    ommers_hash: *EMPTY_OMMER_ROOT_HASH,
                    withdrawals_root: *EMPTY_WITHDRAWALS,
                    beneficiary: Default::default(),
                    difficulty: 0,
                    number: seq_num.0,
                    gas_limit: CHAIN_PARAMS.proposal_gas_limit,
                    timestamp: (timestamp_ns / 1_000_000_000) as u64,
                    mix_hash: round_signature.get_hash().0,
                    nonce: [0_u8; 8],
                    extra_data: [0_u8; 32],
                    base_fee_per_gas: BASE_FEE_PER_GAS,
                    blob_gas_used: 0,
                    excess_blob_gas: 0,
                    parent_beacon_block_root: [0_u8; 32],
                },
                delayed_execution_results,
            )
        }

        // TODO come up with better API for making mal proposals relative to state of proposal_gen
        fn branch_proposal(
            &mut self,
            delayed_execution_results: Vec<EthHeader>,
        ) -> Verified<ST, ProposalMessage<ST, SCT, EthExecutionProtocol>> {
            self.malicious_proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                |seq_num, timestamp_ns, round_signature| ProposedEthHeader {
                    transactions_root: *EMPTY_TRANSACTIONS,
                    ommers_hash: *EMPTY_OMMER_ROOT_HASH,
                    withdrawals_root: *EMPTY_WITHDRAWALS,
                    beneficiary: Default::default(),
                    difficulty: 0,
                    number: seq_num.0,
                    gas_limit: CHAIN_PARAMS.proposal_gas_limit,
                    timestamp: (timestamp_ns / 1_000_000_000) as u64,
                    mix_hash: round_signature.get_hash().0,
                    nonce: [0_u8; 8],
                    extra_data: [0_u8; 32],
                    base_fee_per_gas: BASE_FEE_PER_GAS,
                    blob_gas_used: 0,
                    excess_blob_gas: 0,
                    parent_beacon_block_root: [0_u8; 32],
                },
                delayed_execution_results,
            )
        }

        fn next_tc(
            &mut self,
            epoch: Epoch,
        ) -> Vec<Verified<ST, TimeoutMessage<ST, SCT, EthExecutionProtocol>>> {
            let valset = self.val_epoch_map.get_val_set(&epoch).unwrap();
            let val_cert_pubkeys = self.val_epoch_map.get_cert_pubkeys(&epoch).unwrap();
            self.proposal_gen.next_tc(
                &self.keys,
                &self.cert_keys,
                valset,
                &self.epoch_manager,
                val_cert_pubkeys,
            )
        }
    }

    fn setup<
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT>,
        SBT: StateBackend + StateBackendTest,
        BVT: BlockValidator<ST, SCT, EthExecutionProtocol, BPT, SBT>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    >(
        num_states: u32,
        valset_factory: VTF,
        election: LT,
        block_policy: impl Fn() -> BPT,
        state_backend: impl Fn() -> SBT,
        block_validator: impl Fn() -> BVT,
        execution_delay: SeqNum,
    ) -> (
        EnvContext<ST, SCT, VTF, LT>,
        Vec<NodeContext<ST, SCT, BPT, SBT, BVT, VTF, LT>>,
    ) {
        let (keys, cert_keys, valset, _valmap) =
            create_keys_w_validators::<ST, SCT, _>(num_states, ValidatorSetFactory::default());
        let val_stakes = Vec::from_iter(valset.get_members().clone());
        let val_cert_pubkeys = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(cert_keys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();
        let mut dupkeys = create_keys::<ST>(num_states);
        let mut dupcertkeys = create_certificate_keys::<SCT>(num_states);

        let ctxs: Vec<NodeContext<ST, SCT, BPT, SBT, BVT, _, _>> = (0..num_states)
            .map(|i| {
                let mut val_epoch_map = ValidatorsEpochMapping::new(valset_factory.clone());
                val_epoch_map.insert(
                    Epoch(1),
                    val_stakes.clone(),
                    ValidatorMapping::new(val_cert_pubkeys.clone()),
                );
                val_epoch_map.insert(
                    Epoch(2),
                    val_stakes.clone(),
                    ValidatorMapping::new(val_cert_pubkeys.clone()),
                );
                let epoch_manager =
                    EpochManager::new(SeqNum(100), Round(20), &[(Epoch(1), Round(0))]);

                let default_key =
                    <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut [127; 32]).unwrap();
                let default_cert_key =
                    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(
                        &mut [127; 32],
                    )
                    .unwrap();
                let consensus_config = ConsensusConfig {
                    execution_delay,
                    delta: Duration::from_secs(1),
                    statesync_to_live_threshold: SeqNum(600),
                    live_to_statesync_threshold: SeqNum(900),
                    start_execution_threshold: SeqNum(300),
                    chain_config: MockChainConfig::new(&CHAIN_PARAMS),
                    timestamp_latency_estimate_ns: 1,
                    _phantom: Default::default(),
                };
                let genesis_qc = QuorumCertificate::genesis_qc();
                let cs = ConsensusState::new(
                    &epoch_manager,
                    &consensus_config,
                    RootInfo {
                        round: genesis_qc.get_round(),
                        seq_num: GENESIS_SEQ_NUM,
                        epoch: genesis_qc.get_epoch(),
                        block_id: genesis_qc.get_block_id(),
                        timestamp_ns: GENESIS_TIMESTAMP,
                    },
                    RoundCertificate::Qc(genesis_qc.clone()),
                    genesis_qc,
                );

                NodeContext {
                    consensus_state: cs,

                    metrics: Metrics::default(),
                    epoch_manager,

                    val_epoch_map,
                    election: election.clone(),
                    version: 0,

                    block_validator: block_validator(),
                    block_policy: block_policy(),
                    state_backend: state_backend(),
                    block_timestamp: BlockTimestamp::new(1000, 1),
                    beneficiary: Default::default(),
                    nodeid: NodeId::new(keys[i as usize].pubkey()),
                    consensus_config,

                    keypair: std::mem::replace(&mut dupkeys[i as usize], default_key),
                    cert_keypair: std::mem::replace(&mut dupcertkeys[i as usize], default_cert_key),
                }
            })
            .collect();

        let mut val_epoch_map = ValidatorsEpochMapping::new(valset_factory);
        val_epoch_map.insert(
            Epoch(1),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        val_epoch_map.insert(
            Epoch(2),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );
        let epoch_manager = EpochManager::new(SeqNum(100), Round(20), &[(Epoch(1), Round(0))]);

        let env: EnvContext<ST, SCT, VTF, LT> = EnvContext {
            proposal_gen: ProposalGen::new(),
            malicious_proposal_gen: ProposalGen::new().with_timestamp(100),
            keys,
            cert_keys,
            epoch_manager,
            val_epoch_map,
            election,
        };

        (env, ctxs)
    }

    fn generate_block_body(eth_tx_list: Vec<TxEnvelope>) -> EthBlockBody {
        EthBlockBody {
            transactions: eth_tx_list,
            ommers: Vec::new(),
            withdrawals: Vec::new(),
        }
    }

    fn extract_vote_msgs<ST, SCT, BPT, SBT>(
        cmds: Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>>,
    ) -> Vec<VoteMessage<SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT>,
        SBT: StateBackend,
    {
        cmds.into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message,
                } => match message.deref().deref().message {
                    ProtocolMessage::Vote(vote) => Some(vote),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn extract_schedule_vote_timer<ST, SCT, BPT, SBT>(
        cmds: Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>>,
    ) -> Vec<Round>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT>,
        SBT: StateBackend,
    {
        cmds.into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::ScheduleVote { duration: _, round } => Some(round),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn extract_create_proposal_command_round<ST, SCT, BPT, SBT>(
        cmds: Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>>,
    ) -> Round
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT> + std::fmt::Debug,
        SBT: StateBackend + std::fmt::Debug,
    {
        cmds.iter()
            .find_map(|c| match c {
                ConsensusCommand::CreateProposal { round, .. } => Some(*round),
                _ => None,
            })
            .unwrap_or_else(|| panic!("couldn't extract proposal: {:?}", cmds))
    }

    fn extract_blocksync_requests<ST, SCT, BPT, SBT>(
        cmds: Vec<ConsensusCommand<ST, SCT, EthExecutionProtocol, BPT, SBT>>,
    ) -> Vec<BlockRange>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<ST, SCT, EthExecutionProtocol, SBT>,
        SBT: StateBackend,
    {
        cmds.into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync(block_range) => Some(block_range),
                _ => None,
            })
            .collect()
    }

    fn find_proposal_broadcast<ST, SCT, EPT, BPT, SBT>(
        cmds: Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>>,
    ) -> Option<ProposalMessage<ST, SCT, EPT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        cmds.iter().find_map(|c| match c {
            ConsensusCommand::Publish {
                target: RouterTarget::Raptorcast(_),
                message,
            } => match &message.deref().deref().message {
                ProtocolMessage::Proposal(p) => Some(p.clone()),
                _ => None,
            },
            _ => None,
        })
    }

    fn find_vote_message<ST, SCT, EPT, BPT, SBT>(
        cmds: &[ConsensusCommand<ST, SCT, EPT, BPT, SBT>],
    ) -> Option<&ConsensusCommand<ST, SCT, EPT, BPT, SBT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        cmds.iter().find(|c| match c {
            ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(..),
                message,
            } => matches!(&message.deref().deref().message, ProtocolMessage::Vote(..)),
            _ => false,
        })
    }

    fn extract_proposal_commit_rounds<ST, SCT, EPT, BPT, SBT>(
        cmds: Vec<ConsensusCommand<ST, SCT, EPT, BPT, SBT>>,
    ) -> Vec<Round>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        cmds.iter()
            .filter_map(|c| match c {
                ConsensusCommand::CommitBlocks(OptimisticPolicyCommit::Proposed(block)) => {
                    Some(block.get_round())
                }
                _ => None,
            })
            .collect()
    }

    fn find_blocksync_request<ST, SCT, EPT, BPT, SBT>(
        cmds: &[ConsensusCommand<ST, SCT, EPT, BPT, SBT>],
    ) -> Option<&ConsensusCommand<ST, SCT, EPT, BPT, SBT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        cmds.iter()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. }))
    }

    fn find_commit_cmds<ST, SCT, EPT, BPT, SBT>(
        cmds: &[ConsensusCommand<ST, SCT, EPT, BPT, SBT>],
    ) -> Vec<BPT::ValidatedBlock>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        cmds.iter()
            .filter_map(|c| match c {
                ConsensusCommand::CommitBlocks(OptimisticPolicyCommit::Finalized(committed)) => {
                    Some(committed.to_owned())
                }
                _ => None,
            })
            .collect()
    }

    fn find_timestamp_update_cmd<ST, SCT, EPT, BPT, SBT>(
        cmds: &[ConsensusCommand<ST, SCT, EPT, BPT, SBT>],
    ) -> Option<&ConsensusCommand<ST, SCT, EPT, BPT, SBT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        cmds.iter()
            .find(|c| matches!(c, ConsensusCommand::TimestampUpdate(_)))
    }

    // genesis_qc start with "0" sequence number and Round(0)
    // hence round == seqnum if no round times out
    fn seqnum_to_round_no_tc(seq_num: SeqNum) -> Round {
        Round(seq_num.0)
    }

    // 2f+1 votes for a Vote leads to a QC locking -- ie, high_qc is set to that QC.
    #[traced_test]
    #[test]
    fn lock_qc_high() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let mut wrapped_state = ctx[0].wrapped_state();
        assert_eq!(wrapped_state.consensus.get_high_qc().get_round(), Round(0));

        let expected_qc_high_round = Round(5);

        let v = Vote {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: expected_qc_high_round,
        };

        let vm1 = VoteMessage::<SignatureCollectionType>::new(v, &env.cert_keys[1]);
        let vm2 = VoteMessage::<SignatureCollectionType>::new(v, &env.cert_keys[2]);
        let vm3 = VoteMessage::<SignatureCollectionType>::new(v, &env.cert_keys[3]);

        let v1 = Verified::<SignatureType, _>::new(vm1, &env.keys[1]);
        let v2 = Verified::<SignatureType, _>::new(vm2, &env.keys[2]);
        let v3 = Verified::<SignatureType, _>::new(vm3, &env.keys[3]);

        let _ = wrapped_state.handle_vote_message(*v1.author(), *v1);
        let _ = wrapped_state.handle_vote_message(*v2.author(), *v2);

        // less than 2f+1, so expect not locked
        assert_eq!(wrapped_state.consensus.get_high_qc().get_round(), Round(0));

        let _ = wrapped_state.handle_vote_message(*v3.author(), *v3);
        assert_eq!(
            wrapped_state.consensus.get_high_qc().get_round(),
            expected_qc_high_round
        );
        assert_eq!(wrapped_state.metrics.consensus_events.vote_received, 3);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();
        let p1 = env.next_proposal(Vec::new());

        // local timeout for state in Round 1
        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        let _ = wrapped_state
            .consensus
            .pacemaker
            .handle_event(&mut wrapped_state.consensus.safety);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

        let result = extract_vote_msgs(cmds);
        assert!(result.is_empty());
    }

    #[test]
    fn enter_proposalmsg_round() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );

        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(2)
        );
        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(2))
        );

        for _ in 0..4 {
            env.next_proposal_empty();
        }
        let p7 = env.next_proposal_empty();
        let (author, _, verified_message) = p7.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(7)
        );
    }

    #[test]
    fn scheduled_vote_round() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);
        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );
    }

    #[ignore]
    #[test]
    fn timestamp_update_only_for_higher_round() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // our initial starting logic has consensus in round 1 so the first proposal does not
        // increase the round
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _cmds = wrapped_state.handle_proposal_message(author, verified_message);

        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message.clone());
        assert!(find_timestamp_update_cmd(&cmds).is_some());

        // send same proposal again -- its valid but won't increase round so should not produce a
        // timestamp delta
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        assert!(find_timestamp_update_cmd(&cmds).is_none());

        for _ in 0..4 {
            env.next_proposal_empty();
        }
        let p7 = env.next_proposal_empty();
        let (author, _, verified_message) = p7.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        assert!(find_timestamp_update_cmd(&cmds).is_some());
    }

    #[test]
    fn test_out_of_order_proposals() {
        // Permutation of 1 message isn't interesting to test. Plus p4 can
        // commit p2 before p3 is received
        //
        // 2..=5 enumerates all possible leader cases as we have 4 validators
        // in a round-robin
        for n in 2..=5 {
            let perms = (0..n).permutations(n).collect::<Vec<_>>();
            for perm in perms {
                out_of_order_proposals(perm);
            }
        }
    }

    fn out_of_order_proposals(perms: Vec<usize>) {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // first proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );

        // second proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(2)
        );
        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(2))
        );

        let mut missing_proposals = Vec::new();
        for _ in 0..perms.len() {
            missing_proposals.push(env.next_proposal_empty());
        }

        // last proposal arrvies
        let p_fut = env.next_proposal_empty();
        let (author, _, verified_message) = p_fut.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        // was in Round(2) and skipped over perms.len() proposals. Handling
        // p_fut should be at Round(3+perms.len())
        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(3 + perms.len() as u64)
        );
        assert_eq!(wrapped_state.consensus.pending_block_tree.size(), 3);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(wrapped_state.handle_proposal_message(author, verified_message));
        }

        // next proposal will trigger everything to be committed if there is
        // a consecutive chain as expected
        // last proposal arrvies
        let p_to_validate_blocks = env.next_proposal_empty();
        let (author, _, verified_message) = p_to_validate_blocks.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(wrapped_state.consensus.pending_block_tree.size(), 2);
        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(perms.len() as u64 + 4),
            "order of proposals {:?}",
            perms
        );
    }

    #[test]
    fn test_out_of_order_optimistic_commit() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();
        // first proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );

        let rounds = extract_proposal_commit_rounds(cmds);
        assert_eq!(rounds, vec![Round(1)]);

        // second proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(2))
        );

        let rounds = extract_proposal_commit_rounds(cmds);
        assert_eq!(rounds, vec![Round(2)]);

        let mut missing_proposals = Vec::new();
        for _ in 0..5 {
            missing_proposals.push(env.next_proposal_empty());
        }
        //
        // last proposal arrvies
        let p_fut = env.next_proposal_empty();
        let (author, _, verified_message) = p_fut.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let rounds = extract_proposal_commit_rounds(cmds);
        assert_eq!(rounds, vec![]);

        // was in Round(2) and skipped over 5 proposals. Handling
        // p_fut should be at Round(3+5)
        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(3 + 5)
        );

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for p in missing_proposals {
            let (author, _, verified_message) = p.clone().destructure();
            cmds.extend(wrapped_state.handle_proposal_message(author, verified_message));
        }
        let rounds = extract_proposal_commit_rounds(cmds);
        assert_eq!(
            rounds,
            vec![3, 4, 5, 6, 7, 8]
                .into_iter()
                .map(Round)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_commit_rule_non_consecutive() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // round 1 proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        // round 2 proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        assert!(
            matches!(wrapped_state.consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(2))
        );

        // round 2 timeout
        let pacemaker_cmds = wrapped_state
            .consensus
            .pacemaker
            .handle_event(&mut wrapped_state.consensus.safety);

        let broadcast_cmd = pacemaker_cmds
            .iter()
            .find(|cmd| matches!(cmd, PacemakerCommand::PrepareTimeout(_)))
            .unwrap();

        let tmo = if let PacemakerCommand::PrepareTimeout(tmo) = broadcast_cmd {
            tmo
        } else {
            panic!()
        };
        assert_eq!(tmo.tminfo.round, Round(2));

        let _ = env.next_tc(Epoch(1));

        // round 3 proposal, has qc(1)
        let p3 = env.next_proposal_empty();
        assert_eq!(p3.block_header.qc.get_round(), Round(1));
        assert_eq!(p3.block_header.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

        // proposal and qc have non-consecutive rounds
        // vote after a timeout happens immediately and is therefore extracted from the output cmds
        let p3_votes = extract_vote_msgs(cmds);
        assert!(p3_votes.len() == 1);
        let p3_vote = p3_votes[0].vote;
        assert_eq!(p3_vote.round, Round(3));
    }

    // this test checks that a malicious proposal sent only to the next leader is
    // not incorrectly committed
    #[test]
    fn test_malicious_proposal_and_block_recovery() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let (n1, xs) = ctx.split_first_mut().unwrap();
        let (n2, xs) = xs.split_first_mut().unwrap();
        let (n3, xs) = xs.split_first_mut().unwrap();
        let n4 = &mut xs[0];

        // first_state will send 2 different proposals, A and B. A will be sent to
        // the next leader, all other nodes get B.
        // effect is that nodes send votes for B to the next leader who thinks that
        // the "correct" proposal is A.

        let cp1 = env.next_proposal(Vec::new());
        let mp1 = env.branch_proposal(Vec::new());

        let (author_1, _, proposal_message_1) = cp1.destructure();
        let block_1 = proposal_message_1.block_header.clone();
        let payload_1 = proposal_message_1.block_body.clone();

        // n1 process cp1
        let _ = n1.handle_proposal_message(author_1, proposal_message_1.clone());
        let p1_vote = match n1.consensus_state.scheduled_vote.clone().unwrap() {
            OutgoingVoteStatus::VoteReady(v) => v,
            _ => panic!(),
        };
        let cmds1 = n1
            .wrapped_state()
            .send_vote_and_reset_timer(p1_vote.round, p1_vote);
        let p1_votes = extract_vote_msgs(cmds1)[0];

        // n3 process cp1
        let _ = n3.handle_proposal_message(author_1, proposal_message_1.clone());
        let p3_vote = match n3.consensus_state.scheduled_vote.clone().unwrap() {
            OutgoingVoteStatus::VoteReady(v) => v,
            _ => panic!(),
        };
        let cmds3 = n3
            .wrapped_state()
            .send_vote_and_reset_timer(p3_vote.round, p3_vote);
        let p3_votes = extract_vote_msgs(cmds3)[0];

        // n4 process cp1
        let _ = n4.handle_proposal_message(author_1, proposal_message_1);
        let p4_vote = match n4.consensus_state.scheduled_vote.clone().unwrap() {
            OutgoingVoteStatus::VoteReady(v) => v,
            _ => panic!(),
        };
        let cmds4 = n4
            .wrapped_state()
            .send_vote_and_reset_timer(p4_vote.round, p4_vote);
        let p4_votes = extract_vote_msgs(cmds4)[0];

        // n2 process mp1
        let (mal_author_1, _, mal_proposal_message_1) = mp1.destructure();
        let _ = n2.handle_proposal_message(mal_author_1, mal_proposal_message_1);
        let p2_vote = match n2.consensus_state.scheduled_vote.clone().unwrap() {
            OutgoingVoteStatus::VoteReady(v) => v,
            _ => panic!(),
        };
        let cmds2 = n2
            .wrapped_state()
            .send_vote_and_reset_timer(p2_vote.round, p2_vote);
        let p2_votes = extract_vote_msgs(cmds2)[0];

        assert_eq!(p1_votes.vote, p3_votes.vote);
        assert_eq!(p1_votes.vote, p4_votes.vote);
        assert_ne!(p1_votes.vote, p2_votes.vote);
        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];
        // We Collected 4 votes, 3 of which are valid, 1 of which is not caused by byzantine leader.
        // First 3 (including a false vote) submitted would not cause a qc to form
        // but the last vote would cause a qc to form locally at second_state, thus causing
        // second state to realize its missing a block.
        // request blocks up to root
        let state_2_block_range_req = BlockRange {
            last_block_id: block_1.get_id(),
            num_blocks: SeqNum(1),
        };
        for (i, vote) in votes.iter().enumerate().take(4) {
            let v = Verified::<SignatureType, VoteMessage<_>>::new(*vote, &env.keys[i]);
            let cmds2 = n2.handle_vote_message(*v.author(), *v);
            let res = cmds2
                .into_iter()
                .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
            if i < 3 {
                assert!(res.is_none());
            } else {
                assert!(res.is_some());
                let sync_range = match res.unwrap() {
                    ConsensusCommand::RequestSync(block_range) => Some(block_range),
                    _ => None,
                }
                .unwrap();
                assert_eq!(sync_range, state_2_block_range_req);
            }
        }
        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(n2.consensus_state.get_high_qc().info, votes[0].vote);

        // use the correct proposal gen to make next proposal and send it to second_state
        let cp2 = env.next_proposal(Vec::new());
        let (author_2, _, proposal_message_2) = cp2.destructure();
        let block_2 = proposal_message_2.block_header.clone();
        let payload_2 = proposal_message_2.block_body.clone();
        let cmds2 = n2.handle_proposal_message(author_2, proposal_message_2.clone());
        // Blocksync already requested with the created QC
        assert!(find_blocksync_request(&cmds2).is_none());

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = n1.handle_proposal_message(author_2, proposal_message_2);
        assert!(find_blocksync_request(&cmds1).is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = env.next_proposal(Vec::new());
        let (author_3, _, proposal_message_3) = cp3.destructure();
        let block_3 = proposal_message_3.block_header.clone();
        let payload_3 = proposal_message_3.block_body.clone();

        let cmds2 = n2.handle_proposal_message(author_3, proposal_message_3.clone());
        let cmds1 = n1.handle_proposal_message(author_3, proposal_message_3);

        // second_state has the malicious block in the blocktree, so it will not be able to
        // commit anything
        assert_eq!(n2.consensus_state.pending_block_tree.size(), 3);
        assert!(find_commit_cmds(&cmds2).is_empty());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(n1.consensus_state.pending_block_tree.size(), 2);
        assert!(!find_commit_cmds(&cmds1).is_empty());

        // a block sync request arrived, helping second state to recover
        let full_block_1 = ConsensusFullBlock::new(block_1, payload_1).unwrap();
        let _ = n2.handle_block_sync(state_2_block_range_req, vec![full_block_1.clone()]);

        // in the next round, second_state should recover and able to commit
        let cp4 = env.next_proposal(Vec::new());
        let (author_4, _, proposal_message_4) = cp4.destructure();

        let cmds2 = n2.handle_proposal_message(author_4, proposal_message_4.clone());
        // new block added should allow path_to_root properly, thus no more request sync
        assert!(find_blocksync_request(&cmds2).is_none());

        // second_state has the correct blocks, so expect to see a commit
        assert_eq!(n2.consensus_state.pending_block_tree.size(), 2);
        assert!(!find_commit_cmds(&cmds2).is_empty());

        let cmds1 = n1.handle_proposal_message(author_4, proposal_message_4.clone());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(n1.consensus_state.pending_block_tree.size(), 2);
        assert!(!find_commit_cmds(&cmds1).is_empty());

        // third_state only received proposal for round 1, and is missing proposal for round 2, 3, 4
        // feeding third_state with a proposal from round 4 should trigger a blocksync for the missing range
        let cmds3 = n3.handle_proposal_message(author_4, proposal_message_4);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        let res = find_blocksync_request(&cmds3);
        let Some(ConsensusCommand::RequestSync(state_3_block_range_req)) = res else {
            panic!("request sync is not found")
        };
        // request block up to SeqNum(1) even if block 1 exists in blocktree
        // (never request down to GENESIS_SEQ_NUM)
        assert_eq!(state_3_block_range_req.num_blocks, SeqNum(1));

        let full_block_2 = ConsensusFullBlock::new(block_2, payload_2).unwrap();
        let full_block_3 = ConsensusFullBlock::new(block_3, payload_3).unwrap();
        let cmds3 = n3.handle_block_sync(
            *state_3_block_range_req,
            vec![
                full_block_1.clone(),
                full_block_2.clone(),
                full_block_3.clone(),
            ],
        );

        // the blocksync repairs path to root and block4.qc commits block 1 and 2
        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        assert!(find_blocksync_request(&cmds3).is_none());
        let commit_cmds = find_commit_cmds(&cmds3);
        assert_eq!(commit_cmds.len(), 2);

        // duplicate blocksync event should be ignored.
        let cmds3 = n3.handle_block_sync(
            *state_3_block_range_req,
            vec![full_block_1, full_block_2, full_block_3],
        );
        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        assert!(find_blocksync_request(&cmds3).is_none());
    }

    /// Test the behaviour of consensus when a block is missing
    #[test]
    fn test_missing_block() {
        let num_state = 4;
        let execution_delay = SeqNum(1);
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        env.next_proposal(Vec::new());

        let p1 = env.next_proposal(Vec::new());

        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

        // the proposal still gets processed: the node enters a new round, and
        // issues a request for the block it skipped over
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds[0], ConsensusCommand::EnterRound(_, _)));
        assert!(matches!(
            cmds[1],
            ConsensusCommand::Schedule { duration: _ }
        ));
        assert!(matches!(cmds[2], ConsensusCommand::RequestSync { .. }));
    }

    /// Test consensus behavior with mismatching eth header
    #[test]
    fn test_missing_state_root() {
        let num_state = 4;
        let execution_delay = SeqNum(5);
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let mut wrapped_state = ctx[0].wrapped_state();

        // prepare 5 blocks
        for _ in 0..4 {
            let p = env.next_proposal(Vec::new());
            let (author, _, p) = p.destructure();
            let _cmds = wrapped_state.handle_proposal_message(author, p);
        }
        let p = env.next_proposal(vec![EthHeader(Header {
            number: 0,
            ..Default::default()
        })]);
        let (author, _, p) = p.destructure();
        let bid_1 = p.block_header.get_id();
        let _cmds = wrapped_state.handle_proposal_message(author, p);
        // valid execution result, so is coherent
        assert!(wrapped_state
            .consensus
            .pending_block_tree
            .is_coherent(&bid_1));

        assert_eq!(wrapped_state.consensus.get_current_round(), Round(5));

        assert_eq!(
            wrapped_state.metrics.consensus_events.rx_execution_lagging,
            0
        );

        // Block 11 carries the state root hash from executing block 6 the state
        // root hash is missing. The certificates are processed - consensus enters new round and commit blocks, but it doesn't vote
        let p = env.next_proposal(
            // no eth header here
            Vec::new(),
        );
        let (author, _, p) = p.destructure();
        let bid_2 = p.block_header.get_id();

        let cmds = wrapped_state.handle_proposal_message(author, p);
        // invalid execution result, so incoherent and we don't vote
        assert!(!wrapped_state
            .consensus
            .pending_block_tree
            .is_coherent(&bid_2));
        assert_eq!(wrapped_state.metrics.consensus_events.rx_bad_state_root, 1);

        assert_eq!(wrapped_state.consensus.get_current_round(), Round(6));
        assert_eq!(cmds.len(), 3);
        assert!(matches!(
            cmds[0],
            ConsensusCommand::CommitBlocks(OptimisticPolicyCommit::Finalized(_))
        ));
        assert!(matches!(cmds[1], ConsensusCommand::EnterRound(_, _)));
        assert!(matches!(
            cmds[2],
            ConsensusCommand::Schedule { duration: _ }
        ));
    }

    #[test_case(4; "4 participants")]
    #[test_case(5; "5 participants")]
    #[test_case(6; "6 participants")]
    #[test_case(7; "7 participants")]
    #[test_case(123; "123 participants")]
    fn test_observing_qc_through_votes(num_state: usize) {
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        for i in 0..8 {
            let cp = env.next_proposal(Vec::new());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // observing a qc that link to root should not trigger anything
                assert!(find_blocksync_request(&cmds).is_none());
                assert_eq!(node.consensus_state.get_current_round(), Round(i + 1))
            }
        }
        for c in ctx.iter() {
            assert_eq!(c.consensus_state.get_current_round(), Round(8));
        }
        // proposal 8 commits block from proposal 6
        let num_blocks = SeqNum(1);

        // determine the leader of round 11 because we are going to skip sending them the round 9
        // proposal while the other nodes get it and send their votes
        let epoch = env
            .epoch_manager
            .get_epoch(Round(11))
            .expect("epoch exists");
        let next_leader = env.election.get_leader(
            Round(11),
            env.val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        let mut leader_index = 0;

        // generate proposals 9 and 10 and collect the votes for 10 so that we can deliver them to
        // the leader of 11
        let mut proposal_10_votes = vec![];
        let mut proposal_10_blockid = BlockId(Hash::default());
        for round in 9..11 {
            let cp = env.next_proposal(Vec::new());
            let (author, _, verified_message) = cp.destructure();
            for (i, node) in ctx.iter_mut().enumerate() {
                if node.nodeid != next_leader {
                    let _ = node.handle_proposal_message(author, verified_message.clone());

                    if round == 10 {
                        proposal_10_blockid = verified_message.block_header.get_id();

                        assert!(
                            matches!(node.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(10))
                        );
                        let vote = match node.consensus_state.scheduled_vote.clone().unwrap() {
                            OutgoingVoteStatus::VoteReady(v) => v,
                            _ => panic!(),
                        };
                        let cmds = node
                            .wrapped_state()
                            .send_vote_and_reset_timer(vote.round, vote);

                        let v = extract_vote_msgs(cmds);
                        assert_eq!(v.len(), 1);
                        assert_eq!(v[0].vote.round, Round(10));

                        proposal_10_votes.push((node.nodeid, v[0]));
                    }
                } else {
                    leader_index = i;
                }
            }
        }
        let mut leader_state = ctx[leader_index].wrapped_state();
        for (i, (author, v)) in proposal_10_votes.iter().enumerate() {
            let cmds = leader_state.handle_vote_message(*author, *v);
            if i == (num_state * 2 / 3) {
                let blocksync_req = extract_blocksync_requests(cmds);
                assert_eq!(blocksync_req.len(), 1);
                assert_eq!(
                    blocksync_req[0],
                    BlockRange {
                        last_block_id: proposal_10_blockid,
                        num_blocks,
                    }
                );
            } else {
                assert!(find_blocksync_request(&cmds).is_none());
            }
        }
    }

    #[test]
    fn test_observe_qc_through_tmo() {
        let num_state = 5;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = env.next_proposal(Vec::new());

            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut().skip(1) {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // observing a qc that link to root should not trigger anything
                assert!(find_blocksync_request(&cmds).is_none());
            }
            blocks.push(verified_message.block_header);
        }

        let (node0, xs) = ctx.split_first_mut().unwrap();
        let node1 = &mut xs[0];

        // now timeout someone
        let cmds = node1.wrapped_state().handle_timeout_expiry();
        let tmo: Vec<&Timeout<SignatureType, SignatureCollectionType, EthExecutionProtocol>> = cmds
            .iter()
            .filter_map(|cmd| match cmd {
                ConsensusCommand::Publish { target: _, message } => {
                    match &message.deref().message {
                        ProtocolMessage::Timeout(timeout_message) => Some(&timeout_message.timeout),
                        _ => None,
                    }
                }
                _ => None,
            })
            .collect();

        assert_eq!(tmo.len(), 1);
        assert_eq!(tmo[0].tminfo.round, Round(4));
        let author = node1.nodeid;
        let timeout_msg = TimeoutMessage::new(tmo[0].clone(), &env.cert_keys[1]);
        let cmds = node0.handle_timeout_message(author, timeout_msg);
        let req = extract_blocksync_requests(cmds);
        assert_eq!(req.len(), 1);
        // last committed block was 0 and high_qc is for block 3. node should request to SeqNum(1)
        assert_eq!(
            req[0],
            BlockRange {
                last_block_id: blocks[2].get_id(),
                num_blocks: SeqNum(1),
            }
        );
    }

    #[test]
    fn test_observe_qc_through_proposal() {
        let num_state = 5;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = env.next_proposal(Vec::new());
            let (_, _, verified_message) = cp.destructure();
            blocks.push(verified_message.block_header);
        }
        let cp = env.next_proposal(Vec::new());

        let (author, _, verified_message) = cp.destructure();
        let node = &mut ctx[1];
        let cmds = node.handle_proposal_message(author, verified_message);
        let req = extract_blocksync_requests(cmds);
        assert_eq!(req.len(), 1);
        // last committed block was 0 and high_qc is for block 4. node should request to SeqNum(1)
        assert_eq!(
            req[0],
            BlockRange {
                last_block_id: blocks[3].get_id(),
                num_blocks: SeqNum(1),
            }
        );
    }

    // Expected behaviour when leader N+2 receives the votes for N+1 before receiving the proposal
    // for N+1 is that it creates the QC, but does not send the proposal until N+1 arrives.
    #[test]
    fn test_votes_with_missing_parent_block() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let missing_round = 9;

        for i in 0..missing_round - 1 {
            let cp = env.next_proposal_empty();
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                assert!(extract_blocksync_requests(cmds).is_empty());
                assert_eq!(node.consensus_state.get_current_round(), Round(i + 1))
            }
        }
        for node in ctx.iter() {
            assert_eq!(
                node.consensus_state.get_current_round(),
                Round(missing_round - 1)
            );
        }

        // determine the leader of round after missing round so we can skip sending them missing
        // round and then send the votes for missing round
        let epoch = env
            .epoch_manager
            .get_epoch(Round(missing_round + 1))
            .expect("epoch exists");
        let next_leader = env.election.get_leader(
            Round(missing_round + 1),
            env.val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        let mut leader_index = 0;
        let mut votes = vec![];
        let cp = env.next_proposal_empty();
        let (author, _, verified_message) = cp.destructure();

        // get the votes for missing round
        for (i, node) in ctx.iter_mut().enumerate() {
            if node.nodeid != next_leader {
                let _ = node.handle_proposal_message(author, verified_message.clone());
                let vote = match node.consensus_state.scheduled_vote.clone().unwrap() {
                    OutgoingVoteStatus::VoteReady(v) => v,
                    _ => panic!(),
                };
                let cmds = node
                    .wrapped_state()
                    .send_vote_and_reset_timer(vote.round, vote);

                let v = extract_vote_msgs(cmds);
                assert_eq!(v.len(), 1);
                votes.push((node.nodeid, v[0]));
            } else {
                leader_index = i;
            }
        }

        // deliver the votes for missing round
        for (i, (author, v)) in votes.iter().enumerate() {
            let leader = &mut ctx[leader_index];
            let cmds = leader.handle_vote_message(*author, *v);
            // make sure that after super majority of votes, we are not prodcuing any proposal
            // commands
            if i >= (num_state * 2 / 3) {
                let proposal_exists = cmds.into_iter().any(|c| match c {
                    ConsensusCommand::Publish {
                        target: RouterTarget::Broadcast(_),
                        message,
                    } => matches!(
                        &message.deref().deref().message,
                        ProtocolMessage::Proposal(_)
                    ),
                    _ => false,
                });
                assert!(!proposal_exists);
            }
        }

        // when the missing proposal arrives, we expect the pending qc to now propose
        // and we also expect the node to process the QC and therefore move into missing_round+1
        let leader = &mut ctx[leader_index];
        let cmds = leader.handle_proposal_message(
            author,
            verified_message, // this was the missing proposal message
        );

        assert_eq!(
            leader.consensus_state.pacemaker.get_current_round(),
            Round(missing_round + 1)
        );
        let round = extract_create_proposal_command_round(cmds);
        assert_eq!(Round(missing_round + 1), round);
    }

    /// This test asserts that proposal not from the round leader is not added
    /// to the blocktree
    ///
    /// 1. state[0] processes a valid proposal `p1``, accepts it and adds it to
    ///    the blocktree. There are 2 blocks in the blocktree
    ///
    /// 2. state[0] processes an invalid proposal `invalid_p2`. It's rejected
    ///    and not added to the block tree. It emits an event for
    ///    `invalid_proposal_round_leader`
    #[traced_test]
    #[test]
    fn test_reject_non_leader_proposal() {
        let num_state = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let node = &mut ctx[0];

        let verified_p1 = env.next_proposal_empty();
        let (_, _, p1) = verified_p1.destructure();

        let _ = node.handle_proposal_message(p1.block_header.author, p1);
        assert_eq!(node.consensus_state.blocktree().size(), 1);

        let verified_p2 = env.next_proposal_empty();
        let (_, _, p2) = verified_p2.destructure();

        let epoch = env.epoch_manager.get_epoch(Round(4)).expect("epoch exists");
        let invalid_author = env.election.get_leader(
            Round(4),
            env.val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        assert!(invalid_author != node.nodeid);
        assert!(invalid_author != p2.block_header.author);
        let invalid_b2 = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody::default(),
        });
        let invalid_bh2 = ConsensusBlockHeader::new(
            invalid_author,
            p2.block_header.epoch,
            p2.block_header.round,
            p2.block_header.delayed_execution_results.clone(),
            p2.block_header.execution_inputs.clone(),
            invalid_b2.get_id(),
            p2.block_header.qc.clone(),
            p2.block_header.seq_num,
            p2.block_header.timestamp_ns,
            p2.block_header.round_signature,
        );
        let invalid_p2 = ProposalMessage {
            block_header: invalid_bh2,
            block_body: invalid_b2,
            last_round_tc: None,
        };

        let _ = node.handle_proposal_message(invalid_p2.block_header.author, invalid_p2);

        // p2 is not added because author is not the round leader
        assert_eq!(node.consensus_state.blocktree().size(), 1);
        assert_eq!(
            node.metrics.consensus_events.invalid_proposal_round_leader,
            1
        );
    }

    #[test]
    fn test_schedule_next_epoch() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // handle update_block + 2 proposals to commit the update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(Vec::new());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert!(current_epoch == Epoch(1));

            let expected_epoch_start_round =
                update_block_round + node.epoch_manager.epoch_start_delay;
            // verify that the start of next epoch is scheduled correctly
            assert!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1))
                    .unwrap()
                    == Epoch(1)
            );
            assert!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round)
                    .unwrap()
                    == Epoch(2)
            );
        }
    }

    #[test]
    fn test_advance_epoch_through_proposal_qc() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(Vec::new());

            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1))
                    .unwrap(),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round)
                    .unwrap(),
                Epoch(2)
            );
        }

        env.epoch_manager
            .schedule_epoch_start(update_block, update_block_round);

        // handle proposals until the last round of the epoch
        for _ in (update_block_round.0 + 2)..(expected_epoch_start_round.0 - 1) {
            let cp = env.next_proposal_empty();
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));
        }

        // proposal for Round(expected_epoch_start_round)
        let cp = env.next_proposal_empty();
        let (author, _, verified_message) = cp.destructure();
        let node = &mut ctx[0];
        // observe QC to advance round and epoch
        let _ = node.handle_proposal_message(author, verified_message);
        let current_epoch = node
            .epoch_manager
            .get_epoch(node.consensus_state.get_current_round())
            .unwrap();
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_advance_epoch_through_proposal_tc() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(Vec::new());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1))
                    .unwrap(),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round)
                    .unwrap(),
                Epoch(2)
            );
        }

        env.epoch_manager
            .schedule_epoch_start(update_block, update_block_round);

        // generate TCs until the last round of the epoch
        for _ in (update_block_round.0 + 1)..(expected_epoch_start_round.0 - 1) {
            let _ = env.next_tc(Epoch(1));
        }

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));
        }

        // proposal for Round(expected_epoch_start_round)
        let cp = env.next_proposal(Vec::new());
        let (author, _, verified_message) = cp.destructure();
        let node = &mut ctx[0];
        // observe TC to advance round and epoch
        let _ = node.handle_proposal_message(author, verified_message);
        let current_epoch = node
            .epoch_manager
            .get_epoch(node.consensus_state.get_current_round())
            .unwrap();
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_advance_epoch_through_local_tc() {
        let num_states = 4;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(Vec::new());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1))
                    .unwrap(),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round)
                    .unwrap(),
                Epoch(2)
            );
        }

        env.epoch_manager
            .schedule_epoch_start(update_block, update_block_round);

        // handle proposals until the last round of the epoch
        for _ in (update_block_round.0 + 2)..(expected_epoch_start_round.0 - 1) {
            let cp = env.next_proposal(Vec::new());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));
        }

        // generate TC for the last round of the epoch
        let tmo_msgs = env.next_tc(Epoch(1));

        let (_, tmo_msgs) = tmo_msgs.split_first().unwrap();

        // handle the three timeout messages from other nodes
        let node = &mut ctx[0];
        for tmo_msg in tmo_msgs {
            let (author, _, tm) = tmo_msg.clone().destructure();

            let _ = node.handle_timeout_message(author, tm);
        }

        let current_epoch = node
            .epoch_manager
            .get_epoch(node.consensus_state.get_current_round())
            .unwrap();
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_schedule_epoch_on_blocksync() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let mut blocks = vec![];
        // Sequence number of the block which updates the validator set
        let update_block_num = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block_num);

        // handle blocks until update_block - 1
        for _ in 0..(update_block_round.0 - 1) {
            let proposal = env.next_proposal(Vec::new());
            let (author, _, verified_message) = proposal.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push((verified_message.block_header, verified_message.block_body));
        }

        // let mut block_sync_blocks = Vec::new();
        // handle 3 blocks for only state 0
        for _ in (update_block_round.0)..(update_block_round.0 + 3) {
            let proposal = env.next_proposal(Vec::new());
            println!("proposal seq num {:?}", proposal.block_header.seq_num);
            let (author, _, verified_message) = proposal.destructure();

            let state = &mut ctx[0];
            let cmds = state.handle_proposal_message(author, verified_message.clone());
            // state should not request blocksync
            assert!(extract_blocksync_requests(cmds).is_empty());

            blocks.push((verified_message.block_header, verified_message.block_body));
        }
        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        // state 0 should have committed update_block and scheduled next epoch
        // verify state 0 is still in epoch 1
        let state_0_epoch = ctx[0]
            .epoch_manager
            .get_epoch(ctx[0].consensus_state.get_current_round())
            .unwrap();
        assert_eq!(state_0_epoch, Epoch(1));

        // verify state 0 scheduled the next epoch correctly
        assert_eq!(
            ctx[0]
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1))
                .unwrap(),
            Epoch(1)
        );
        assert_eq!(
            ctx[0]
                .epoch_manager
                .get_epoch(expected_epoch_start_round)
                .unwrap(),
            Epoch(2)
        );

        // state 1 should still not have scheduled next epoch since it didn't commit update_block
        let state_1_epoch = ctx[1]
            .epoch_manager
            .get_epoch(ctx[1].consensus_state.get_current_round())
            .unwrap();
        assert_eq!(state_1_epoch, Epoch(1));
        assert_eq!(
            ctx[1]
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1))
                .unwrap(),
            Epoch(1)
        );
        assert_eq!(
            ctx[1]
                .epoch_manager
                .get_epoch(expected_epoch_start_round)
                .unwrap(),
            Epoch(1)
        ); // STILL EPOCH 1

        // generate proposal for update_block + 3
        let proposal = env.next_proposal_empty();

        let (author, _, verified_message) = proposal.destructure();
        let state = &mut ctx[1];
        let cmds = state.handle_proposal_message(author, verified_message);
        // state 1 should request blocksync
        let requested_ranges = extract_blocksync_requests(cmds);
        assert_eq!(requested_ranges.len(), 1);
        let requested_range = requested_ranges[0];
        // last received block was update_block - 1, last committed block was update_block - 3
        assert_eq!(requested_range.num_blocks, SeqNum(1));

        let state1 = &mut ctx[1];
        let (_, requested_blocks) = blocks
            .as_slice()
            .split_at((update_block_round.0 as usize) - 3);
        let blocksync_blocks = requested_blocks
            .iter()
            .cloned()
            .map(|(block, payload)| ConsensusFullBlock::new(block, payload).unwrap())
            .collect_vec();
        let _ = state1.handle_block_sync(requested_ranges[0], blocksync_blocks);

        // blocks are committed immediately after blocksync is finished
        // state 1 should have scheduled the next epoch
        let state_1_epoch = state1
            .epoch_manager
            .get_epoch(state1.consensus_state.get_current_round())
            .unwrap();
        assert_eq!(state_1_epoch, Epoch(1));
        assert_eq!(
            state1
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1))
                .unwrap(),
            Epoch(1)
        );
        assert_eq!(
            state1
                .epoch_manager
                .get_epoch(expected_epoch_start_round)
                .unwrap(),
            Epoch(2)
        ); // NOW EPOCH 2
    }

    #[test]
    fn test_advance_epoch_with_blocksync() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        // handle proposals until expected_epoch_start_round - 1
        for _ in 0..(expected_epoch_start_round.0 - 1) {
            let cp = env.next_proposal(Vec::new());

            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block_header);
        }

        for node in ctx.iter_mut() {
            // verify all states are still in epoch 1
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1))
                    .unwrap(),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round)
                    .unwrap(),
                Epoch(2)
            );
        }

        env.epoch_manager
            .schedule_epoch_start(update_block, update_block_round);

        let val_stakes: Vec<(NodeId<_>, Stake)> = env
            .val_epoch_map
            .get_val_set(&Epoch(2))
            .unwrap()
            .get_members()
            .iter()
            .map(|(p, s)| (*p, *s))
            .collect();
        let val_cert_pubkeys = env
            .val_epoch_map
            .get_cert_pubkeys(&Epoch(2))
            .unwrap()
            .map
            .clone();
        env.val_epoch_map.insert(
            Epoch(3),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map.insert(
                Epoch(3),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
        }

        // skip block on round expected_epoch_start_round
        let _unused_proposal = env.next_proposal(Vec::new());
        ();
        // generate proposal for expected_epoch_start_round + 1
        let cp = env.next_proposal(Vec::new());
        ();

        let (author, _, verified_message) = cp.destructure();
        for node in ctx.iter_mut() {
            let cmds = node.handle_proposal_message(author, verified_message.clone());
            // state should have requested blocksync
            assert_eq!(extract_blocksync_requests(cmds).len(), 1);

            // verify state is now in epoch 2
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round())
                .unwrap();
            assert_eq!(current_epoch, Epoch(2));
        }
    }

    #[test]
    fn test_blocksync_invariant() {
        let num_state = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );
        let p1 = env.next_proposal(Vec::new());
        // there's no child block in the blocktree, so this must be ignored
        // (because invariant is broken)
        let mut wrapped_state = ctx[0].wrapped_state();
        let block_range = BlockRange {
            last_block_id: p1.block_header.get_id(),
            num_blocks: SeqNum(1),
        };
        let full_block =
            ConsensusFullBlock::new(p1.block_header.clone(), p1.block_body.clone()).unwrap();
        let cmds = wrapped_state.handle_block_sync(block_range, vec![full_block]);
        assert!(cmds.is_empty());

        // assert that consensus state wasn't mutated by comparing it against an unchanged
        // consensus state
        assert_eq!(ctx[0].consensus_state, ctx[1].consensus_state);

        // now test normal proposal path and make sure that mutates state
        let (author, _, verified_message) = p1.destructure();
        let mut wrapped_state = ctx[0].wrapped_state();
        let _cmds = wrapped_state.handle_proposal_message(author, verified_message);
        assert_ne!(ctx[0].consensus_state, ctx[1].consensus_state);
    }

    #[test]
    fn test_blocksync_requests() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(Vec::new());
        let (author_1, _, proposal_message_1) = cp.destructure();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // vote for block 1
        assert!(
            matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );
        // no blocksync requests
        assert!(find_blocksync_request(&cmds).is_none());

        // generate block 2
        let cp = env.next_proposal(Vec::new());
        let (_author_2, _, _proposal_message_2) = cp.destructure();

        // generate block 3
        let cp = env.next_proposal(Vec::new());
        let (author_3, _, proposal_message_3) = cp.destructure();

        // state receives block 3
        let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
        // expect blocksync request for block 2
        let requested_ranges = extract_blocksync_requests(cmds);
        assert_eq!(requested_ranges.len(), 1);

        // generate block 4
        let cp = env.next_proposal(Vec::new());
        let (author_4, _, proposal_message_4) = cp.destructure();

        // state receives block 4
        let cmds = n1.handle_proposal_message(author_4, proposal_message_4);
        // expect no blocksync request since the missing ancestor is the same
        let requested_ranges = extract_blocksync_requests(cmds);
        assert_eq!(requested_ranges.len(), 0);
    }

    #[test]
    fn test_vote_sent_to_leader_in_next_epoch() {
        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let val_stakes: Vec<(NodeId<_>, Stake)> = env
            .val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members()
            .iter()
            .map(|(p, s)| (*p, *s))
            .collect();
        let val_cert_pubkeys = env
            .val_epoch_map
            .get_cert_pubkeys(&Epoch(1))
            .unwrap()
            .map
            .clone();
        env.val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        env.val_epoch_map.insert(
            Epoch(1),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
            node.val_epoch_map.insert(
                Epoch(1),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
        }
        // generate a random key as a validator in epoch 2
        let epoch_2_leader = NodeId::new(get_key::<SignatureType>(100).pubkey());
        env.val_epoch_map.insert(
            Epoch(2),
            vec![(epoch_2_leader, Stake(1))],
            ValidatorMapping::new(vec![(epoch_2_leader, epoch_2_leader.pubkey())]),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map.insert(
                Epoch(2),
                vec![(epoch_2_leader, Stake(1))],
                ValidatorMapping::new(vec![(epoch_2_leader, epoch_2_leader.pubkey())]),
            );
        }

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval - SeqNum(1);
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        env.epoch_manager
            .schedule_epoch_start(update_block, update_block_round);

        // commit blocks until the last round of the epoch
        for _ in 0..(expected_epoch_start_round.0 - 2) {
            let cp = env.next_proposal_empty();
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }
        }

        // handle proposal on the last round of the epoch
        let cp = env.next_proposal_empty();
        let (author, _, verified_message) = cp.destructure();
        for node in ctx.iter_mut() {
            let _ = node.handle_proposal_message(author, verified_message.clone());
            // state should send vote to the leader in epoch 2
            assert!(
                matches!(node.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == (expected_epoch_start_round - Round(1)))
            );
        }
    }

    #[test_case(true; "Receive missed block through blocksync")]
    #[test_case(false; "Receive missed block through out of order proposal")]
    fn test_delay_coherency_check_of_proposal_message_scenario(through_blocksync: bool) {
        // Scenario and expected result:
        // State 1 receives Block 1. It should store Block 1 as validated in blocktree and send a vote message
        // State 1 receives Block 3. It should request blocksync for Block 2, skip voting, and store Block 3
        //      as unvalidated in blocktree
        // State 1 receives Block 2 (blocksync/out-of-order proposal [ARGUMENT]). It should store Block 2 as
        //      validated in blocktree, and skip voting. Block 3 should still be validated in the blocktree.
        // State 1 receives Block 4. It should validate Block 4, and send a vote message.

        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (n2, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(Vec::new());
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block_header.get_id();

        println!(
            "root seq num: {:?}",
            n1.consensus_state.pending_block_tree.get_root_seq_num()
        );

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // vote for block 1
        assert!(
            matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );
        // no blocksync requests
        assert!(find_blocksync_request(&cmds).is_none());
        // block 1 should be in the blocktree as coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);

        println!(
            "root seq num: {:?}",
            n1.consensus_state.pending_block_tree.get_root_seq_num()
        );

        // generate block 2
        let cp = env.next_proposal(Vec::new());
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block_header.get_id();

        // generate block 3
        let cp = env.next_proposal(Vec::new());
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block_header.get_id();

        // state receives block 3
        let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
        // should not vote for block 3
        assert!(
            matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round != Round(3))
        );
        let requested_ranges = extract_blocksync_requests(cmds);
        assert_eq!(requested_ranges.len(), 1);
        // block 3 should be in the blocktree as incoherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(!block_3_blocktree_entry.is_coherent);

        // state receives block 2
        if through_blocksync {
            // blocksync response for state 2
            let full_block_2 = ConsensusFullBlock::new(
                proposal_message_2.block_header,
                proposal_message_2.block_body,
            )
            .unwrap();
            let _ = n1.handle_block_sync(requested_ranges[0], vec![full_block_2]);
        } else {
            let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
            // should not vote for block or blocksync
            assert!(
                matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round != Round(2))
            );
            assert!(find_blocksync_request(&cmds).is_none());
        }
        // block 2 should be in the blocktree as coherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_id)
            .expect("should be in the blocktree");
        assert!(block_2_blocktree_entry.is_coherent);

        // block 3 should be in the blocktree as coherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(block_3_blocktree_entry.is_coherent);
    }

    #[test_case(true; "Delay validation of blocksync block")]
    #[test_case(false; "Delay validation of out of order proposal")]
    fn test_update_coherency_of_missed_blocks(through_blocksync: bool) {
        // Scenario and expected result:
        // State 1 receives Block 1. It should store Block 1 as validated in blocktree and send a vote message
        // State 1 receives Block 4. It should request blocksync for Blocks 3 -> 1, skip voting, and store
        //      Block 4 as unvalidated in blocktree
        // State 1 receives Blocks 3 -> 1 (blocksync/out-of-order proposal [ARGUMENT]). If received through
        //      proposal, it should request for Blocks 2 -> 1, and store Block 3 as unvalidated in blocktree
        // State 1 receives Block 2. It should store Block 2 as validated in blocktree, and skip voting.
        //      Block 3/4 has a path to root via Block 2. It should be validated. Block 2 is committed

        let num_states = 2;
        let execution_delay = SeqNum::MAX;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (n2, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(Vec::new());
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block_header.get_id();
        let full_block_1 = ConsensusFullBlock::new(
            proposal_message_1.block_header.clone(),
            proposal_message_1.block_body.clone(),
        )
        .unwrap();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // vote for block 1
        assert!(
            matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(1))
        );
        // no blocksync requests
        assert!(find_blocksync_request(&cmds).is_none());
        // block 1 should be in the blocktree as coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);

        // generate block 2
        let cp = env.next_proposal(Vec::new());
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block_header.get_id();

        // generate block 3
        let cp = env.next_proposal(Vec::new());
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block_header.get_id();

        // generate block 4
        let cp = env.next_proposal(Vec::new());
        let (author_4, _, proposal_message_4) = cp.destructure();
        let block_4_id = proposal_message_4.block_header.get_id();

        // state receives block 4
        let cmds = n1.handle_proposal_message(author_4, proposal_message_4);
        // should not vote for block 4
        assert!(
            matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round != Round(4))
        );
        let requested_ranges = extract_blocksync_requests(cmds);
        assert_eq!(requested_ranges.len(), 1);
        let requested_range = requested_ranges[0];
        // should request all blocks up to root
        assert_eq!(requested_range.num_blocks, SeqNum(1));
        // block 4 should be in the blocktree as not coherent
        let block_4_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_4_id)
            .expect("should be in the blocktree");
        assert!(!block_4_blocktree_entry.is_coherent);

        // state receives block 3
        if through_blocksync {
            // blocksync response
            let full_block_2 = ConsensusFullBlock::new(
                proposal_message_2.block_header,
                proposal_message_2.block_body,
            )
            .unwrap();
            let full_block_3 = ConsensusFullBlock::new(
                proposal_message_3.block_header,
                proposal_message_3.block_body,
            )
            .unwrap();
            let cmds = n1.handle_block_sync(
                requested_range,
                vec![full_block_1, full_block_2, full_block_3],
            );
            // should not vote for block or blocksync
            assert!(
                matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round != Round(3))
            );
            assert!(find_blocksync_request(&cmds).is_none());
            assert_eq!(find_commit_cmds(&cmds).len(), 2);
        } else {
            let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
            // should not vote for block or blocksync
            assert!(
                matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round != Round(3))
            );
            // request blocksync for block 2
            assert!(find_blocksync_request(&cmds).is_some());

            // block 3 should be in the blocktree as not coherent
            let block_3_blocktree_entry = n1
                .consensus_state
                .pending_block_tree
                .get_entry(&block_3_id)
                .expect("should be in the blocktree");
            assert!(!block_3_blocktree_entry.is_coherent);

            // block 4 should still be in the blocktree as not coherent
            let block_4_blocktree_entry = n1
                .consensus_state
                .pending_block_tree
                .get_entry(&block_4_id)
                .expect("should be in the blocktree");
            assert!(!block_4_blocktree_entry.is_coherent);

            // state receives block 2
            let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
            // should not vote for block or blocksync
            assert!(
                matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round != Round(2))
            );
            assert!(find_blocksync_request(&cmds).is_none());
            assert_eq!(find_commit_cmds(&cmds).len(), 2);
        }

        // block 3 should be in the blocktree as coherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(block_3_blocktree_entry.is_coherent);

        // block 4 should be in the blocktree as coherent
        let block_4_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_4_id)
            .expect("should be in the blocktree");
        assert!(block_4_blocktree_entry.is_coherent);
    }

    #[test]
    fn test_ledger_propose_and_commit() {
        let num_states = 2;
        let execution_delay = SeqNum(4);
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337),
            || InMemoryStateInner::genesis(Balance::MAX, execution_delay),
            || EthValidator::new(0),
            execution_delay,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();

        // prepare execution_delay blocks
        let mut proposed_block_headers = BTreeMap::new();
        for _ in 0..execution_delay.0 - 1 {
            let p = env.next_proposal(Vec::new());
            let (author, _, p) = p.destructure();
            proposed_block_headers.insert(p.block_header.seq_num, p.block_header.clone());
            let _cmds = n1.handle_proposal_message(author, p.clone());
        }
        assert!(
            matches!(n1.wrapped_state().consensus.scheduled_vote, Some(OutgoingVoteStatus::VoteReady(v)) if v.round == Round(execution_delay.0 - 1))
        );

        // The author_<n>, proposal_message_<n> refer to the messages after the initial setup with execution_delay proposals.
        let cp = env.next_proposal(vec![EthHeader(Header {
            number: 0,
            ..Default::default()
        })]);
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block_header.get_id();

        let _ = n1.handle_proposal_message(author_1, proposal_message_1);

        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);

        let cp = env.next_proposal(vec![EthHeader(Header {
            number: 1,
            ..Default::default()
        })]);
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block_header.get_id();

        let cp = env.next_proposal(vec![EthHeader(Header {
            number: 2,
            ..Default::default()
        })]);
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block_header.get_id();

        let _ = n1.handle_proposal_message(author_2, proposal_message_2);

        // block 2 should be in the blocktree but its not coherent yet
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_id)
            .expect("should be in the blocktree");
        assert!(!block_2_blocktree_entry.is_coherent);

        // add blocks to state_backend
        n1.ledger_propose(
            proposed_block_headers
                .get(&(GENESIS_SEQ_NUM + SeqNum(1)))
                .unwrap(),
        );
        n1.ledger_commit(
            proposed_block_headers
                .get(&(GENESIS_SEQ_NUM + SeqNum(1)))
                .unwrap(),
        );

        n1.ledger_propose(
            proposed_block_headers
                .get(&(GENESIS_SEQ_NUM + SeqNum(2)))
                .unwrap(),
        );

        let _ = n1.handle_proposal_message(author_3, proposal_message_3);

        // block 2 should be in the blocktree as coherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_id)
            .expect("should be in the blocktree");
        assert!(block_2_blocktree_entry.is_coherent);

        // block 3 should be in the blocktree as coherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(block_3_blocktree_entry.is_coherent);
    }
}
