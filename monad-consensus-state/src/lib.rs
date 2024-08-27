use std::{collections::BTreeMap, time::Duration};

use monad_blocktree::blocktree::{BlockTree, BlockTreeError};
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
    block::{Block, BlockKind, BlockPolicy, BlockType, FullBlock},
    block_validator::{BlockValidationError, BlockValidator},
    checkpoint::{Checkpoint, RootInfo},
    metrics::Metrics,
    payload::{
        ExecutionProtocol, Payload, RandaoReveal, StateRootResult, StateRootValidator,
        TransactionPayload,
    },
    quorum_certificate::{QuorumCertificate, Rank},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHash,
    timeout::TimeoutCertificate,
    txpool::TxPool,
    validator_data::{ValidatorData, ValidatorSetData, ValidatorSetDataWithEpoch},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_state_backend::StateBackend;
use monad_types::{BlockId, Epoch, NodeId, Round, RouterTarget, SeqNum};
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
pub struct ConsensusState<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    /// Prospective blocks are stored here while they wait to be
    /// committed
    pending_block_tree: BlockTree<SCT, BPT, SBT>,
    /// State machine to track collected votes for proposals
    vote_state: VoteState<SCT>,
    /// The highest QC (QC height determined by Round) known to this node
    high_qc: QuorumCertificate<SCT>,
    /// Tracks and updates the current round
    pacemaker: Pacemaker<SCT>,
    /// Policy for upholding consensus safety when voting or extending branches
    safety: Safety,
    block_sync_requests: BTreeMap<BlockId, Round>,
    last_proposed_round: Round,

    /// Set to true once consensus has kicked off execution
    started_execution: bool,
    /// Set to true once consensus has kicked off stastesync
    /// This is a bit janky; because initiating statesync is asynchronous (via loopback executor)
    /// Ideally we can delete this and initiate statesync synchronously... needs some thought
    started_statesync: bool,
}

impl<SCT, BPT, SBT> PartialEq for ConsensusState<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.pending_block_tree.eq(&other.pending_block_tree)
            && self.vote_state.eq(&other.vote_state)
            && self.high_qc.eq(&other.high_qc)
            && self.pacemaker.eq(&other.pacemaker)
            && self.safety.eq(&other.safety)
    }
}

impl<SCT, BPT, SBT> std::fmt::Debug for ConsensusState<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusState")
            .field("pending_block_tree", &self.pending_block_tree)
            .field("vote_state", &self.vote_state)
            .field("high_qc", &self.high_qc)
            .field("pacemaker", &self.pacemaker)
            .field("safety", &self.safety)
            .finish()
    }
}

pub struct ConsensusStateWrapper<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,
    SVT: StateRootValidator,
{
    pub consensus: &'a mut ConsensusState<SCT, BPT, SBT>,

    pub metrics: &'a mut Metrics,
    pub tx_pool: &'a mut TT,
    pub epoch_manager: &'a mut EpochManager,
    /// Policy for validating chain extension
    /// Mutable because consensus tip will be updated
    pub block_policy: &'a mut BPT,
    pub state_backend: &'a SBT,

    pub val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    pub election: &'a LT,
    pub version: &'a str,

    /// Policy for validating the state root hashes included in proposals
    pub state_root_validator: &'a SVT,
    /// Policy for validating incoming proposals
    pub block_validator: &'a BVT,
    /// Track local timestamp and validate proposal timestamps
    pub block_timestamp: &'a BlockTimestamp,

    /// Destination address for proposal payments
    pub beneficiary: &'a EthAddress,
    /// This nodes public NodeId; what other nodes see in the validator set
    pub nodeid: &'a NodeId<SCT::NodeIdPubKey>,
    /// Parameters for consensus algorithm behaviour
    pub config: &'a ConsensusConfig,

    // TODO-2 deprecate keypairs should probably have a different interface
    // so that users have options for securely storing their keys
    pub keypair: &'a ST::KeyPairType,
    pub cert_keypair: &'a SignatureCollectionKeyPairType<SCT>,
}

/// Consensus algorithm's configurable parameters
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct ConsensusConfig {
    /// Maximum number of transactions allowed in a proposal
    pub proposal_txn_limit: usize,
    /// Maximum cumulative gas allowed for all transactions in a proposal
    pub proposal_gas_limit: u64,
    /// Duration used by consensus to determine timeout lengths
    /// delta should be approximately equal to the upper bound of message
    /// delivery during a broadcast
    pub delta: Duration,
    /// If the node is lagging over state_sync_threshold blocks behind,
    /// then it should trigger statesync.
    /// Lagging (0, state_sync_threshold) blocks: request blocksync
    /// Lagging  >= state_sync_threshold  blocks: trigger statesync
    pub state_sync_threshold: SeqNum,

    pub timestamp_latency_estimate_ms: u64,
}

/// Possible actions a leader node can take when entering a new round
pub enum ConsensusAction {
    /// Create a proposal with this state-root-hash
    Propose(StateRootHash),
    /// Create an empty block proposal
    ProposeNull,
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

impl<SCT, BPT, SBT> ConsensusState<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    /// Create the core consensus state
    ///
    /// Arguments
    ///
    /// config - collection of configurable parameters for core consensus algorithm
    pub fn new(
        epoch_manager: &EpochManager,
        config: &ConsensusConfig,
        root: RootInfo,
        high_qc: QuorumCertificate<SCT>,
    ) -> Self {
        let high_qc_round = high_qc.get_round();
        assert!(high_qc_round >= root.round);
        let consensus_round = high_qc_round + Round(1);
        let consensus_epoch = epoch_manager.get_epoch(consensus_round).unwrap_or_else(|| {
            panic!(
                "unknown epoch for consensus_round={:?}. try a newer forkpoint?",
                consensus_round
            )
        });
        ConsensusState {
            pending_block_tree: BlockTree::new(root),
            vote_state: VoteState::new(consensus_round),
            high_qc,
            pacemaker: Pacemaker::new(config.delta, consensus_epoch, consensus_round, None),
            safety: Safety::new(
                // ConsensusState is timed out synchronously on MonadState init, which always sets
                // highest_vote_round to consensus_round
                //
                // this gives us the guarantee that we will never vote on consensus_round
                //
                // we set this to high_qc_round here just because our consensus state machine unit
                // tests don't assume that Round(1) gets timed out
                high_qc_round, // highest_vote_round
                high_qc_round, // highest_qc_round
            ),

            block_sync_requests: Default::default(),

            // initial value here doesn't matter; it just must increase monotonically
            // this is because we never propose on restart - we always time out
            //
            // on our first proposal, we will set this to the appropriate value
            last_proposed_round: Round(0),

            started_execution: false,
            started_statesync: false,
        }
    }

    /// a blocksync request could be for a block that is not yet committed so we
    /// try and fetch it from the blocktree
    pub fn fetch_uncommitted_block(&self, bid: &BlockId) -> Option<FullBlock<SCT>> {
        self.pending_block_tree
            .get_block(bid)
            .map(|b| b.clone().get_full_block()) //TODO revisit
    }

    pub fn blocktree(&self) -> &BlockTree<SCT, BPT, SBT> {
        &self.pending_block_tree
    }

    pub fn get_high_qc(&self) -> &QuorumCertificate<SCT> {
        &self.high_qc
    }

    pub fn get_current_epoch(&self) -> Epoch {
        self.pacemaker.get_current_epoch()
    }

    pub fn get_current_round(&self) -> Round {
        self.pacemaker.get_current_round()
    }
}

impl<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT>
    ConsensusStateWrapper<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,

    SVT: StateRootValidator,
{
    /// handles the local timeout expiry event
    pub fn handle_timeout_expiry(&mut self) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = Vec::new();

        self.metrics.consensus_events.local_timeout += 1;
        debug!(
            round = ?self.consensus.pacemaker.get_current_round(),
            "local timeout"
        );
        cmds.extend(
            self.consensus
                .pacemaker
                .handle_event(&mut self.consensus.safety, &self.consensus.high_qc)
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
        p: ProposalMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let _handle_proposal_span =
            tracing::info_span!("handle_proposal_span", "{}", author).entered();
        info!(round = ?p.block.get_round(), "Received Proposal");
        debug!(proposal = ?p, "Proposal Message");
        self.metrics.consensus_events.handle_proposal += 1;
        let mut cmds = Vec::new();

        let epoch = self
            .epoch_manager
            .get_epoch(p.block.round)
            .expect("epoch verified");
        let validator_set = self
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("proposal message was verified");

        let state_root_action = self.state_root_hash_validation(&p);

        if matches!(state_root_action, StateRootAction::Reject) {
            return cmds;
        }

        // a valid proposal will advance the pacemaker round so capture the original round before
        // handling the proposal certificate
        let original_round = self.consensus.pacemaker.get_current_round();
        cmds.extend(self.proposal_certificate_handling(&p));

        // author, leader, round checks
        let round = self.consensus.pacemaker.get_current_round();
        let block_round_leader = self
            .election
            .get_leader(p.block.get_round(), validator_set.get_members());
        if p.block.get_round() > round
            || author != block_round_leader
            || p.block.get_author() != block_round_leader
        {
            debug!(
                expected_round = ?round,
                round = ?p.block.get_round(),
                expected_leader = ?block_round_leader,
                author = ?author,
                block_author = ?p.block.get_author(),
                "Invalid Proposal"
            );
            self.metrics.consensus_events.invalid_proposal_round_leader += 1;
            return cmds;
        }

        let author_pubkey = self
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("proposal message was verified")
            .map
            .get(&author)
            .expect("proposal author exists in validator_mapping");
        let block = match self
            .block_validator
            .validate(p.block, p.payload, author_pubkey)
        {
            Ok(block) => block,
            Err(BlockValidationError::TxnError) => {
                warn!("Transaction validation failed");
                self.metrics.consensus_events.failed_txn_validation += 1;
                return cmds;
            }
            Err(BlockValidationError::RandaoError) => {
                self.metrics
                    .consensus_events
                    .failed_verify_randao_reveal_sig += 1;
                return cmds;
            }
            Err(BlockValidationError::HeaderPayloadMismatchError) => {
                // TODO: this is malicious behaviour?
                return cmds;
            }
            Err(BlockValidationError::PayloadError) => {
                return cmds;
            }
            Err(BlockValidationError::HeaderError) => {
                return cmds;
            }
        };

        if let Some(ts_delta) = self
            .block_timestamp
            .valid_block_timestamp(block.get_qc().get_timestamp(), block.get_timestamp())
        {
            // only update timestamp if the block advanced us our round
            if block.get_round() > original_round {
                cmds.push(ConsensusCommand::TimestampUpdate(ts_delta));
            }
        } else {
            self.metrics.consensus_events.failed_ts_validation += 1;
            warn!(prev_block_ts = ?block.get_qc().get_timestamp(),
                  curr_block_ts = ?block.get_timestamp(),
                  local_ts = ?self.block_timestamp.get_current_time(),
                  "Timestamp validation failed"
            );
            return cmds;
        }

        // at this point, block is valid and can be added to the blocktree
        if let Ok(res_cmds) = self.try_add_and_commit_blocktree(&block, Some(state_root_action)) {
            cmds.extend(res_cmds);
        } else {
            warn!("Transaction validation failed");
            self.metrics.consensus_events.failed_txn_validation += 1;
            return cmds;
        }

        // out-of-order proposals are possible if some round R+1 proposal arrives
        // before R because of network conditions. The proposals are still valid
        if block.get_round() != round {
            debug!(
                expected_round = ?round,
                round = ?block.get_round(),
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
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        debug!(?vote_msg, "Vote Message");
        if vote_msg.vote.vote_info.round < self.consensus.pacemaker.get_current_round() {
            self.metrics.consensus_events.old_vote_received += 1;
            return Default::default();
        }
        self.metrics.consensus_events.vote_received += 1;

        let mut cmds = Vec::new();

        let epoch = self
            .epoch_manager
            .get_epoch(vote_msg.vote.vote_info.round)
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

            cmds.extend(self.process_certificate_qc(&qc));
            cmds.extend(self.try_propose());
        };

        cmds
    }

    /// handling remote timeout messages from other nodes
    #[must_use]
    pub fn handle_timeout_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        tmo_msg: TimeoutMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let tm = &tmo_msg.timeout;
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.consensus.pacemaker.get_current_round() {
            self.metrics.consensus_events.old_remote_timeout += 1;
            return cmds;
        }

        debug!(timeout_msg = ?tm, "Remote timeout msg");
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

        let process_certificate_cmds = self.process_certificate_qc(&tm.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            info!(?last_round_tc, "advance round from remote TC");
            self.metrics.consensus_events.remote_timeout_msg_with_tc += 1;
            let advance_round_cmds = self
                .consensus
                .pacemaker
                .advance_round_tc(last_round_tc, self.epoch_manager, self.metrics)
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

        let (tc, remote_timeout_cmds) = self
            .consensus
            .pacemaker
            .process_remote_timeout::<VTF::ValidatorSetType>(
                validator_set,
                validator_mapping,
                &mut self.consensus.safety,
                &self.consensus.high_qc,
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
        if let Some(tc) = tc {
            info!(?tc, "Created TC");
            self.metrics.consensus_events.created_tc += 1;
            let advance_round_cmds = self
                .consensus
                .pacemaker
                .advance_round_tc(&tc, self.epoch_manager, self.metrics)
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
            cmds.extend(self.try_propose());
        }

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
        block: Block<SCT>,
        payload: Payload,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = vec![];

        let author_pubkey = self
            .val_epoch_map
            .get_cert_pubkeys(&block.epoch)
            .expect("epoch should be available for blocksync'd block")
            .map
            .get(&block.author)
            .expect("blocksync'd block author should be in validator set");
        let block = self
            .block_validator
            .validate(block, payload, author_pubkey)
            .expect("majority extended invalid block"); //FIXME: header payload could mismatch if
                                                        //they respond with incorrect thing. DONT
                                                        //UNWRAP THIS
        if self.consensus.pending_block_tree.is_valid_to_insert(&block) {
            let removed = self.consensus.block_sync_requests.remove(&block.get_id());
            if removed.is_none() {
                warn!(
                    ?block,
                    "invariant broken, received blocksync'd block that wasn't requested"
                );
                return cmds;
            }
            assert_eq!(removed, Some(block.get_round()));
            cmds.extend(self.request_block_if_missing_ancestor(block.get_qc()));

            let res_cmds = self
                .try_add_and_commit_blocktree(&block, None)
                .expect("majority extend incoherent block");
            cmds.extend(res_cmds);
        }
        cmds
    }

    /// If the qc has a commit_state_hash, commit the parent block and prune the
    /// block tree
    /// Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    pub fn process_qc(&mut self, qc: &QuorumCertificate<SCT>) -> Vec<ConsensusCommand<ST, SCT>> {
        trace!(?qc, our_high_qc = ?self.consensus.high_qc, "process qc");

        if Rank(qc.info) <= Rank(self.consensus.high_qc.info) {
            self.metrics.consensus_events.process_old_qc += 1;
            return Vec::new();
        }
        self.metrics.consensus_events.process_qc += 1;

        self.consensus.high_qc = qc.clone();
        let mut cmds = Vec::new();
        cmds.extend(self.try_commit(qc));

        // cancel any obsolete blocksync requests
        let to_cancel: Vec<_> = self
            .consensus
            .block_sync_requests
            .iter()
            .filter_map(|(block_id, round)| {
                if round <= &self.consensus.pending_block_tree.root().round {
                    Some(block_id)
                } else {
                    None
                }
            })
            .copied()
            .collect();
        for cancel in &to_cancel {
            self.consensus.block_sync_requests.remove(cancel);
        }
        cmds.extend(
            to_cancel
                .into_iter()
                .map(|block_id| ConsensusCommand::CancelSync { block_id }),
        );

        // statesync if too far from tip && have close enough committed block to statesync to
        cmds.extend(self.maybe_statesync());

        // start execution if close enough to tip
        cmds.extend(self.maybe_start_execution());

        // any time high_qc is updated, we generate a new checkpoint
        cmds.push(ConsensusCommand::CheckpointSave(self.checkpoint()));
        cmds
    }

    #[must_use]
    fn checkpoint(&self) -> Checkpoint<SCT> {
        let val_set_data = |epoch: Epoch| {
            let round = self.epoch_manager.epoch_starts.get(&epoch).copied();
            let validator_set = self.val_epoch_map.get_val_set(&epoch)?.get_members();

            let cert_pubkeys = self
                .val_epoch_map
                .get_cert_pubkeys(&epoch)
                .unwrap()
                .map
                .clone();
            let validators = cert_pubkeys
                .iter()
                .map(|(node_id, cert_pub_key)| {
                    let stake = validator_set
                        .get(node_id)
                        .expect("no validator_set for node_id");
                    ValidatorData {
                        node_id: *node_id,
                        stake: *stake,
                        cert_pubkey: *cert_pub_key,
                    }
                })
                .collect::<Vec<_>>();

            Some(ValidatorSetDataWithEpoch {
                epoch,
                round,
                validators: ValidatorSetData(validators),
            })
        };

        let base_epoch = self.consensus.pending_block_tree.root().epoch;
        Checkpoint {
            root: self.consensus.pending_block_tree.root().clone(),
            high_qc: self.consensus.high_qc.clone(),
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
    fn try_commit(&mut self, qc: &QuorumCertificate<SCT>) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = Vec::new();
        debug!(?qc, "try committing blocks using qc");

        if qc.info.vote.ledger_commit_info.is_commitable()
            && self
                .consensus
                .pending_block_tree
                .is_coherent(&qc.info.vote.vote_info.parent_id)
        {
            let blocks_to_commit = self
                .consensus
                .pending_block_tree
                .prune(&qc.info.vote.vote_info.parent_id);

            debug!(
                num_commits = ?blocks_to_commit.len(),
                "qc triggered commit"
            );

            if !blocks_to_commit.is_empty() {
                for block in blocks_to_commit.iter() {
                    // when epoch boundary block is committed, this updates
                    // epoch manager records
                    self.metrics.consensus_events.commit_block += 1;
                    self.block_policy.update_committed_block(block);
                    if !block.is_empty_block() {
                        self.epoch_manager
                            .schedule_epoch_start(block.get_seq_num(), block.get_round());
                    } else {
                        self.metrics.consensus_events.commit_empty_block += 1;
                    }
                    self.metrics.consensus_events.committed_bytes +=
                        block.get_txn_list_len() as u64;
                }

                let unvalidated_blocks = blocks_to_commit
                    .into_iter()
                    .map(|b| b.get_full_block())
                    .collect();
                cmds.push(ConsensusCommand::<ST, SCT>::LedgerCommit(
                    unvalidated_blocks,
                ));

                // enter new pacemaker epoch if committing the boundary block
                // bumps the current epoch
                cmds.extend(
                    self.consensus
                        .pacemaker
                        .advance_epoch(self.epoch_manager)
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
            }
        }
        cmds
    }

    #[must_use]
    fn process_certificate_qc(
        &mut self,
        qc: &QuorumCertificate<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = Vec::new();
        cmds.extend(self.process_qc(qc));

        cmds.extend(
            self.consensus
                .pacemaker
                .advance_round_qc(qc, self.epoch_manager, self.metrics)
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

        // if the qc points to a block that is missing from the blocktree, we need
        // to request it.
        cmds.extend(self.request_block_if_missing_ancestor(qc));

        // update vote_state round
        // it's ok if not leader for round; we will never propose
        self.consensus
            .vote_state
            .start_new_round(self.consensus.pacemaker.get_current_round());

        cmds
    }

    fn try_add_and_commit_blocktree(
        &mut self,
        block: &BPT::ValidatedBlock,
        state_root_action: Option<StateRootAction>,
    ) -> Result<Vec<ConsensusCommand<ST, SCT>>, BlockTreeError> {
        trace!(?block, "adding block to blocktree");
        // this shouldn't happen first
        if let Err(err) = self.consensus.pending_block_tree.add(
            block.clone(),
            self.block_policy,
            self.state_backend,
        ) {
            match err {
                BlockTreeError::StateBackendError(err) => {
                    // Block is still inserted into the tree, with is_coherent = false
                }
                BlockTreeError::BlockNotCoherent(bid) => {
                    return Err(BlockTreeError::BlockNotCoherent(bid));
                }
            }
        }

        let mut cmds = Vec::new();

        // commit any committable blocks to set the epoch manager correctly,
        // then try propose
        let high_commit_qc = self.consensus.pending_block_tree.get_high_committable_qc();
        if let Some(qc) = high_commit_qc {
            cmds.extend(self.try_commit(&qc));
        }

        // if the current round is the same as block round, try to vote. else, try to propose
        if block.get_round() == self.consensus.pacemaker.get_current_round() {
            // TODO: This block could be received via blocksync. Retrieve the block received
            // this round and try to vote for that
            let state_root_action =
                state_root_action.expect("should exist if proposal was handled this round");
            cmds.extend(self.try_vote(block, state_root_action));
        } else {
            cmds.extend(self.try_propose());
        }

        // statesync if too far from tip && have close enough committed block to statesync to
        cmds.extend(self.maybe_statesync());

        Ok(cmds)
    }

    #[must_use]
    fn maybe_start_execution(&mut self) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = Vec::new();
        // TODO make this lower start_execution threshold also configurable instead of overloading
        let execution_start_threshold = SeqNum(self.config.state_sync_threshold.0 / 2);
        if !self.consensus.started_execution
            && self.consensus.pending_block_tree.get_root_seq_num() + execution_start_threshold
                > self.consensus.high_qc.get_seq_num()
        {
            tracing::info!(
                root =? self.consensus.pending_block_tree.root(),
                high_qc =? self.consensus.high_qc,
                ?execution_start_threshold,
                "starting execution - close enough to tip",
            );
            cmds.push(ConsensusCommand::StartExecution);
            self.consensus.started_execution = true;
        }
        cmds
    }

    #[must_use]
    fn maybe_statesync(&mut self) -> Vec<ConsensusCommand<ST, SCT>> {
        if self.consensus.started_statesync {
            return Vec::new();
        }

        let high_qc_seq_num = self.consensus.high_qc.get_seq_num();
        if self.consensus.pending_block_tree.get_root_seq_num() + self.config.state_sync_threshold
            > high_qc_seq_num
        {
            return Vec::new();
        }

        let connected_blocks = self
            .consensus
            .pending_block_tree
            .get_parent_block_chain(&self.consensus.high_qc.get_block_id());

        // set consensus_root N-delay
        // set high_qc to N
        // execution will sync to N-2*delay using state_root in consensus_root

        assert!(self.config.state_sync_threshold > self.state_root_validator.get_delay());
        let max_committed_seq_num = high_qc_seq_num - self.state_root_validator.get_delay();

        let earliest_block_exists_and_is_not_empty = connected_blocks
            .first()
            .is_some_and(|block| !block.is_empty_block());
        if connected_blocks
            .first()
            .map(|block| block.get_seq_num())
            .unwrap_or(SeqNum::MAX)
            > max_committed_seq_num
            || !earliest_block_exists_and_is_not_empty
        {
            info!(
                branch_len = connected_blocks.len(),
                state_sync_threshold =? self.config.state_sync_threshold,
                ?high_qc_seq_num,
                "waiting for enough blocks to trigger statesync",
            );
            return Vec::new();
        }

        info!(
            branch_len = connected_blocks.len(),
            state_sync_threshold =? self.config.state_sync_threshold,
            ?high_qc_seq_num,
            "triggering statesync",
        );
        self.metrics.consensus_events.trigger_state_sync += 1;

        // Execution doesn't support resyncing upon falling behind
        assert!(
            !self.consensus.started_execution,
            "can't statesync after execution has been started, root={:?}, high_qc={:?}",
            self.consensus.pending_block_tree.root(),
            self.consensus.high_qc
        );

        let root_block = connected_blocks
            .iter()
            .find(|block| block.get_seq_num() == max_committed_seq_num)
            .expect("statesync block should exist");

        self.consensus.started_statesync = true;
        vec![ConsensusCommand::RequestStateSync {
            root: RootInfo {
                round: root_block.get_round(),
                seq_num: root_block.get_seq_num(),
                epoch: root_block.get_epoch(),
                block_id: root_block.get_id(),
                state_root: root_block.get_state_root(),
            },
            high_qc: self.consensus.high_qc.clone(),
        }]
    }

    #[must_use]
    fn try_vote(
        &mut self,
        validated_block: &BPT::ValidatedBlock,
        state_root_action: StateRootAction,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = Vec::new();
        let round = self.consensus.pacemaker.get_current_round();

        // check that the block is coherent
        if !self
            .consensus
            .pending_block_tree
            .is_coherent(&validated_block.get_id())
        {
            return cmds;
        }

        debug!(?round, block_id = ?validated_block.get_id(), ?state_root_action, "try vote");

        // StateRootAction::Defer means we don't have enough information to
        // decide if the state root hash is valid. It's ok to insert into the
        // block tree, but we can't vote on the block
        if matches!(state_root_action, StateRootAction::Defer) {
            return cmds;
        }

        assert!(matches!(state_root_action, StateRootAction::Proceed));

        // decide if its safe to cast vote on this proposal
        let last_tc = self.consensus.pacemaker.get_last_round_tc();
        let vote = self
            .consensus
            .safety
            .make_vote::<SCT, BPT, SBT>(validated_block, last_tc);

        debug!(?round, ?vote, "vote result");

        if let Some(v) = vote {
            let vote_msg = VoteMessage::<SCT>::new(v, self.cert_keypair);

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
                version: self.version.into(),
                message: ProtocolMessage::Vote(vote_msg),
            }
            .sign(self.keypair);
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(next_leader),
                message: msg,
            };
            debug!(?round, vote = ?v, ?next_leader, "created vote");
            self.metrics.consensus_events.created_vote += 1;
            cmds.push(send_cmd);
        }

        cmds
    }

    #[must_use]
    fn try_propose(&mut self) -> Vec<ConsensusCommand<ST, SCT>> {
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
        trace!(?round, ?leader, ?self.consensus.last_proposed_round, "try propose");

        // check that self is leader
        if self.nodeid != leader {
            return cmds;
        }

        // make sure we haven't proposed in this round
        if round <= self.consensus.last_proposed_round {
            return cmds;
        }

        // check that we have path to root and block is coherent
        if !self
            .consensus
            .pending_block_tree
            .is_coherent(&self.consensus.high_qc.get_block_id())
        {
            return cmds;
        }

        // we passed all try_propose guards, so begin proposing
        self.consensus.last_proposed_round = round;

        let last_round_tc = self.consensus.pacemaker.get_last_round_tc().clone();
        cmds.extend(self.process_new_round_event(last_round_tc));
        cmds
    }

    /// called when the node is entering a new round and is the leader for that round
    /// TODO this function can be folded into try_propose; it's only called there
    #[must_use]
    fn process_new_round_event(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let node_id = *self.nodeid;
        let round = self.consensus.pacemaker.get_current_round();
        let epoch = self
            .epoch_manager
            .get_epoch(round)
            .expect("higher epoch exists");
        let high_qc = self.consensus.high_qc.clone();

        let parent_bid = high_qc.get_block_id();
        let seq_num_qc = high_qc.get_seq_num();
        let try_propose_seq_num = seq_num_qc + SeqNum(1);

        let proposer_builder =
            |block_kind: BlockKind,
             txns: TransactionPayload,
             hash: StateRootHash,
             seq_num: SeqNum,
             last_round_tc: Option<TimeoutCertificate<SCT>>| {
                let payload = Payload { txns };
                let b = Block::new(
                    node_id,
                    self.block_timestamp
                        .get_valid_block_timestamp(high_qc.get_timestamp()),
                    epoch,
                    round,
                    &ExecutionProtocol {
                        state_root: hash,
                        seq_num,
                        beneficiary: *self.beneficiary,
                        randao_reveal: RandaoReveal::new::<SCT::SignatureType>(
                            round,
                            self.cert_keypair,
                        ),
                    },
                    payload.get_id(),
                    block_kind,
                    &high_qc,
                );

                let p = ProposalMessage {
                    block: b,
                    payload,
                    last_round_tc,
                };
                let msg = ConsensusMessage {
                    version: self.version.into(),
                    message: ProtocolMessage::Proposal(p),
                }
                .sign(self.keypair);

                vec![
                    ConsensusCommand::Publish {
                        target: RouterTarget::Raptorcast(epoch, round),
                        message: msg,
                    },
                    ConsensusCommand::ClearMempool,
                ]
            };

        match self.proposal_policy(try_propose_seq_num) {
            ConsensusAction::Propose(propose_state_root_hash) => {
                let _create_proposal_span =
                    tracing::info_span!("create_proposal_span", ?round).entered();

                // Propose when there's a path to root
                let pending_blocktree_blocks = self
                    .consensus
                    .pending_block_tree
                    .get_blocks_on_path_from_root(&parent_bid)
                    .expect("there should be a path to root");

                debug!(
                    ?node_id,
                    ?round,
                    ?high_qc,
                    ?try_propose_seq_num,
                    ?last_round_tc,
                    "Creating Proposal"
                );

                match self.tx_pool.create_proposal(
                    try_propose_seq_num,
                    self.config.proposal_txn_limit,
                    self.config.proposal_gas_limit,
                    self.block_policy,
                    pending_blocktree_blocks,
                    self.state_backend,
                ) {
                    Ok(prop_txns) => {
                        self.metrics.consensus_events.creating_proposal += 1;
                        proposer_builder(
                            BlockKind::Executable,
                            TransactionPayload::List(prop_txns),
                            propose_state_root_hash,
                            try_propose_seq_num,
                            last_round_tc,
                        )
                    }
                    Err(err) => {
                        // TODO: add metrics for different carriage cost validation errors
                        debug!(?err, "Creating Proposal error, proposing empty block");
                        self.metrics.consensus_events.creating_empty_block_proposal += 1;
                        // An empty block uses the same SeqNum and state root as its
                        // parent to facilitate consensus commit
                        let srh_qc = self
                            .consensus
                            .pending_block_tree
                            .get_block_state_root(&high_qc.get_block_id())
                            .expect("parent block is coherent");
                        proposer_builder(
                            BlockKind::Null,
                            TransactionPayload::Null,
                            srh_qc,
                            seq_num_qc,
                            last_round_tc,
                        )
                    }
                }
            }
            ConsensusAction::ProposeNull => {
                tracing::info_span!("create_proposal_empty_span", ?round);
                // Don't have the necessary state root hash ready so propose
                // a NULL block
                self.metrics.consensus_events.creating_empty_block_proposal += 1;
                debug!(
                    ?node_id,
                    ?round,
                    ?high_qc,
                    ?seq_num_qc,
                    ?last_round_tc,
                    "Creating Empty Proposal"
                );

                // An empty block uses the same SeqNum and state root as its
                // parent to facilitate consensus commit
                let srh_qc = self
                    .consensus
                    .pending_block_tree
                    .get_block_state_root(&high_qc.get_block_id())
                    .expect("parent block is coherent");
                proposer_builder(
                    BlockKind::Null,
                    TransactionPayload::Null,
                    srh_qc,
                    seq_num_qc,
                    last_round_tc,
                )
            }
        }
    }

    #[must_use]
    fn proposal_policy(&self, proposed_seq_num: SeqNum) -> ConsensusAction {
        // Can't propose txs without state root hash
        let Some(h) = self
            .state_root_validator
            .get_next_state_root(proposed_seq_num)
        else {
            // propose empty also needs a state root hash with it
            return ConsensusAction::ProposeNull;
        };

        ConsensusAction::Propose(h)
    }

    #[must_use]
    fn request_block_if_missing_ancestor(
        &mut self,
        qc: &QuorumCertificate<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        if self.consensus.started_statesync {
            // stop consensus blocksync after statesync is initiated
            return Vec::new();
        }
        let Some(qc) = self.consensus.pending_block_tree.get_missing_ancestor(qc) else {
            return Vec::new();
        };

        let already_requested = self
            .consensus
            .block_sync_requests
            .insert(qc.get_block_id(), qc.get_round())
            .is_some();

        if already_requested {
            return Vec::new();
        }

        vec![ConsensusCommand::RequestSync {
            block_id: qc.get_block_id(),
        }]
    }

    #[must_use]
    fn state_root_hash_validation(&mut self, p: &ProposalMessage<SCT>) -> StateRootAction {
        if p.block.is_empty_block() {
            debug!(block = ?p.block, "Received empty block");
            self.metrics.consensus_events.rx_empty_block += 1;
            return StateRootAction::Proceed;
        }
        match self
            .state_root_validator
            .validate(p.block.get_seq_num(), p.block.get_state_root())
        {
            // TODO-1 execution lagging too far behind should be a trigger for
            // something to try and catch up faster. For now, just wait
            StateRootResult::OutOfRange => {
                debug!(
                    "Proposal Message carries state root hash for a block higher than highest locally known, unable to verify"
                );
                self.metrics.consensus_events.rx_execution_lagging += 1;
                StateRootAction::Defer
            }
            // Don't vote and locally timeout if the proposed state root does
            // not match
            StateRootResult::Mismatch => {
                debug!("State root hash in proposal conflicts with local value");
                self.metrics.consensus_events.rx_bad_state_root += 1;
                StateRootAction::Reject
            }
            // Don't vote and locally timeout if we don't have enough
            // information to decide whether the state root is valid. It's still
            // safe to insert to the block tree if other checks passes. So we
            // don't need to request block sync if other blocks forms a QC on it
            StateRootResult::Missing => {
                debug!("Missing state root hash value locally, unable to verify");
                self.metrics.consensus_events.rx_missing_state_root += 1;
                StateRootAction::Defer
            }
            StateRootResult::Success => {
                debug!("Received Proposal Message with valid state root hash");
                self.metrics.consensus_events.rx_proposal += 1;
                StateRootAction::Proceed
            }
        }
    }

    #[must_use]
    fn proposal_certificate_handling(
        &mut self,
        p: &ProposalMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = vec![];

        let process_certificate_cmds = self.process_certificate_qc(&p.block.qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            debug!(?last_round_tc, "Handled proposal with TC");
            self.metrics.consensus_events.proposal_with_tc += 1;
            let advance_round_cmds = self
                .consensus
                .pacemaker
                .advance_round_tc(last_round_tc, self.epoch_manager, self.metrics)
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

        cmds
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Deref, time::Duration};

    use itertools::Itertools;
    use monad_consensus::{
        messages::{
            consensus_message::ProtocolMessage,
            message::{ProposalMessage, TimeoutMessage, VoteMessage},
        },
        pacemaker::PacemakerCommand,
        validation::signing::Verified,
    };
    use monad_consensus_types::{
        block::{Block, BlockKind, BlockPolicy, BlockType, PassthruBlockPolicy},
        block_validator::{BlockValidator, MockValidator},
        checkpoint::RootInfo,
        ledger::CommitResult,
        metrics::Metrics,
        payload::{
            FullTransactionList, MissingNextStateRoot, NopStateRoot, Payload, StateRoot,
            StateRootValidator, TransactionPayload,
        },
        quorum_certificate::QuorumCertificate,
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        state_root_hash::StateRootHash,
        timeout::Timeout,
        txpool::{MockTxPool, TxPool},
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
        },
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
    use monad_eth_block_validator::EthValidator;
    use monad_eth_testutil::make_tx;
    use monad_eth_tx::{EthFullTransactionList, EthSignedTransaction, EthTransaction};
    use monad_eth_txpool::EthTxPool;
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use monad_state_backend::{
        InMemoryBlockState, InMemoryState, InMemoryStateInner, StateBackend,
    };
    use monad_testutil::{
        proposal::ProposalGen,
        signing::{create_certificate_keys, create_keys, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{
        BlockId, Epoch, NodeId, Round, RouterTarget, SeqNum, Stake, GENESIS_SEQ_NUM,
    };
    use monad_validator::{
        epoch_manager::EpochManager,
        leader_election::LeaderElection,
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use reth_primitives::B256;
    use test_case::test_case;
    use tracing_test::traced_test;

    use crate::{
        timestamp::BlockTimestamp, ConsensusCommand, ConsensusConfig, ConsensusState,
        ConsensusStateWrapper,
    };

    const BASE_FEE: u128 = 1000;
    const GAS_LIMIT: u64 = 30000;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;
    type BlockValidatorType = MockValidator;
    type StateRootValidatorType = NopStateRoot;

    struct NodeContext<ST, SCT, BPT, SBT, BVT, VTF, SVT, LT, TT>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<SCT, SBT>,
        SBT: StateBackend,
        BVT: BlockValidator<SCT, BPT, SBT>,
        SVT: StateRootValidator,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT, SBT>,
    {
        consensus_state: ConsensusState<SCT, BPT, SBT>,

        metrics: Metrics,
        txpool: TT,
        epoch_manager: EpochManager,

        val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
        election: LT,
        version: &'static str,

        state_root_validator: SVT,
        block_validator: BVT,
        block_policy: BPT,
        state_backend: SBT,
        block_timestamp: BlockTimestamp,
        beneficiary: EthAddress,
        nodeid: NodeId<CertificateSignaturePubKey<ST>>,
        consensus_config: ConsensusConfig,

        keypair: ST::KeyPairType,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
    }

    impl<ST, SCT, BPT, SBT, BVT, VTF, SVT, LT, TT> NodeContext<ST, SCT, BPT, SBT, BVT, VTF, SVT, LT, TT>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<SCT, SBT>,
        SBT: StateBackend,
        BVT: BlockValidator<SCT, BPT, SBT>,
        SVT: StateRootValidator,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT, SBT>,
    {
        fn wrapped_state(
            &mut self,
        ) -> ConsensusStateWrapper<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT> {
            ConsensusStateWrapper {
                consensus: &mut self.consensus_state,

                metrics: &mut self.metrics,
                tx_pool: &mut self.txpool,
                epoch_manager: &mut self.epoch_manager,

                val_epoch_map: &self.val_epoch_map,
                election: &self.election,
                version: self.version,

                state_root_validator: &self.state_root_validator,
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
            p: ProposalMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            self.wrapped_state().handle_proposal_message(author, p)
        }

        fn handle_timeout_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: TimeoutMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            self.wrapped_state().handle_timeout_message(author, p)
        }

        fn handle_vote_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: VoteMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            self.wrapped_state().handle_vote_message(author, p)
        }

        fn handle_block_sync(
            &mut self,
            b: Block<SCT>,
            p: Payload,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            self.wrapped_state().handle_block_sync(b, p)
        }
    }

    struct EnvContext<ST, SCT, VTF, LT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        proposal_gen: ProposalGen<ST, SCT>,
        malicious_proposal_gen: ProposalGen<ST, SCT>,
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
        fn next_proposal_empty(&mut self) -> Verified<ST, ProposalMessage<SCT>> {
            self.proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                TransactionPayload::Null,
                StateRootHash::default(),
            )
        }

        fn next_proposal(
            &mut self,
            txn_list: FullTransactionList,
            state_root: StateRootHash,
        ) -> Verified<ST, ProposalMessage<SCT>> {
            self.proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                TransactionPayload::List(txn_list),
                state_root,
            )
        }

        // TODO come up with better API for making mal proposals relative to state of proposal_gen
        fn mal_proposal_empty(&mut self) -> Verified<ST, ProposalMessage<SCT>> {
            self.malicious_proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                TransactionPayload::List(FullTransactionList::new(vec![5].into())),
                StateRootHash::default(),
            )
        }

        // TODO come up with better API for making mal proposals relative to state of proposal_gen
        fn branch_proposal(
            &mut self,
            txn_list: FullTransactionList,
            state_root: StateRootHash,
        ) -> Verified<ST, ProposalMessage<SCT>> {
            self.malicious_proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                TransactionPayload::List(txn_list),
                state_root,
            )
        }

        fn next_tc(&mut self, epoch: Epoch) -> Vec<Verified<ST, TimeoutMessage<SCT>>> {
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
        BPT: BlockPolicy<SCT, SBT>,
        SBT: StateBackend,
        BVT: BlockValidator<SCT, BPT, SBT>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        SVT: StateRootValidator,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        TT: TxPool<SCT, BPT, SBT> + Clone,
    >(
        num_states: u32,
        valset_factory: VTF,
        election: LT,
        block_policy: impl Fn() -> BPT,
        state_backend: impl Fn() -> SBT,
        block_validator: impl Fn() -> BVT,
        txpool: TT,
        state_root: impl Fn() -> SVT,
    ) -> (
        EnvContext<ST, SCT, VTF, LT>,
        Vec<NodeContext<ST, SCT, BPT, SBT, BVT, VTF, SVT, LT, TT>>,
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

        let ctxs: Vec<NodeContext<ST, SCT, BPT, SBT, BVT, _, _, _, _>> = (0..num_states)
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
                    proposal_txn_limit: 5000,
                    proposal_gas_limit: 8_000_000,
                    delta: Duration::from_secs(1),
                    state_sync_threshold: SeqNum(100),
                    timestamp_latency_estimate_ms: 1,
                };
                let genesis_qc = QuorumCertificate::genesis_qc();
                let cs = ConsensusState::new(
                    &epoch_manager,
                    &consensus_config,
                    RootInfo {
                        round: genesis_qc.get_round(),
                        seq_num: genesis_qc.get_seq_num(),
                        epoch: genesis_qc.get_epoch(),
                        block_id: genesis_qc.get_block_id(),
                        state_root: StateRootHash(Hash([0xb; 32])),
                    },
                    genesis_qc,
                );

                NodeContext {
                    consensus_state: cs,

                    metrics: Metrics::default(),
                    txpool: txpool.clone(),
                    epoch_manager,

                    val_epoch_map,
                    election: election.clone(),
                    version: "TEST",

                    state_root_validator: state_root(),
                    block_validator: block_validator(),
                    block_policy: block_policy(),
                    state_backend: state_backend(),
                    block_timestamp: BlockTimestamp::new(1000, 1),
                    beneficiary: EthAddress::default(),
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
            proposal_gen: ProposalGen::<ST, SCT>::new(),
            malicious_proposal_gen: ProposalGen::<ST, SCT>::new(),
            keys,
            cert_keys,
            epoch_manager,
            val_epoch_map,
            election,
        };

        (env, ctxs)
    }

    fn generate_full_tx_list(eth_tx_list: Vec<EthSignedTransaction>) -> FullTransactionList {
        let eth_full_tx_list = EthFullTransactionList(
            eth_tx_list
                .into_iter()
                .map(|signed_txn| {
                    let sender_address = signed_txn.recover_signer().unwrap();
                    EthTransaction::from_signed_transaction(signed_txn, sender_address)
                })
                .collect(),
        );

        FullTransactionList::new(eth_full_tx_list.rlp_encode())
    }

    fn extract_vote_msgs<ST, SCT>(cmds: Vec<ConsensusCommand<ST, SCT>>) -> Vec<VoteMessage<SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
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

    fn extract_proposal_broadcast<ST, SCT>(
        cmds: Vec<ConsensusCommand<ST, SCT>>,
    ) -> ProposalMessage<SCT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.iter()
            .find_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::Raptorcast(_, _),
                    message,
                } => match &message.deref().deref().message {
                    ProtocolMessage::Proposal(p) => Some(p.clone()),
                    _ => None,
                },
                _ => None,
            })
            .expect(&format!("couldn't extract proposal: {:?}", cmds))
    }

    fn extract_blocksync_requests<ST, SCT>(cmds: Vec<ConsensusCommand<ST, SCT>>) -> Vec<BlockId>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { block_id } => Some(block_id),
                _ => None,
            })
            .collect()
    }

    fn find_vote_message<ST, SCT>(
        cmds: &[ConsensusCommand<ST, SCT>],
    ) -> Option<&ConsensusCommand<ST, SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.iter().find(|c| match c {
            ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(..),
                message,
            } => matches!(&message.deref().deref().message, ProtocolMessage::Vote(..)),
            _ => false,
        })
    }

    fn find_blocksync_request<ST, SCT>(
        cmds: &[ConsensusCommand<ST, SCT>],
    ) -> Option<&ConsensusCommand<ST, SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.iter()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. }))
    }

    fn find_commit_cmd<ST, SCT>(
        cmds: &[ConsensusCommand<ST, SCT>],
    ) -> Option<&ConsensusCommand<ST, SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)))
    }

    fn find_timestamp_update_cmd<ST, SCT>(
        cmds: &[ConsensusCommand<ST, SCT>],
    ) -> Option<&ConsensusCommand<ST, SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.iter()
            .find(|c| matches!(c, ConsensusCommand::TimestampUpdate(_)))
    }

    // genesis_qc start with "0" sequence number and Round(0)
    // hence round == seqnum if no round times out
    fn seqnum_to_round_no_tc(seq_num: SeqNum) -> Round {
        Round(seq_num.0)
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[traced_test]
    #[test]
    fn lock_qc_high() {
        let num_state = 4;
        let (env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );

        let mut wrapped_state = ctx[0].wrapped_state();
        assert_eq!(wrapped_state.consensus.high_qc.get_round(), Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
            seq_num: GENESIS_SEQ_NUM + SeqNum(1),
            timestamp: 0,
        };
        let v = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
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
        assert_eq!(wrapped_state.consensus.high_qc.get_round(), Round(0));

        let _ = wrapped_state.handle_vote_message(*v3.author(), *v3);
        assert_eq!(
            wrapped_state.consensus.high_qc.get_round(),
            expected_qc_high_round
        );
        assert_eq!(wrapped_state.metrics.consensus_events.vote_received, 3);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();
        let p1 = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());

        // local timeout for state in Round 1
        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        let _ = wrapped_state.consensus.pacemaker.handle_event(
            &mut wrapped_state.consensus.safety,
            &wrapped_state.consensus.high_qc,
        );

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

        let result = extract_vote_msgs(cmds);
        assert!(result.is_empty());
    }

    #[test]
    fn enter_proposalmsg_round() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let result = extract_vote_msgs(cmds);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        assert_eq!(result.len(), 1);

        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let result = extract_vote_msgs(cmds);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(2)
        );
        assert_eq!(result.len(), 1);

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
    fn duplicate_proposals() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let result = extract_vote_msgs(cmds);
        assert_eq!(result.len(), 1);

        // send duplicate of p1, expect it to be ignored and no output commands
        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        assert!(cmds.is_empty());
    }

    #[test]
    fn timestamp_update_only_for_higher_round() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // our initial starting logic has consensus in round 1 so the first proposal does not
        // increase the round
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);

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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // first proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let result = extract_vote_msgs(cmds);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(1)
        );
        assert_eq!(result.len(), 1);

        // second proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let result = extract_vote_msgs(cmds);

        assert_eq!(
            wrapped_state.consensus.pacemaker.get_current_round(),
            Round(2)
        );
        assert_eq!(result.len(), 1);

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
    fn test_commit_rule_consecutive() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // round 1 proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let p1_cmds = wrapped_state.handle_proposal_message(author, verified_message);

        let p1_votes = extract_vote_msgs(p1_cmds);
        assert!(p1_votes.len() == 1);
        assert!(p1_votes[0].vote.ledger_commit_info.is_commitable());

        // round 2 proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = wrapped_state.handle_proposal_message(author, verified_message);
        assert!(find_commit_cmd(&p2_cmds).is_none());

        let p2_votes = extract_vote_msgs(p2_cmds);
        assert!(p2_votes.len() == 1);
        // csh is some: the proposal and qc have consecutive rounds
        assert!(p2_votes[0].vote.ledger_commit_info.is_commitable());

        let p3 = env.next_proposal_empty();
        let (author, _, verified_message) = p3.destructure();

        let p2_cmds = wrapped_state.handle_proposal_message(author, verified_message);
        assert!(find_commit_cmd(&p2_cmds).is_some());
    }
    #[test]
    fn test_commit_rule_non_consecutive() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        // round 1 proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _ = wrapped_state.handle_proposal_message(author, verified_message);

        // round 2 proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = wrapped_state.handle_proposal_message(author, verified_message);

        let p2_votes = extract_vote_msgs(p2_cmds);
        assert!(p2_votes.len() == 1);
        assert!(p2_votes[0].vote.ledger_commit_info.is_commitable());

        // round 2 timeout
        let pacemaker_cmds = wrapped_state.consensus.pacemaker.handle_event(
            &mut wrapped_state.consensus.safety,
            &wrapped_state.consensus.high_qc,
        );

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
        assert_eq!(p3.block.qc.get_round(), Round(1));
        assert_eq!(p3.block.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let p3_cmds = wrapped_state.handle_proposal_message(author, verified_message);

        let p3_votes = extract_vote_msgs(p3_cmds);
        assert!(p3_votes.len() == 1);
        // proposal and qc have non-consecutive rounds
        assert!(!p3_votes[0].vote.ledger_commit_info.is_commitable());
    }

    // this test checks that a malicious proposal sent only to the next leader is
    // not incorrectly committed
    #[test]
    fn test_malicious_proposal_and_block_recovery() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (n1, xs) = ctx.split_first_mut().unwrap();
        let (n2, xs) = xs.split_first_mut().unwrap();
        let (n3, xs) = xs.split_first_mut().unwrap();
        let n4 = &mut xs[0];

        // first_state will send 2 different proposals, A and B. A will be sent to
        // the next leader, all other nodes get B.
        // effect is that nodes send votes for B to the next leader who thinks that
        // the "correct" proposal is A.

        let cp1 = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let mp1 = env.mal_proposal_empty();

        let (author_1, _, proposal_message_1) = cp1.destructure();
        let block_1 = proposal_message_1.block.clone();
        let payload_1 = proposal_message_1.payload.clone();
        let cmds1 = n1.handle_proposal_message(author_1, proposal_message_1.clone());
        let p1_votes = extract_vote_msgs(cmds1)[0];

        let cmds3 = n3.handle_proposal_message(author_1, proposal_message_1.clone());
        let p3_votes = extract_vote_msgs(cmds3)[0];

        let cmds4 = n4.handle_proposal_message(author_1, proposal_message_1);
        let p4_votes = extract_vote_msgs(cmds4)[0];

        let (mal_author_1, _, mal_proposal_message_1) = mp1.destructure();
        let cmds2 = n2.handle_proposal_message(mal_author_1, mal_proposal_message_1);
        let p2_votes = extract_vote_msgs(cmds2)[0];

        assert_eq!(p1_votes.vote, p3_votes.vote);
        assert_eq!(p1_votes.vote, p4_votes.vote);
        assert_ne!(p1_votes.vote, p2_votes.vote);
        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];
        // We Collected 4 votes, 3 of which are valid, 1 of which is not caused by byzantine leader.
        // First 3 (including a false vote) submitted would not cause a qc to form
        // but the last vote would cause a qc to form locally at second_state, thus causing
        // second state to realize its missing a block.
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
                let sync_bid = match res.unwrap() {
                    ConsensusCommand::RequestSync { block_id } => Some(block_id),
                    _ => None,
                }
                .unwrap();
                assert!(sync_bid == block_1.get_id());
            }
        }
        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(n2.consensus_state.high_qc.info.vote, votes[0].vote);

        // use the correct proposal gen to make next proposal and send it to second_state
        // this should cause it to emit the RequestBlockSync Message because the the QC parent
        // points to a different proposal (second_state has the malicious proposal in its blocktree)
        let cp2 = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_2, _, proposal_message_2) = cp2.destructure();
        let block_2 = proposal_message_2.block.clone();
        let payload_2 = proposal_message_2.payload.clone();
        let cmds2 = n2.handle_proposal_message(author_2, proposal_message_2.clone());
        // Blocksync already requested with the created QC
        assert!(find_blocksync_request(&cmds2).is_none());

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = n1.handle_proposal_message(author_2, proposal_message_2.clone());
        assert!(find_blocksync_request(&cmds1).is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_3, _, proposal_message_3) = cp3.destructure();
        let block_3 = proposal_message_3.block.clone();
        let payload_3 = proposal_message_3.payload.clone();

        let cmds2 = n2.handle_proposal_message(author_3, proposal_message_3.clone());
        let cmds1 = n1.handle_proposal_message(author_3, proposal_message_3);

        // second_state has the malicious block in the blocktree, so it will not be able to
        // commit anything
        assert_eq!(n2.consensus_state.pending_block_tree.size(), 3);
        assert!(find_commit_cmd(&cmds2).is_none());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(n1.consensus_state.pending_block_tree.size(), 2);
        assert!(find_commit_cmd(&cmds1).is_some());

        // a block sync request arrived, helping second state to recover
        let _ = n2.handle_block_sync(block_1, payload_1);

        // in the next round, second_state should recover and able to commit
        let cp4 = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_4, _, proposal_message_4) = cp4.destructure();

        let cmds2 = n2.handle_proposal_message(author_4, proposal_message_4.clone());
        // new block added should allow path_to_root properly, thus no more request sync
        assert!(find_blocksync_request(&cmds2).is_none());

        // second_state has the correct blocks, so expect to see a commit
        assert_eq!(n2.consensus_state.pending_block_tree.size(), 2);
        assert!(find_commit_cmd(&cmds2).is_some());

        let cmds1 = n1.handle_proposal_message(author_4, proposal_message_4.clone());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(n1.consensus_state.pending_block_tree.size(), 2);
        assert!(find_commit_cmd(&cmds1).is_some());

        // third_state only received proposal for round 1, and is missing proposal for round 2, 3, 4
        // feeding third_state with a proposal from round 4 should trigger a recursive behaviour to ask for blocks

        let cmds3 = n3.handle_proposal_message(author_4, proposal_message_4);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        let res = find_blocksync_request(&cmds3);
        let Some(ConsensusCommand::RequestSync { block_id: _ }) = res else {
            panic!("request sync is not found")
        };

        let cmds3 = n3.handle_block_sync(block_3.clone(), payload_3.clone());

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 3);
        let res = find_blocksync_request(&cmds3);
        let Some(ConsensusCommand::RequestSync { block_id: _ }) = res else {
            panic!("request sync is not found")
        };
        // repeated handling of the requested block should be ignored
        let cmds3 = n3.handle_block_sync(block_3, payload_3);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 3);
        assert!(find_blocksync_request(&cmds3).is_none());

        // arrival of proposal should also prevent block_sync_request from modifying the tree
        // the proposal repairs path to root and block4.qc commits block 1 and 2
        let cmds2 = n3.handle_proposal_message(author_2, proposal_message_2);
        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        assert!(find_blocksync_request(&cmds2).is_none());
        let commit_cmds = find_commit_cmd(&cmds2);
        assert!(commit_cmds.is_some());
        if let Some(ConsensusCommand::LedgerCommit(blocks)) = commit_cmds {
            assert_eq!(blocks.len(), 2);
        } else {
            unreachable!();
        }

        // request sync which did not arrive in time should be ignored.
        let cmds3 = n3.handle_block_sync(block_2, payload_2);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        assert!(find_blocksync_request(&cmds3).is_none());
    }

    #[test]
    fn test_receive_empty_block() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRoot,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(1)),
            || MockValidator,
            MockTxPool::default(),
            || StateRoot::new(SeqNum(1)),
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, verified_message);
        let vote = extract_vote_msgs(cmds);
        assert_eq!(vote.len(), 1);
        assert_eq!(wrapped_state.metrics.consensus_events.rx_empty_block, 1);
    }

    /// Test the behaviour of consensus when execution is lagging. This is tested
    /// by handling a non-empty proposal which is missing the state root hash (lagging execution)
    #[test]
    fn test_lagging_execution() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRoot,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(1)),
            || MockValidator,
            MockTxPool::default(),
            || StateRoot::new(SeqNum(1)),
        );
        let mut wrapped_state = ctx[0].wrapped_state();

        env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            StateRootHash::default(),
        );

        let p1 = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            StateRootHash::default(),
        );

        let (author, _, verified_message) = p1.destructure();
        let cmds: Vec<ConsensusCommand<NopSignature, MultiSig<NopSignature>>> =
            wrapped_state.handle_proposal_message(author, verified_message);

        // the proposal still gets processed: the node enters a new round, and
        // issues a request for the block it skipped over
        assert_eq!(cmds.len(), 6);
        assert!(matches!(cmds[0], ConsensusCommand::StartExecution));
        assert!(matches!(cmds[1], ConsensusCommand::CheckpointSave(_)));
        assert!(matches!(cmds[2], ConsensusCommand::EnterRound(_, _)));
        assert!(matches!(
            cmds[3],
            ConsensusCommand::Schedule { duration: _ }
        ));
        assert!(matches!(cmds[4], ConsensusCommand::RequestSync { .. }));
        assert!(matches!(cmds[5], ConsensusCommand::TimestampUpdate(_)));
        assert_eq!(
            wrapped_state.metrics.consensus_events.rx_execution_lagging,
            1
        );
    }

    /// Test consensus behavior when a state root hash is missing. This is
    /// tested by only updating consensus with one state root, then handling a
    /// proposal with state root lower than that
    #[test]
    fn test_missing_state_root() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRoot,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(5)),
            || MockValidator,
            MockTxPool::default(),
            || StateRoot::new(SeqNum(5)),
        );

        // add state root for the genesis block
        ctx[0]
            .state_root_validator
            .add_state_root(GENESIS_SEQ_NUM, StateRootHash::default());

        let mut wrapped_state = ctx[0].wrapped_state();

        // prepare 5 blocks
        for _ in 0..5 {
            let p = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, p) = p.destructure();
            let _cmds = wrapped_state.handle_proposal_message(author, p);
        }

        assert_eq!(wrapped_state.metrics.consensus_events.rx_proposal, 5);
        assert_eq!(wrapped_state.consensus.get_current_round(), Round(5));

        // only execution update for block 3 comes
        ctx[0]
            .state_root_validator
            .add_state_root(SeqNum(3), StateRootHash(Hash([0x08_u8; 32])));
        let mut wrapped_state = ctx[0].wrapped_state();

        // Block 11 carries the state root hash from executing block 6 the state
        // root hash is missing. The certificates are processed - consensus enters new round and commit blocks, but it doesn't vote
        let p = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            StateRootHash(Hash([0x06_u8; 32])),
        );
        let (author, _, p) = p.destructure();

        let cmds = wrapped_state.handle_proposal_message(author, p);

        assert_eq!(wrapped_state.consensus.get_current_round(), Round(6));
        assert_eq!(
            wrapped_state.metrics.consensus_events.rx_missing_state_root,
            1
        );
        assert_eq!(cmds.len(), 5);
        assert!(matches!(cmds[0], ConsensusCommand::LedgerCommit(_)));
        assert!(matches!(cmds[1], ConsensusCommand::CheckpointSave(_)));
        assert!(matches!(cmds[2], ConsensusCommand::EnterRound(_, _)));
        assert!(matches!(
            cmds[3],
            ConsensusCommand::Schedule { duration: _ }
        ));
        assert!(matches!(cmds[4], ConsensusCommand::TimestampUpdate(_)));
    }

    /// Test consensus behaviour of a leader who is supposed to propose
    /// the next round but does not have a recent enough state root hash
    #[traced_test]
    #[test]
    fn test_unavailable_state_root_during_proposal() {
        let num_state = 4;
        // MissingNextStateRoot forces the proposer's state root hash
        // to be unavailable
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            MissingNextStateRoot,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            MissingNextStateRoot::default,
        );
        let (n1, xs) = ctx.split_first_mut().unwrap();
        let (n2, xs) = xs.split_first_mut().unwrap();
        let (n3, xs) = xs.split_first_mut().unwrap();
        let n4 = &mut xs[0];

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds1 = n1.handle_proposal_message(author, verified_message.clone());
        let p1_votes = extract_vote_msgs(cmds1)[0];

        let cmds2 = n2.handle_proposal_message(author, verified_message.clone());
        let p2_votes = extract_vote_msgs(cmds2)[0];

        let cmds3 = n3.handle_proposal_message(author, verified_message.clone());
        let p3_votes = extract_vote_msgs(cmds3)[0];

        let cmds4 = n4.handle_proposal_message(author, verified_message);
        let p4_votes = extract_vote_msgs(cmds4)[0];

        let next_leader = {
            let node_id = *env.next_proposal_empty().author();
            if node_id == n1.nodeid {
                n1
            } else if node_id == n2.nodeid {
                n2
            } else if node_id == n3.nodeid {
                n3
            } else if node_id == n4.nodeid {
                n4
            } else {
                unreachable!("next leader should be one of the 4 nodes")
            }
        };

        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];
        for (i, vote) in votes.iter().enumerate().take(4) {
            let v = Verified::<SignatureType, VoteMessage<_>>::new(*vote, &env.keys[i]);
            let cmds = next_leader.handle_vote_message(*v.author(), *v);

            // after 2f + 1 votes, we expect that an empty proposal is created
            if i == 2 {
                let p = extract_proposal_broadcast(cmds);
                assert_eq!(p.payload.txns, TransactionPayload::Null);
                assert_eq!(
                    next_leader
                        .metrics
                        .consensus_events
                        .creating_empty_block_proposal,
                    1
                );
            }
        }
    }

    #[test]
    fn test_state_root_updates() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRoot,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(1)),
            || MockValidator,
            MockTxPool::default(),
            || StateRoot::new(SeqNum(1)),
        );
        let node = &mut ctx[0];

        // delay gap in setup is 1

        let p0 = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author, _, verified_message) = p0.destructure();

        let _ = node.handle_proposal_message(author, verified_message);

        let p1 = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            StateRootHash(Hash([0x99; 32])),
        );
        let (author, _, verified_message) = p1.destructure();
        // p1 has seq_num 1 and therefore requires state_root 0
        // the state_root 0's hash should be Hash([0x99; 32])
        node.state_root_validator
            .add_state_root(SeqNum(0), StateRootHash(Hash([0x99; 32])));

        let _ = node.handle_proposal_message(author, verified_message);

        // commit some blocks and confirm cleanup of state root hashes happened

        let p2 = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            StateRootHash(Hash([0xbb; 32])),
        );

        let (author, _, verified_message) = p2.destructure();
        // p2 should have seqnum 2 and therefore only require state_root 1
        node.state_root_validator
            .add_state_root(SeqNum(1), StateRootHash(Hash([0xbb; 32])));
        let p2_cmds = node.handle_proposal_message(author, verified_message);
        assert!(find_commit_cmd(&p2_cmds).is_some());

        let p3 = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            StateRootHash(Hash([0xcc; 32])),
        );

        let (author, _, verified_message) = p3.destructure();
        node.state_root_validator
            .add_state_root(SeqNum(2), StateRootHash(Hash([0xcc; 32])));
        let p3_cmds = node.handle_proposal_message(author, verified_message);
        assert!(find_commit_cmd(&p3_cmds).is_some());

        // Delay gap is 1 and we have received proposals with seq num 0, 1, 2, 3
        // state_root_validator had updates for 0, 1, 2
        //
        // Proposals with seq num 1 and 2 are committed, so expect 2 to remain
        // in the state_root_validator
        assert_eq!(2, node.state_root_validator.root_hashes.len());
        assert!(node
            .state_root_validator
            .root_hashes
            .contains_key(&SeqNum(2)));
    }

    #[test]
    fn test_fetch_uncommitted_block() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let node = &mut ctx[0];

        let cp1 = env.next_proposal_empty();
        let (author, _, verified_message) = cp1.destructure();
        let block_1 = verified_message.block.clone();
        let bid_correct = block_1.get_id();
        // requesting a block that's doesn't exists should yield None
        assert_eq!(
            node.consensus_state.fetch_uncommitted_block(&bid_correct),
            None
        );
        // assuming a proposal comes in, should allow it to be fetched as it is within pending block tree
        let _ = node.handle_proposal_message(author, verified_message);
        let full_block = node
            .consensus_state
            .fetch_uncommitted_block(&bid_correct)
            .unwrap();
        assert_eq!(full_block.get_id(), bid_correct);

        // you can also receive a branch, which would cause pending block tree retrieval to also be valid
        let bp1 = env.branch_proposal(
            FullTransactionList::new(vec![13, 32].into()),
            StateRootHash::default(),
        );

        let (author, _, verified_message) = bp1.destructure();
        let block_1 = verified_message.block.clone();
        let bid_branch = block_1.get_id();
        assert_eq!(
            node.consensus_state.fetch_uncommitted_block(&bid_branch),
            None
        );

        let _ = node.handle_proposal_message(author, verified_message);
        let full_block = node
            .consensus_state
            .fetch_uncommitted_block(&bid_branch)
            .unwrap();
        assert_eq!(full_block.get_id(), bid_branch);

        let mut ledger_blocks = Vec::new();
        // if a certain commit is triggered, then fetching block would fail
        for _ in 0..3 {
            let cp = env.next_proposal_empty();
            let (author, _, verified_message) = cp.destructure();
            let block = verified_message.block.clone();
            let bid = block.get_id();
            // requesting a block that's doesn't exists should yield None
            assert_eq!(node.consensus_state.fetch_uncommitted_block(&bid), None);
            // assuming a proposal comes in, should allow it to be fetched as it is within pending block tree

            let cmds = node.handle_proposal_message(author, verified_message);

            for cmd in cmds {
                if let ConsensusCommand::LedgerCommit(blocks) = cmd {
                    ledger_blocks.extend(blocks);
                }
            }
            let full_block = node.consensus_state.fetch_uncommitted_block(&bid).unwrap();
            assert_eq!(full_block.get_id(), bid);
        }
        assert!(ledger_blocks.iter().any(|x| x.get_id() == bid_correct));
        assert!(ledger_blocks.iter().all(|x| x.get_id() != bid_branch));
    }

    #[test_case(4; "4 participants")]
    #[test_case(5; "5 participants")]
    #[test_case(6; "6 participants")]
    #[test_case(7; "7 participants")]
    #[test_case(123; "123 participants")]
    fn test_observing_qc_through_votes(num_state: usize) {
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );

        for i in 0..8 {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
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
        let mut votes = vec![];
        let mut proposal_10_blockid = BlockId(Hash::default());
        for j in 0..2 {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = cp.destructure();
            for (i, node) in ctx.iter_mut().enumerate() {
                if node.nodeid != next_leader {
                    let cmds = node.handle_proposal_message(author, verified_message.clone());

                    if j == 1 {
                        proposal_10_blockid = verified_message.block.get_id();
                        let v = extract_vote_msgs(cmds);
                        assert_eq!(v.len(), 1);
                        assert_eq!(v[0].vote.vote_info.round, Round(10));
                        votes.push((node.nodeid, v[0]));
                    }
                } else {
                    leader_index = i;
                }
            }
        }
        let mut leader_state = ctx[leader_index].wrapped_state();
        for (i, (author, v)) in votes.iter().enumerate() {
            let cmds = leader_state.handle_vote_message(*author, *v);
            if i == (num_state * 2 / 3) {
                let req: Vec<_> = cmds
                    .into_iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert_eq!(req.len(), 1);
                assert_eq!(req[0], proposal_10_blockid);
            } else {
                let req: Vec<_> = cmds
                    .into_iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert_eq!(req.len(), 0);
            }
        }
    }

    #[test]
    fn test_observe_qc_through_tmo() {
        let num_state = 5;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());

            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut().skip(1) {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // observing a qc that link to root should not trigger anything
                assert!(find_blocksync_request(&cmds).is_none());
            }
            blocks.push(verified_message.block);
        }

        let (node0, xs) = ctx.split_first_mut().unwrap();
        let node1 = &mut xs[0];

        // now timeout someone
        let cmds = node1.wrapped_state().handle_timeout_expiry();
        let tmo: Vec<&Timeout<SignatureCollectionType>> = cmds
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
        assert_eq!(req[0], blocks[2].get_id());
    }

    #[test]
    fn test_observe_qc_through_proposal() {
        let num_state = 5;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (_, _, verified_message) = cp.destructure();
            blocks.push(verified_message.block);
        }
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());

        let (author, _, verified_message) = cp.destructure();
        let node = &mut ctx[1];
        let cmds = node.handle_proposal_message(author, verified_message);
        let req = extract_blocksync_requests(cmds);
        assert_eq!(req.len(), 1);
        assert_eq!(req[0], blocks[3].get_id());
    }

    // Expected behaviour when leader N+2 receives the votes for N+1 before receiving the proposal
    // for N+1 is that it creates the QC, but does not send the proposal until N+1 arrives.
    #[test]
    fn test_votes_with_missing_parent_block() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
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
                let cmds = node.handle_proposal_message(author, verified_message.clone());
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
                        target: RouterTarget::Broadcast(_, _),
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
        let p = extract_proposal_broadcast(cmds);
        assert_eq!(Round(missing_round + 1), p.block.get_round());
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let node = &mut ctx[0];

        let verified_p1 = env.next_proposal_empty();
        let (_, _, p1) = verified_p1.destructure();

        let _ = node.handle_proposal_message(p1.block.author, p1);
        assert_eq!(node.consensus_state.blocktree().size(), 1);

        let verified_p2 = env.next_proposal_empty();
        let (_, _, p2) = verified_p2.destructure();

        let epoch = env.epoch_manager.get_epoch(Round(4)).expect("epoch exists");
        let invalid_author = env.election.get_leader(
            Round(4),
            env.val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        assert!(invalid_author != node.nodeid);
        assert!(invalid_author != p2.block.author);
        let invalid_b2 = Block::new(
            invalid_author,
            0,
            epoch,
            p2.block.round,
            &p2.block.execution,
            p2.payload.get_id(),
            BlockKind::Executable,
            &p2.block.qc,
        );
        let invalid_p2 = ProposalMessage {
            block: invalid_b2,
            payload: p2.payload,
            last_round_tc: None,
        };

        let _ = node.handle_proposal_message(invalid_p2.block.author, invalid_p2);

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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // handle update_block + 2 proposals to commit the update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());

            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
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

            blocks.push(verified_message.block);
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
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
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> =
                    node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
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
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
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

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );

        let mut blocks = vec![];
        // Sequence number of the block which updates the validator set
        let update_block_num = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block_num);

        // handle blocks until update_block - 1
        for _ in 0..(update_block_round.0 - 1) {
            let proposal =
                env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = proposal.destructure();
            for node in ctx.iter_mut() {
                let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> =
                    node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
        }

        let mut block_sync_blocks = Vec::new();
        // handle 3 blocks for only state 0
        for _ in (update_block_round.0 - 1)..(update_block_round.0 + 2) {
            let proposal =
                env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            println!("proposal seq num {:?}", proposal.block.execution.seq_num);
            let (author, _, verified_message) = proposal.destructure();

            let state = &mut ctx[0];
            let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> =
                state.handle_proposal_message(author, verified_message.clone());
            // state should not request blocksync
            assert!(extract_blocksync_requests(cmds).is_empty());

            block_sync_blocks.push((verified_message.block, verified_message.payload));
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
        let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> =
            state.handle_proposal_message(author, verified_message.clone());
        // state 1 should request blocksync
        assert_eq!(extract_blocksync_requests(cmds).len(), 1);

        blocks.push(verified_message.block);

        let val_set = env.val_epoch_map.get_val_set(&Epoch(1)).unwrap();
        let nodeid_0 = ctx[0].nodeid;
        let state1 = &mut ctx[1];
        for (block, payload) in block_sync_blocks.into_iter().rev() {
            // blocksync response for state 2
            let _ = state1.handle_block_sync(block, payload);
        }

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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        // handle proposals until expected_epoch_start_round - 1
        for _ in 0..(expected_epoch_start_round.0 - 1) {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());

            let (author, _, verified_message) = cp.destructure();
            for node in ctx.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
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
        let _unused_proposal =
            env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        ();
        // generate proposal for expected_epoch_start_round + 1
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let p1 = env.next_proposal_empty();
        // there's no child block in the blocktree, so this must be ignored
        // (because invariant is broken)
        let mut wrapped_state = ctx[0].wrapped_state();
        let cmds = wrapped_state.handle_block_sync(p1.block.clone(), p1.payload.clone());
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
    fn test_vote_sent_to_leader_in_next_epoch() {
        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
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
        let update_block = env.epoch_manager.val_set_update_interval;
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
            let cmds = node.handle_proposal_message(author, verified_message.clone());
            // state should send vote to the leader in epoch 2
            let vote_messages = extract_vote_msgs(cmds);
            assert_eq!(vote_messages.len(), 1);
        }
    }

    // ignored because statesync trigger is more complicated... it needs a valid root before
    #[ignore]
    #[test]
    fn test_request_over_state_sync_threshold() {
        let num_states = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block after which state sync should be triggered
        let state_sync_threshold_block = ctx[0].consensus_config.state_sync_threshold;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let state_sync_threshold_round = Round(state_sync_threshold_block.0);

        let (n1, other_states) = ctx.split_first_mut().unwrap();

        // handle proposals for 3 states until state_sync_threshold
        for _ in 0..state_sync_threshold_round.0 {
            let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            let (author, _, verified_message) = cp.destructure();
            for node in other_states.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
        }

        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author, _, verified_message) = cp.destructure();
        let _cmds = n1.handle_proposal_message(author, verified_message);

        // Should trigger state sync (only metric assertion for now)
        assert_eq!(n1.metrics.consensus_events.trigger_state_sync, 1);
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (n2, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // vote for block 1
        assert!(find_vote_message(&cmds).is_some());
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
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block.get_id();

        // generate block 3
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block.get_id();

        // state receives block 3
        let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
        // should not vote for block 3
        assert!(find_vote_message(&cmds).is_none());
        assert!(find_blocksync_request(&cmds).is_some());
        // block 3 should be in the blocktree as not coherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(!block_3_blocktree_entry.is_coherent);

        // state receives block 2
        if through_blocksync {
            // blocksync response for state 2
            let _ = n1.handle_block_sync(proposal_message_2.block, proposal_message_2.payload);
        } else {
            let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
            // should not vote for block or blocksync
            assert!(find_vote_message(&cmds).is_none());
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
    fn test_update_coherency_of_missed_block(through_blocksync: bool) {
        // Scenario and expected result:
        // State 1 receives Block 1. It should store Block 1 as validated in blocktree and send a vote message
        // State 1 receives Block 4. It should request blocksync for Block 3, skip voting, and store Block 4
        //      as unvalidated in blocktree
        // State 1 receives Block 3 (blocksync/out-of-order proposal [ARGUMENT]). It should request blocksync
        //      for Block 2, and store Block 3 as unvalidated in blocktree
        // State 1 receives Block 2. It should store Block 2 as validated in blocktree, and skip voting.
        //      Block 3/4 has a path to root via Block 2. It should be validated. Block 2 is committed

        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            StateBackendType,
            BlockValidatorType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || MockValidator,
            MockTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (n2, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // vote for block 1
        assert!(find_vote_message(&cmds).is_some());
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
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block.get_id();

        // generate block 3
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block.get_id();

        // generate block 4
        let cp = env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
        let (author_4, _, proposal_message_4) = cp.destructure();
        let block_4_id = proposal_message_4.block.get_id();

        // state receives block 4
        let cmds = n1.handle_proposal_message(author_4, proposal_message_4);
        // should not vote for block 4
        assert!(find_vote_message(&cmds).is_none());
        assert!(find_blocksync_request(&cmds).is_some());
        // block 4 should be in the blocktree as not coherent
        let block_4_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_4_id)
            .expect("should be in the blocktree");
        assert!(!block_4_blocktree_entry.is_coherent);

        // state receives block 3
        if through_blocksync {
            // blocksync response for state 3
            let cmds = n1.handle_block_sync(proposal_message_3.block, proposal_message_3.payload);
            // request blocksync for block 2
            assert!(find_blocksync_request(&cmds).is_some());
        } else {
            let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
            // should not vote for block or blocksync
            assert!(find_vote_message(&cmds).is_none());
            // request blocksync for block 2
            assert!(find_blocksync_request(&cmds).is_some());
        }
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
        if through_blocksync {
            // blocksync response for state 2
            let cmds = n1.handle_block_sync(proposal_message_2.block, proposal_message_2.payload);
            assert!(find_blocksync_request(&cmds).is_none());
            assert!(find_commit_cmd(&cmds).is_some());
            if let ConsensusCommand::LedgerCommit(commit) = find_commit_cmd(&cmds).unwrap() {
                assert_eq!(commit.len(), 2);
                // Commit block 1
                assert_eq!(commit[0].get_seq_num(), GENESIS_SEQ_NUM + SeqNum(1));
                // Commit block 2
                assert_eq!(commit[1].get_seq_num(), GENESIS_SEQ_NUM + SeqNum(2));
            }
        } else {
            let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
            // should not vote for block or blocksync
            assert!(find_vote_message(&cmds).is_none());
            assert!(find_blocksync_request(&cmds).is_none());
            assert!(find_commit_cmd(&cmds).is_some());
            if let ConsensusCommand::LedgerCommit(commit) = find_commit_cmd(&cmds).unwrap() {
                assert_eq!(commit.len(), 2);
                // Commit block 1
                assert_eq!(commit[0].get_seq_num(), GENESIS_SEQ_NUM + SeqNum(1));
                // Commit block 2
                assert_eq!(commit[1].get_seq_num(), GENESIS_SEQ_NUM + SeqNum(2));
            }
        }

        // block 3 should still be in the blocktree as coherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(block_3_blocktree_entry.is_coherent);

        // block 4 should still be in the blocktree as coherent
        let block_4_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_4_id)
            .expect("should be in the blocktree");
        assert!(block_4_blocktree_entry.is_coherent);
    }

    /// Coherency check for a block with a valid first transaction from an account (nonce = 0) should
    /// pass and the state should vote for the block
    #[test]
    fn test_coherent_block_zero_nonce() {
        let num_states = 2;
        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            EthBlockPolicy,
            InMemoryState,
            EthValidator,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    u128::MAX,
                    NopStateRoot {}.get_delay().0,
                    0,
                    1337,
                )
            },
            || {
                InMemoryStateInner::new(
                    u128::MAX,
                    SeqNum(4),
                    InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
                )
            },
            || EthValidator::new(10000, u64::MAX, 1337),
            EthTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (_, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_nonce_zero]),
            StateRootHash::default(),
        );
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // vote for block 1
        assert!(find_vote_message(&cmds).is_some());
        // block 1 should be in the blocktree as coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);
    }

    /// Coherency check for a block with invalid first transaction should fail and
    /// state should not vote for the block
    #[test]
    fn test_incoherent_block_invalid_nonce() {
        let num_states = 2;
        let sender_1_key = B256::random();
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);
        let sender_1_address = EthAddress(txn_nonce_one.recover_signer().unwrap());

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            EthBlockPolicy,
            InMemoryState,
            EthValidator,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    u128::MAX,
                    NopStateRoot {}.get_delay().0,
                    0,
                    1337,
                )
            },
            || {
                InMemoryStateInner::new(
                    u128::MAX,
                    SeqNum(4),
                    InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
                )
            },
            || EthValidator::new(10000, u64::MAX, 1337),
            EthTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (_, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(
            // first nonce from an account should be 0. this tx list is invalid since the transaction
            // has nonce = 1
            generate_full_tx_list(vec![txn_nonce_one]),
            StateRootHash::default(),
        );
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // should not vote for block 1
        assert!(find_vote_message(&cmds).is_none());
        // block 1 should be in the blocktree as non coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(!block_1_blocktree_entry.is_coherent);
    }

    #[traced_test]
    #[test]
    fn test_incoherent_block_duplicate_nonce() {
        let num_states = 2;
        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        let txn_nonce_zero_prime = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 1000);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            EthBlockPolicy,
            InMemoryState,
            EthValidator,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    u128::MAX,
                    NopStateRoot {}.get_delay().0,
                    0,
                    1337,
                )
            },
            || {
                InMemoryStateInner::new(
                    u128::MAX,
                    SeqNum(4),
                    InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
                )
            },
            || EthValidator::new(10000, u64::MAX, 1337),
            EthTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (_, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_nonce_zero]),
            StateRootHash::default(),
        );
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // should vote for block 1
        assert!(find_vote_message(&cmds).is_some());
        // block 1 should be in the blocktree as coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);

        // state receives incoherent block 2
        let cp = env.next_proposal(
            // next nonce from this account must be 1. this tx is invalid since it has nonce = 0
            generate_full_tx_list(vec![txn_nonce_zero_prime]),
            StateRootHash::default(),
        );
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block.get_id();

        let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
        // should not vote for block 2
        assert!(find_vote_message(&cmds).is_none());
        // block 2 should be in the blocktree as incoherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_id)
            .expect("should be in the blocktree");
        assert!(!block_2_blocktree_entry.is_coherent);
    }

    #[traced_test]
    #[test]
    fn test_coherent_block_valid_nonce() {
        let num_states = 2;

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        let txn_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);
        let txn_nonce_two = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 2, 10);
        let txn_nonce_three = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 3, 10);

        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            EthBlockPolicy,
            InMemoryState,
            EthValidator,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    u128::MAX,
                    NopStateRoot {}.get_delay().0,
                    0,
                    1337,
                )
            },
            || {
                InMemoryStateInner::new(
                    u128::MAX,
                    SeqNum(4),
                    InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
                )
            },
            || EthValidator::new(10000, u64::MAX, 1337),
            EthTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (_, _) = other_states.split_first_mut().unwrap();

        // state receives block 1 with nonce zero txn
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_nonce_zero]),
            StateRootHash::default(),
        );
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // should vote for block 1
        assert!(find_vote_message(&cmds).is_some());
        // block 1 should be in the blocktree as coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);

        // state receives block 2 with txns with nonce one and two
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_nonce_one, txn_nonce_two]),
            StateRootHash::default(),
        );
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block.get_id();

        let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
        // should vote for block 2
        assert!(find_vote_message(&cmds).is_some());
        // block 2 should be in the blocktree as coherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_id)
            .expect("should be in the blocktree");
        assert!(block_2_blocktree_entry.is_coherent);

        // state receives block 3 with txns with nonce three
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_nonce_three]),
            StateRootHash::default(),
        );
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block.get_id();

        let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
        // should vote for block 3
        assert!(find_vote_message(&cmds).is_some());
        // should commit block 1
        assert!(find_commit_cmd(&cmds).is_some());
        // block 3 should be in the blocktree as coherent
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(block_3_blocktree_entry.is_coherent);

        // block policy should be updated with latest account nonces from block 1
        assert_eq!(
            n1.block_policy
                .get_account_base_nonces(
                    SeqNum(4),
                    &n1.state_backend,
                    &Vec::<&EthValidatedBlock<SignatureCollectionType>>::default(),
                    std::iter::once(&sender_1_address),
                )
                .unwrap()
                .first_key_value()
                .unwrap()
                .1,
            &1
        );
    }

    #[test]
    fn test_branched_coherent_block_valid_nonce() {
        let num_states = 2;

        let sender_1_key = B256::random();
        let txn_nonce_zero = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        let txn_1_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 10);
        let txn_2_nonce_one = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 1, 1000);
        let sender_1_address = EthAddress(txn_nonce_zero.recover_signer().unwrap());

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            EthBlockPolicy,
            InMemoryState,
            EthValidator,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    u128::MAX,
                    NopStateRoot {}.get_delay().0,
                    0,
                    1337,
                )
            },
            || {
                InMemoryStateInner::new(
                    u128::MAX,
                    SeqNum(4),
                    InMemoryBlockState::genesis(std::iter::once((sender_1_address, 0)).collect()),
                )
            },
            || EthValidator::new(10000, u64::MAX, 1337),
            EthTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (_, _) = other_states.split_first_mut().unwrap();

        // state receives block 1 with nonce zero txn
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_nonce_zero]),
            StateRootHash::default(),
        );
        let (author_1, _, proposal_message_1) = cp.destructure();
        let block_1_id = proposal_message_1.block.get_id();

        let cmds = n1.handle_proposal_message(author_1, proposal_message_1);
        // should vote for block 1
        assert!(find_vote_message(&cmds).is_some());
        // block 1 should be in the blocktree as coherent
        let block_1_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_1_id)
            .expect("should be in the blocktree");
        assert!(block_1_blocktree_entry.is_coherent);

        // state receives block 2 with nonce one txn
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_1_nonce_one]),
            StateRootHash::default(),
        );
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_round_2_id = proposal_message_2.block.get_id();

        let cmds = n1.handle_proposal_message(author_2, proposal_message_2);
        // should vote for block 2
        assert!(find_vote_message(&cmds).is_some());
        // block 2 should be in the blocktree as coherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_round_2_id)
            .expect("should be in the blocktree");
        assert!(block_2_blocktree_entry.is_coherent);

        // the round times out, so the QC for block 2 is not created
        let _timeouts = env.next_tc(Epoch(1));

        // state receives block 2 (in round 3) with nonce one txn
        let cp = env.next_proposal(
            generate_full_tx_list(vec![txn_2_nonce_one]),
            StateRootHash::default(),
        );
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_2_round_3_id = proposal_message_3.block.get_id();

        let cmds = n1.handle_proposal_message(author_3, proposal_message_3);
        // should vote for block 2
        assert!(find_vote_message(&cmds).is_some());
        // block 2 from round 3 should be in the blocktree as coherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_round_3_id)
            .expect("should be in the blocktree");
        assert!(block_2_blocktree_entry.is_coherent);

        // block 2 from round 2 should also be in the blocktree as coherent
        let block_2_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_2_round_2_id)
            .expect("should be in the blocktree");
        assert!(block_2_blocktree_entry.is_coherent);
    }

    /// Asserts that pacemaker epoch is brought to sync if blocktree commits
    /// bump the current epoch
    ///
    /// 1. The node receives all the blocks in epoch 1 except the boundary
    ///    block. These blocks bring it to the first round of epoch(2) but the
    ///    node is still on epoch(1) as it hasn't committed the boundary block.
    /// 2. Node receives boundary block and commits it. Assert that pacemaker
    ///    epoch is bumped to epoch(2)
    #[test]
    fn test_pacemaker_advance_epoch_on_blocktree_commit() {
        let num_states = 4;

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            EthBlockPolicy,
            InMemoryState,
            EthValidator,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    u128::MAX,
                    NopStateRoot {}.get_delay().0,
                    0,
                    1337,
                )
            },
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || EthValidator::new(10000, u64::MAX, 1337),
            EthTxPool::default(),
            || NopStateRoot,
        );

        let epoch_length = ctx[0].epoch_manager.val_set_update_interval;
        let epoch_start_delay = ctx[0].epoch_manager.epoch_start_delay;
        // we don't want to start execution, so set statesync threshold very high
        ctx[0].consensus_config.state_sync_threshold = SeqNum(u32::MAX.into());

        // enter the first round of epoch(2) via a QC. Node is unaware of the epoch change because of missing boundary block
        // generate many proposal, skips the boundary block proposal
        let mut proposals = Vec::new();
        while proposals.len() < 2 * epoch_length.0 as usize {
            let proposal =
                env.next_proposal(FullTransactionList::empty(), StateRootHash::default());
            proposals.push(proposal);
        }

        let boundary_block_pos = proposals
            .iter()
            .position(|p| p.block.get_seq_num().is_epoch_end(epoch_length))
            .expect("boundary block in vector");
        let boundary_block = proposals.remove(boundary_block_pos);
        let epoch_2_start = boundary_block.block.get_round() + epoch_start_delay;
        proposals.retain(|p| p.block.get_round() <= epoch_2_start);

        // populate the next validator set
        let valset = ctx[0]
            .val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members()
            .iter()
            .map(|(node, stake)| (*node, *stake))
            .collect_vec();
        let cert_pubkeys = ValidatorMapping::new(
            ctx[0]
                .val_epoch_map
                .get_cert_pubkeys(&(Epoch(1)))
                .unwrap()
                .map
                .iter()
                .map(|(node, pubkey)| (*node, *pubkey)),
        );

        ctx[0].val_epoch_map.insert(Epoch(2), valset, cert_pubkeys);

        let (ctx0, ctx) = ctx.split_first_mut().unwrap();
        let mut wrapped_state = ctx0.wrapped_state();

        for verified_proposal in proposals {
            let (author, _, proposal) = verified_proposal.destructure();
            let _ = wrapped_state.handle_proposal_message(author, proposal);
        }

        // the node hasn't received the boundary block, so it is still in epoch(1)
        assert_eq!(wrapped_state.consensus.get_current_epoch(), Epoch(1));
        assert_eq!(wrapped_state.consensus.get_current_round(), epoch_2_start);

        // receives boundary block, commits the entire block tree, enters new epoch
        let (author, _, proposal) = boundary_block.destructure();
        let cmds = wrapped_state.handle_proposal_message(author, proposal);
        let enter_round_cmd = cmds
            .iter()
            .find(|cmd| matches!(cmd, ConsensusCommand::EnterRound(_, _)));
        assert!(enter_round_cmd.is_some());
        if let Some(ConsensusCommand::EnterRound(epoch, round)) = enter_round_cmd {
            assert_eq!(epoch, &Epoch(2));
            assert_eq!(round, &epoch_2_start);
        } else {
            unreachable!();
        }
        assert_eq!(wrapped_state.consensus.get_current_epoch(), Epoch(2));
        assert_eq!(wrapped_state.consensus.get_current_round(), epoch_2_start);
    }
}
