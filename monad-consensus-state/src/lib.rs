use std::time::Duration;

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{BlockSyncResponseMessage, ProposalMessage, TimeoutMessage, VoteMessage},
    },
    pacemaker::{Pacemaker, PacemakerCommand},
    validation::safety::Safety,
    vote_state::VoteState,
};
use monad_consensus_types::{
    block::{Block, BlockType, UnverifiedBlock},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::{
        ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal, StateRootResult,
        StateRootValidator, INITIAL_DELAY_STATE_ROOT_HASH,
    },
    quorum_certificate::{QuorumCertificate, Rank},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHash,
    timeout::TimeoutCertificate,
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_types::{BlockId, Epoch, NodeId, Round, RouterTarget, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use tracing::{debug, warn};

use crate::{
    blocksync::{BlockSyncRequester, BlockSyncResult},
    command::ConsensusCommand,
};

pub mod blocksync;
pub mod command;
pub mod wrapper;

/// core consensus algorithm
pub struct ConsensusState<ST, SCT, BV, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// This nodes public NodeId; what other nodes see in the validator set
    nodeid: NodeId<SCT::NodeIdPubKey>,
    /// Parameters for consensus algorithm behaviour
    config: ConsensusConfig,
    /// Prospective blocks are stored here while they wait to be
    /// committed
    pending_block_tree: BlockTree<SCT>,
    /// State machine to track collected votes for proposals
    vote_state: VoteState<SCT>,
    /// The highest QC (QC height determined by Round) known to this node
    high_qc: QuorumCertificate<SCT>,
    /// Policy for validating the state root hashes included in proposals
    state_root_validator: SVT,
    /// Tracks and updates the current round
    pacemaker: Pacemaker<SCT>,
    /// Policy for upholding consensus safety when voting or extending branches
    safety: Safety,
    /// Policy for validating incoming proposals
    block_validator: BV,
    /// State machine used to request missing blocks from previous rounds
    block_sync_requester: BlockSyncRequester<ST, SCT>,
    /// Destination address for proposal payments
    beneficiary: EthAddress,

    // TODO-2 deprecate keypairs should probably have a different interface
    // so that users have options for securely storing their keys
    keypair: ST::KeyPairType,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,

    last_proposed_round: Round,
}

/// Consensus algorithm's configurable parameters
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct ConsensusConfig {
    /// Maximum number of transactions allowed in a proposal
    pub proposal_txn_limit: usize,
    /// Maximum cumulative gas allowed for all transactions in a proposal
    pub proposal_gas_limit: u64,
    /// If the current leader has the highest qc but is missing some blocks in the
    /// pending blocktree (ie, there isn't a path to the root of the tree from the
    /// highest qc), this bool controls whether we still try to propose a block with
    /// transactions instead of proposing an empty block
    pub propose_with_missing_blocks: bool,
    /// Duration used by consensus to determine timeout lengths
    /// delta should be approximately equal to the upper bound of message
    /// delivery during a broadcast
    pub delta: Duration,
    /// Maximum number of blocksync retries allowed per block
    pub max_blocksync_retries: usize,
    /// If the node is lagging over state_sync_threshold blocks behind,
    /// then it should trigger statesync.
    /// Lagging (0, state_sync_threshold) blocks: request blocksync
    /// Lagging  >= state_sync_threshold  blocks: trigger statesync
    pub state_sync_threshold: SeqNum,
}

impl<ST, SCT, BVT, SVT> PartialEq for ConsensusState<ST, SCT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn eq(&self, other: &Self) -> bool {
        self.pending_block_tree.eq(&other.pending_block_tree)
            && self.vote_state.eq(&other.vote_state)
            && self.high_qc.eq(&other.high_qc)
            && self.pacemaker.eq(&other.pacemaker)
            && self.safety.eq(&other.safety)
            && self.nodeid.eq(&other.nodeid)
            && self.config.eq(&other.config)
            && self.block_sync_requester.eq(&other.block_sync_requester)
    }
}
impl<ST, SCT, BVT, SVT> Eq for ConsensusState<ST, SCT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
}

impl<ST, SCT, BVT, SVT> std::fmt::Debug for ConsensusState<ST, SCT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusState")
            .field("pending_block_tree", &self.pending_block_tree)
            .field("vote_state", &self.vote_state)
            .field("high_qc", &self.high_qc)
            .field("pacemaker", &self.pacemaker)
            .field("safety", &self.safety)
            .field("nodeid", &self.nodeid)
            .field("config", &self.config)
            .field("block_sync_requester", &self.block_sync_requester)
            .finish()
    }
}

/// Possible actions a leader node can take when entering a new round
pub enum ConsensusAction {
    /// Create a proposal with this state-root-hash and txn hash list
    Propose(StateRootHash, Vec<FullTransactionList>),
    /// Create an empty block proposal
    ProposeEmpty,
    /// Do nothing which will lead to the round timing out
    Abstain,
}

/// Actions after state root validation
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

impl<ST, SCT, BVT, SVT> ConsensusState<ST, SCT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator,
    SVT: StateRootValidator,
{
    /// Create the core consensus state
    ///
    /// Arguments
    ///
    /// block_validator - validation for incoming proposals
    /// my_pubkey - pubkey for NodeId used to identify this Node to the network
    /// config - collection of configurable parameters for core consensus algorithm
    /// beneficiary - Eth format address to deliver proposer rewards to
    /// keypair - keypair used for protocol level message signing
    /// cert_keypair - keypair used for certificate level signing
    pub fn new(
        block_validator: BVT,
        state_root_validator: SVT,
        my_pubkey: SCT::NodeIdPubKey,
        config: ConsensusConfig,
        beneficiary: EthAddress,

        // TODO-2 deprecate
        keypair: ST::KeyPairType,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
    ) -> Self {
        let genesis_qc = QuorumCertificate::genesis_qc();
        ConsensusState {
            pending_block_tree: BlockTree::new(genesis_qc.clone()),
            vote_state: VoteState::default(),
            high_qc: genesis_qc,
            state_root_validator,
            // high_qc round is 0, so pacemaker round should start at 1
            pacemaker: Pacemaker::new(config.delta, Round(1), None),
            safety: Safety::default(),
            nodeid: NodeId::new(my_pubkey),
            config,

            block_validator,
            // timeout has to be proportional to delta, too slow/fast is bad
            // assuming 2 * delta is the duration which it takes for perfect message transmission
            // 3 * delta is a reasonable amount for timeout, (4 * delta is good too)
            // as 1 * delta for original ask, 2 * delta for reaction from peer
            block_sync_requester: BlockSyncRequester::new(
                NodeId::new(my_pubkey),
                config.delta * 3,
                config.max_blocksync_retries,
            ),
            keypair,
            cert_keypair,
            beneficiary,

            // initial value here doesn't matter; it just must increase monotonically
            last_proposed_round: Round(0),
        }
    }

    /// handles the local timeout expiry event
    pub fn handle_timeout_expiry(&mut self, metrics: &mut Metrics) -> Vec<PacemakerCommand<SCT>> {
        metrics.consensus_events.local_timeout += 1;
        debug!(
            "local timeout: round={:?}",
            self.pacemaker.get_current_round()
        );
        self.pacemaker
            .handle_event(&mut self.safety, &self.high_qc)
            .into_iter()
            .collect()
    }

    /// handles proposal messages from other nodes
    /// validators and election are required as part of verifying the proposal certificates
    /// as well as determining the next leader
    /// Proposals can include NULL blocks which are blocks containing 0 transactions,
    /// an empty list.
    /// NULL block proposals are not required to validate the state_root field of the
    /// proposal's payload
    #[must_use]
    pub fn handle_proposal_message<VTF, LT, TT>(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        p: ProposalMessage<SCT>,
        tx_pool: &mut TT,
        epoch_manager: &mut EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool,
    {
        tracing::info_span!("handle_proposal_span", ?author);
        debug!("Proposal Message: {:?}", p);
        metrics.consensus_events.handle_proposal += 1;
        let mut cmds = Vec::new();

        let epoch = epoch_manager.get_epoch(p.block.0.round);
        let validator_set = val_epoch_map
            .get_val_set(&epoch)
            .expect("proposal message was verified");

        let state_root_action = self.state_root_hash_validation(&p, metrics);

        if matches!(state_root_action, StateRootAction::Reject) {
            return cmds;
        }

        cmds.extend(self.proposal_certificate_handling(
            &p,
            epoch_manager,
            validator_set,
            metrics,
            version,
        ));

        let Some(block) = Block::try_from_unverified(p.block, &self.block_validator) else {
            warn!("Transaction validation failed");
            metrics.consensus_events.failed_txn_validation += 1;
            return cmds;
        };

        // author, leader, round checks
        let round = self.pacemaker.get_current_round();
        let block_round_leader =
            election.get_leader(block.get_round(), epoch, validator_set.get_members());
        if block.get_round() > round
            || author != block_round_leader
            || block.get_author() != block_round_leader
        {
            debug!(
                "Invalid proposal: expected-round={:?} \
                round={:?} \
                expected-leader={:?} \
                author={:?} \
                block-author={:?}",
                round,
                block.get_round(),
                block_round_leader,
                author,
                block.get_author()
            );
            metrics.consensus_events.invalid_proposal_round_leader += 1;
            return cmds;
        }

        if !self.randao_validation(&block, author, epoch, val_epoch_map, metrics) {
            return cmds;
        }

        // at this point, block is valid and can be added to the blocktree
        cmds.extend(self.try_add_blocktree(
            tx_pool,
            epoch_manager,
            val_epoch_map,
            election,
            metrics,
            version,
            &block,
        ));

        // out-of-order proposals are possible if some round R+1 proposal arrives
        // before R because of network conditions. The proposals are still valid but
        // we don't vote on them
        if block.get_round() != round {
            debug!(
                "Out-of-order proposal: expected-round={:?} \
                round={:?}",
                round,
                block.get_round(),
            );
            metrics.consensus_events.out_of_order_proposals += 1;
            return cmds;
        }

        // StateRootAction::Defer means we don't have enough information to
        // decide if the state root hash is valid. It's ok to insert into the
        // block tree, but we can't vote on the block
        if matches!(state_root_action, StateRootAction::Defer) {
            return cmds;
        }

        assert!(matches!(state_root_action, StateRootAction::Proceed));
        // decide if its safe to cast vote on this proposal
        let vote = self.safety.make_vote::<SCT>(&block, &p.last_round_tc);

        if let Some(v) = vote {
            let vote_msg = VoteMessage::<SCT>::new(v, &self.cert_keypair);

            // TODO this grouping should be enforced by epoch_manager/val_epoch_map to be less
            // error-prone
            let (next_round, next_epoch, next_validator_set) = {
                let next_round = round + Round(1);
                let next_epoch = epoch_manager.get_epoch(next_round);
                let Some(next_validator_set) = val_epoch_map.get_val_set(&next_epoch) else {
                    todo!("handle non-existent validatorset for next round epoch");
                };
                (next_round, next_epoch, next_validator_set.get_members())
            };
            let next_leader = election.get_leader(next_round, next_epoch, next_validator_set);
            let msg = ConsensusMessage {
                version: version.into(),
                message: ProtocolMessage::Vote(vote_msg),
            }
            .sign(&self.keypair);
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(next_leader),
                message: msg,
            };
            debug!("Created Vote: vote={:?} next_leader={:?}", v, next_leader);
            metrics.consensus_events.created_vote += 1;
            cmds.push(send_cmd);
        }

        cmds
    }

    /// collect votes from other nodes and handle at vote_state state machine
    /// When enough votes are collected, a QC is formed and broadcast to other nodes
    #[must_use]
    pub fn handle_vote_message<VTF, LT, TT>(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        vote_msg: VoteMessage<SCT>,
        tx_pool: &mut TT,
        epoch_manager: &mut EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool,
    {
        debug!("Vote Message: {:?}", vote_msg);
        if vote_msg.vote.vote_info.round < self.pacemaker.get_current_round() {
            metrics.consensus_events.old_vote_received += 1;
            return Default::default();
        }
        metrics.consensus_events.vote_received += 1;

        let mut cmds = Vec::new();

        let epoch = epoch_manager.get_epoch(vote_msg.vote.vote_info.round);
        let validator_set = val_epoch_map
            .get_val_set(&epoch)
            .expect("vote message was verified");
        let validator_mapping = val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("vote message was verified");
        let (maybe_qc, vote_state_cmds) =
            self.vote_state
                .process_vote(&author, &vote_msg, validator_set, validator_mapping);
        cmds.extend(vote_state_cmds.into_iter().map(Into::into));

        if let Some(qc) = maybe_qc {
            debug!("Created QC {:?}", qc);
            metrics.consensus_events.created_qc += 1;

            cmds.extend(self.process_certificate_qc(
                &qc,
                epoch_manager,
                validator_set,
                metrics,
                version,
            ));

            cmds.extend(self.try_propose(
                tx_pool,
                epoch_manager,
                val_epoch_map,
                election,
                metrics,
                version,
            ));
        };

        cmds
    }

    /// handling remote timeout messages from other nodes
    #[must_use]
    pub fn handle_timeout_message<VTF, LT, TT>(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        tmo_msg: TimeoutMessage<SCT>,
        tx_pool: &mut TT,
        epoch_manager: &mut EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool,
    {
        let tm = &tmo_msg.timeout;
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            metrics.consensus_events.old_remote_timeout += 1;
            return cmds;
        }

        debug!("Remote timeout msg: {:?}", tm);
        metrics.consensus_events.remote_timeout_msg += 1;

        let epoch = epoch_manager.get_epoch(tm.tminfo.round);
        let validator_set = val_epoch_map
            .get_val_set(&epoch)
            .expect("timeout message was verified");
        let validator_mapping = val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("timeout message was verified");

        let process_certificate_cmds = self.process_certificate_qc(
            &tm.tminfo.high_qc,
            epoch_manager,
            validator_set,
            metrics,
            version,
        );
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            metrics.consensus_events.remote_timeout_msg_with_tc += 1;
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc, metrics)
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        &self.keypair,
                        &self.cert_keypair,
                        version,
                        cmd,
                    )
                })
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let (tc, remote_timeout_cmds) = self
            .pacemaker
            .process_remote_timeout::<VTF::ValidatorSetType>(
                validator_set,
                validator_mapping,
                &mut self.safety,
                &self.high_qc,
                author,
                tmo_msg,
            );

        cmds.extend(remote_timeout_cmds.into_iter().map(|cmd| {
            ConsensusCommand::from_pacemaker_command(
                &self.keypair,
                &self.cert_keypair,
                version,
                cmd,
            )
        }));
        if let Some(tc) = tc {
            debug!("Created TC: {:?}", tc);
            metrics.consensus_events.created_tc += 1;
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(&tc, metrics)
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        &self.keypair,
                        &self.cert_keypair,
                        version,
                        cmd,
                    )
                });
            cmds.extend(advance_round_cmds);
            cmds.extend(self.try_propose(
                tx_pool,
                epoch_manager,
                val_epoch_map,
                election,
                metrics,
                version,
            ));
        }

        cmds
    }

    /// Handle_block_sync only respond to blocks that was previously requested.
    /// Once successfully removed from requested dict, it then checks if its valid to add to
    /// pending_block_tree.
    ///
    /// it is possible that a requested block failed to be added to the tree
    /// due to the original proposal arriving before the requested block is returned,
    /// or the requested block is no longer relevant due to prune
    #[must_use]
    pub fn handle_block_sync<VTF, LT, TT>(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        msg: BlockSyncResponseMessage<SCT>,

        tx_pool: &mut TT,
        epoch_manager: &mut EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool,
    {
        let mut cmds = vec![];

        let current_epoch = epoch_manager.get_epoch(self.get_current_round());
        let val_set = val_epoch_map
            .get_val_set(&current_epoch)
            .expect("current validator set should be in the map");

        let bid = msg.get_block_id();
        let block_sync_result = self.block_sync_requester.handle_response(
            &author,
            msg,
            val_set,
            &self.block_validator,
            metrics,
        );
        block_sync_result.log(bid, metrics);

        match block_sync_result {
            BlockSyncResult::Success(block) => {
                if self.pending_block_tree.is_valid_to_insert(&block) {
                    cmds.extend(
                        self.request_block_if_missing_ancestor(&block.qc, val_set, metrics),
                    );
                    cmds.extend(self.try_add_blocktree(
                        tx_pool,
                        epoch_manager,
                        val_epoch_map,
                        election,
                        metrics,
                        version,
                        &block,
                    ));
                }
            }
            BlockSyncResult::Failed(retry_cmd) => cmds.extend(retry_cmd),
            BlockSyncResult::UnexpectedResponse => {
                debug!("Block sync unexpected response: author={:?}", author);
            }
        }
        cmds
    }

    /// a blocksync request could be for a block that is not yet committed so we
    /// try and fetch it from the blocktree
    pub fn fetch_uncommitted_block(&self, bid: &BlockId) -> Option<&Block<SCT>> {
        self.pending_block_tree.tree().get(bid)
    }

    /// if a blocksync request timesout, try again with a different validator
    pub fn handle_block_sync_tmo<VT>(
        &mut self,
        bid: BlockId,
        validators: &VT,
        metrics: &mut Metrics,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        self.block_sync_requester
            .handle_timeout(bid, validators, metrics)
    }

    /// state root hashes are produced when blocks are executed. They can
    /// arrive after the delay-gap between execution so they need to be handled
    /// asynchronously
    pub fn handle_state_root_update(&mut self, seq_num: SeqNum, root_hash: StateRootHash) {
        debug!(
            "handle_state_root_update seq_num: {:?} state root: {:?}",
            seq_num, root_hash
        );
        self.state_root_validator.add_state_root(seq_num, root_hash)
    }

    /// If the qc has a commit_state_hash, commit the parent block and prune the
    /// block tree
    /// Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    pub fn process_qc(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        epoch_manager: &mut EpochManager,
        metrics: &mut Metrics,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        if Rank(qc.info) <= Rank(self.high_qc.info) {
            metrics.consensus_events.process_old_qc += 1;
            return Vec::new();
        }
        metrics.consensus_events.process_qc += 1;

        self.high_qc = qc.clone();
        let mut cmds = Vec::new();
        if qc.info.vote.ledger_commit_info.is_commitable()
            && self
                .pending_block_tree
                .has_path_to_root(&qc.info.vote.vote_info.parent_id)
        {
            let blocks_to_commit = self
                .pending_block_tree
                .prune(&qc.info.vote.vote_info.parent_id);

            if let Some(b) = blocks_to_commit.last() {
                self.state_root_validator.remove_old_roots(b.get_seq_num());
            }

            debug!(
                "QC triggered commit: num_commits={:?}",
                blocks_to_commit.len()
            );

            if !blocks_to_commit.is_empty() {
                // remove inflight requests for older blocks
                self.block_sync_requester
                    .remove_old_requests(blocks_to_commit.last().unwrap().get_seq_num());

                for block in blocks_to_commit.iter() {
                    epoch_manager.schedule_epoch_start(block.get_seq_num(), block.get_round());
                    if block.payload.txns == FullTransactionList::empty() {
                        metrics.consensus_events.commit_empty_block += 1;
                    }
                }

                cmds.extend(
                    blocks_to_commit
                        .iter()
                        .map(|b| ConsensusCommand::StateRootHash(b.clone())),
                );
                cmds.push(ConsensusCommand::<ST, SCT>::LedgerCommit(blocks_to_commit));
            }
        }
        cmds
    }

    #[must_use]
    fn process_certificate_qc<VT>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        epoch_manager: &mut EpochManager,
        validators: &VT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let mut cmds = Vec::new();
        cmds.extend(self.process_qc(qc, epoch_manager, metrics));

        cmds.extend(self.pacemaker.advance_round_qc(qc, metrics).map(|cmd| {
            ConsensusCommand::from_pacemaker_command(
                &self.keypair,
                &self.cert_keypair,
                version,
                cmd,
            )
        }));

        // if the qc points to a block that is missing from the blocktree, we need
        // to request it.
        cmds.extend(self.request_block_if_missing_ancestor(qc, validators, metrics));

        // update vote_state round
        // it's ok if not leader for round; we will never propose
        self.vote_state
            .start_new_round(self.pacemaker.get_current_round());

        cmds
    }

    #[must_use]
    fn try_add_blocktree<VTF, LT, TT>(
        &mut self,
        tx_pool: &mut TT,
        epoch_manager: &mut EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        metrics: &mut Metrics,
        version: &str,

        block: &Block<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool,
    {
        self.pending_block_tree
            .add(block.clone())
            .expect("Failed to add block to blocktree");
        self.try_propose(
            tx_pool,
            epoch_manager,
            val_epoch_map,
            election,
            metrics,
            version,
        )
    }

    #[must_use]
    fn try_propose<VTF, LT, TT>(
        &mut self,
        tx_pool: &mut TT,
        epoch_manager: &mut EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool,
    {
        let mut cmds = Vec::new();

        let (round, epoch, validator_set) = {
            let round = self.pacemaker.get_current_round();
            // TODO this grouping should be enforced by epoch_manager/val_epoch_map to be less
            // error-prone
            let epoch = epoch_manager.get_epoch(round);
            let Some(validator_set) = val_epoch_map.get_val_set(&epoch) else {
                todo!("handle non-existent validatorset for next round epoch");
            };
            (round, epoch, validator_set)
        };

        // check that self is leader
        if self.nodeid != election.get_leader(round, epoch, validator_set.get_members()) {
            return cmds;
        }

        // make sure we haven't proposed in this round
        if round <= self.last_proposed_round {
            return cmds;
        }

        // check that we have path to root
        if !self
            .pending_block_tree
            .has_path_to_root(&self.high_qc.get_block_id())
        {
            return cmds;
        }

        // we passed all try_propose guards, so begin proposing
        self.last_proposed_round = round;

        let last_round_tc = self.pacemaker.get_last_round_tc().clone();
        cmds.extend(self.process_new_round_event(
            tx_pool,
            validator_set,
            last_round_tc,
            metrics,
            version,
        ));
        cmds
    }

    /// called when the node is entering a new round and is the leader for that round
    /// TODO this function can be folded into try_propose; it's only called there
    #[must_use]
    fn process_new_round_event<VT, TT: TxPool>(
        &mut self,
        txpool: &mut TT,
        validators: &VT,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let node_id = self.nodeid;
        let round = self.pacemaker.get_current_round();
        let high_qc = self.high_qc.clone();

        let parent_bid = high_qc.get_block_id();
        let seq_num_qc = high_qc.get_seq_num();
        let proposed_seq_num = seq_num_qc + SeqNum(1);

        let proposer_builder =
            |txns: FullTransactionList,
             hash: StateRootHash,
             last_round_tc: Option<TimeoutCertificate<SCT>>| {
                let mut header = ExecutionArtifacts::zero();
                header.state_root = hash;
                let b = Block::new(
                    node_id,
                    round,
                    &Payload {
                        txns,
                        header,
                        seq_num: proposed_seq_num,
                        beneficiary: self.get_beneficiary(),
                        randao_reveal: RandaoReveal::new::<SCT::SignatureType>(
                            round,
                            self.get_cert_keypair(),
                        ),
                    },
                    &high_qc,
                );

                let p = ProposalMessage {
                    block: UnverifiedBlock(b),
                    last_round_tc,
                };
                let msg = ConsensusMessage {
                    version: version.into(),
                    message: ProtocolMessage::Proposal(p),
                }
                .sign(&self.keypair);

                vec![ConsensusCommand::Publish {
                    target: RouterTarget::Broadcast,
                    message: msg,
                }]
            };

        match self.proposal_policy(&parent_bid, proposed_seq_num) {
            ConsensusAction::Propose(h, pending_blocktree_txs) => {
                tracing::info_span!("create_proposal_span", ?round);
                metrics.consensus_events.creating_proposal += 1;
                debug!("Creating Proposal: node_id={:?} round={:?} high_qc={:?}, seq_num={:?}, last_round_tc={:?}", 
                                node_id, round, high_qc, proposed_seq_num, last_round_tc);

                let (prop_txns, leftover_txns) = txpool.create_proposal(
                    self.config.proposal_txn_limit,
                    self.config.proposal_gas_limit,
                    pending_blocktree_txs,
                );
                let mut cmds = proposer_builder(prop_txns, h, last_round_tc);
                if let (Some(txns), Some(target)) = (leftover_txns, self.cascade_target(validators))
                {
                    cmds.push(ConsensusCommand::CascadeTxns {
                        peer: target,
                        txns: txns.bytes().clone(),
                    });
                }
                cmds
            }
            ConsensusAction::Abstain => {
                metrics.consensus_events.abstain_proposal += 1;
                // TODO-2: This could potentially be an empty block
                vec![]
            }
            ConsensusAction::ProposeEmpty => {
                tracing::info_span!("create_proposal_empty_span", ?round);
                // Don't have the necessary state root hash ready so propose
                // a NULL block
                metrics.consensus_events.creating_empty_block_proposal += 1;
                debug!("Creating Empty Proposal: node_id={:?} round={:?} high_qc={:?}, seq_num={:?}, last_round_tc={:?}", 
                                node_id, round, high_qc, proposed_seq_num, last_round_tc);

                let txns = FullTransactionList::empty();
                // TODO: should empty blocks have their own special hash?
                proposer_builder(txns, INITIAL_DELAY_STATE_ROOT_HASH, last_round_tc)
            }
        }
    }

    #[must_use]
    fn proposal_policy(&self, parent_bid: &BlockId, proposed_seq_num: SeqNum) -> ConsensusAction {
        // TODO when statesync ready - Never propose while syncing

        // Can't propose txs without state root hash
        let Some(h) = self
            .state_root_validator
            .get_next_state_root(proposed_seq_num)
        else {
            return ConsensusAction::ProposeEmpty;
        };

        // Always propose when there's a path to root
        if let Some(pending_blocktree_txs) =
            self.pending_block_tree.get_txs_on_path_to_root(parent_bid)
        {
            return ConsensusAction::Propose(h, pending_blocktree_txs);
        }

        // Still propose but with the chance of proposing duplicate txs
        if self.config.propose_with_missing_blocks {
            return ConsensusAction::Propose(h, vec![]);
        };

        ConsensusAction::Abstain
    }

    #[must_use]
    fn request_block_if_missing_ancestor<VT>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validators: &VT,
        metrics: &mut Metrics,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        if let Some(qc) = self.pending_block_tree.get_missing_ancestor(qc) {
            let max_seq_num_to_request =
                self.pending_block_tree.get_root_seq_num() + self.config.state_sync_threshold;

            if qc.get_seq_num() >= max_seq_num_to_request {
                // TODO: This should trigger statesync. Remove blocksync request.
                warn!(
                    "Lagging over {:?} blocks behind. Trigger statesync",
                    self.config.state_sync_threshold
                );

                metrics.consensus_events.trigger_state_sync += 1;
            }

            self.block_sync_requester
                .request::<VT>(&qc, validators, metrics)
        } else {
            vec![]
        }
    }

    #[must_use]
    fn state_root_hash_validation(
        &mut self,
        p: &ProposalMessage<SCT>,
        metrics: &mut Metrics,
    ) -> StateRootAction {
        if p.block.0.payload.txns == FullTransactionList::empty()
            && p.block.0.payload.header.state_root == INITIAL_DELAY_STATE_ROOT_HASH
        {
            debug!("Received empty block: block={:?}", p.block);
            metrics.consensus_events.rx_empty_block += 1;
            return StateRootAction::Proceed;
        }
        match self.state_root_validator.validate(
            p.block.0.payload.seq_num,
            p.block.0.payload.header.state_root,
        ) {
            // TODO-1 execution lagging too far behind should be a trigger for
            // something to try and catch up faster. For now, just wait
            StateRootResult::OutOfRange => {
                debug!(
                    "Proposal Message carries state root hash for a block higher than highest locally known, unable to verify"
                );
                metrics.consensus_events.rx_execution_lagging += 1;
                StateRootAction::Defer
            }
            // Don't vote and locally timeout if the proposed state root does
            // not match
            StateRootResult::Mismatch => {
                debug!("State root hash in proposal conflicts with local value");
                metrics.consensus_events.rx_bad_state_root += 1;
                StateRootAction::Reject
            }
            // Don't vote and locally timeout if we don't have enough
            // information to decide whether the state root is valid. It's still
            // safe to insert to the block tree if other checks passes. So we
            // don't need to request block sync if other blocks forms a QC on it
            StateRootResult::Missing => {
                debug!("Missing state root hash value locally, unable to verify");
                metrics.consensus_events.rx_missing_state_root += 1;
                StateRootAction::Defer
            }
            StateRootResult::Success => {
                debug!("Received Proposal Message with valid state root hash");
                metrics.consensus_events.rx_proposal += 1;
                StateRootAction::Proceed
            }
        }
    }

    #[must_use]
    fn proposal_certificate_handling<VT>(
        &mut self,
        p: &ProposalMessage<SCT>,
        epoch_manager: &mut EpochManager,
        validators: &VT,
        metrics: &mut Metrics,
        version: &str,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let mut cmds = vec![];

        let process_certificate_cmds =
            self.process_certificate_qc(&p.block.0.qc, epoch_manager, validators, metrics, version);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            debug!("Handled proposal with TC: {:?}", last_round_tc);
            metrics.consensus_events.proposal_with_tc += 1;
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc, metrics)
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        &self.keypair,
                        &self.cert_keypair,
                        version,
                        cmd,
                    )
                })
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        cmds
    }

    #[must_use]
    fn randao_validation<VTF>(
        &self,
        block: &Block<SCT>,
        author: NodeId<SCT::NodeIdPubKey>,
        epoch: Epoch,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        metrics: &mut Metrics,
    ) -> bool
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        let author_pubkey = val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("proposal message was verified")
            .map
            .get(&author)
            .expect("proposal author exists in validator_mapping");

        if let Err(e) = block
            .payload
            .randao_reveal
            .verify::<SCT::SignatureType>(block.get_round(), author_pubkey)
        {
            warn!("Invalid randao_reveal signature, reason: {:?}", e);
            metrics.consensus_events.failed_verify_randao_reveal_sig += 1;
            return false;
        }
        true
    }

    // TODO: use a real algorithm to choose target
    #[must_use]
    fn cascade_target<VT>(&self, validators: &VT) -> Option<NodeId<SCT::NodeIdPubKey>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        for (nodeid, _) in validators.get_members().iter() {
            if *nodeid != self.nodeid {
                return Some(*nodeid);
            }
        }
        None
    }

    pub fn get_pubkey(&self) -> SCT::NodeIdPubKey {
        self.keypair.pubkey()
    }

    pub fn get_nodeid(&self) -> NodeId<SCT::NodeIdPubKey> {
        self.nodeid
    }

    pub fn get_beneficiary(&self) -> EthAddress {
        self.beneficiary
    }

    pub fn blocktree(&self) -> &BlockTree<SCT> {
        &self.pending_block_tree
    }

    pub fn block_sync_requester(&self) -> &BlockSyncRequester<ST, SCT> {
        &self.block_sync_requester
    }

    pub fn get_current_round(&self) -> Round {
        self.pacemaker.get_current_round()
    }

    pub fn get_keypair(&self) -> &ST::KeyPairType {
        &self.keypair
    }

    pub fn get_cert_keypair(&self) -> &SignatureCollectionKeyPairType<SCT> {
        &self.cert_keypair
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Deref, time::Duration};

    use itertools::Itertools;
    use monad_consensus::{
        messages::{
            consensus_message::ProtocolMessage,
            message::{BlockSyncResponseMessage, ProposalMessage, TimeoutMessage, VoteMessage},
        },
        pacemaker::PacemakerCommand,
        validation::signing::Verified,
    };
    use monad_consensus_types::{
        block::{Block, BlockType, UnverifiedBlock},
        block_validator::MockValidator,
        ledger::CommitResult,
        metrics::Metrics,
        payload::{
            Bloom, ExecutionArtifacts, FullTransactionList, Gas, MissingNextStateRoot,
            NopStateRoot, StateRoot, StateRootValidator, INITIAL_DELAY_STATE_ROOT_HASH,
        },
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        state_root_hash::StateRootHash,
        timeout::Timeout,
        txpool::MockTxPool,
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
        },
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use monad_testutil::{
        proposal::ProposalGen,
        signing::{create_certificate_keys, create_keys, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Epoch, NodeId, Round, RouterTarget, SeqNum, Stake, TimeoutVariant};
    use monad_validator::{
        epoch_manager::EpochManager,
        leader_election::LeaderElection,
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetType},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use test_case::test_case;
    use tracing_test::traced_test;

    use crate::{ConsensusCommand, ConsensusConfig, ConsensusState};

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type StateRootValidatorType = NopStateRoot;

    fn setup<
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        SVT: StateRootValidator,
    >(
        num_states: u32,
        state_root: impl Fn() -> SVT,
    ) -> (
        Vec<ST::KeyPairType>,
        Vec<SignatureCollectionKeyPairType<SCT>>,
        EpochManager,
        ValidatorsEpochMapping<ValidatorSetFactory<CertificateSignaturePubKey<ST>>, SCT>,
        Vec<ConsensusState<ST, SCT, MockValidator, SVT>>,
    ) {
        let (keys, cert_keys, valset, valmap) =
            create_keys_w_validators::<ST, SCT, _>(num_states, ValidatorSetFactory::default());
        let val_stakes = Vec::from_iter(valset.get_members().clone());
        let val_cert_pubkeys = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(cert_keys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(
            Epoch(1),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );

        let epoch_manager = EpochManager::new(SeqNum(100), Round(20));

        let mut dupkeys = create_keys::<ST>(num_states);
        let mut dupcertkeys = create_certificate_keys::<SCT>(num_states);
        let consensus_states = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let default_key =
                    <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut [127; 32]).unwrap();
                let default_cert_key =
                    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(
                        &mut [127; 32],
                    )
                    .unwrap();
                ConsensusState::<ST, SCT, _, SVT>::new(
                    MockValidator,
                    state_root(),
                    k.pubkey(),
                    ConsensusConfig {
                        proposal_txn_limit: 5000,
                        proposal_gas_limit: 8_000_000,
                        propose_with_missing_blocks: false,
                        delta: Duration::from_secs(1),
                        max_blocksync_retries: 5,
                        state_sync_threshold: SeqNum(100),
                    },
                    EthAddress::default(),
                    std::mem::replace(&mut dupkeys[i], default_key),
                    std::mem::replace(&mut dupcertkeys[i], default_cert_key),
                )
            })
            .collect::<Vec<_>>();

        (
            keys,
            cert_keys,
            epoch_manager,
            val_epoch_map,
            consensus_states,
        )
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
        cmds.into_iter()
            .find_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::Broadcast,
                    message,
                } => match &message.deref().deref().message {
                    ProtocolMessage::Proposal(p) => Some(p.clone()),
                    _ => None,
                },
                _ => None,
            })
            .expect("proposal")
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[traced_test]
    #[test]
    fn lock_qc_high() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let state = &mut states[0];
        assert_eq!(state.high_qc.get_round(), Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
            seq_num: SeqNum(0),
        };
        let v = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        };

        let vm1 = VoteMessage::<SignatureCollectionType>::new(v, &certkeys[1]);
        let vm2 = VoteMessage::<SignatureCollectionType>::new(v, &certkeys[2]);
        let vm3 = VoteMessage::<SignatureCollectionType>::new(v, &certkeys[3]);

        let v1 = Verified::<SignatureType, _>::new(vm1, &keys[1]);
        let v2 = Verified::<SignatureType, _>::new(vm2, &keys[2]);
        let v3 = Verified::<SignatureType, _>::new(vm3, &keys[3]);

        let _ = state.handle_vote_message(
            *v1.author(),
            *v1,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let _ = state.handle_vote_message(
            *v2.author(),
            *v2,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        // less than 2f+1, so expect not locked
        assert_eq!(state.high_qc.get_round(), Round(0));

        let _ = state.handle_vote_message(
            *v3.author(),
            *v3,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        assert_eq!(state.high_qc.get_round(), expected_qc_high_round);
        assert_eq!(metrics.consensus_events.vote_received, 3);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let state = &mut states[0];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        // local timeout for state in Round 1
        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        let _ = state
            .pacemaker
            .handle_event(&mut state.safety, &state.high_qc);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        let result = extract_vote_msgs(cmds);
        assert!(result.is_empty());
    }

    #[test]
    fn enter_proposalmsg_round() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let state = &mut states[0];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, SignatureCollectionType>::new();

        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let result = extract_vote_msgs(cmds);

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert_eq!(result.len(), 1);

        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let result = extract_vote_msgs(cmds);

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert_eq!(result.len(), 1);

        for _ in 0..4 {
            propgen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );
        }
        let p7 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p7.destructure();
        let _ = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        assert_eq!(state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn duplicate_proposals() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let state = &mut states[0];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, SignatureCollectionType>::new();

        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let result = extract_vote_msgs(cmds);
        assert_eq!(result.len(), 1);

        // send duplicate of p1, expect it to be ignored and no output commands
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        assert!(cmds.is_empty());
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
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let state = &mut states[0];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, _>::new();

        // first proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let result = extract_vote_msgs(cmds);

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert_eq!(result.len(), 1);

        // second proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let result = extract_vote_msgs(cmds);

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert_eq!(result.len(), 1);

        let mut missing_proposals = Vec::new();
        for _ in 0..perms.len() {
            missing_proposals.push(propgen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            ));
        }

        // last proposal arrvies
        let p_fut = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p_fut.destructure();
        let _ = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        // was in Round(2) and skipped over perms.len() proposals. Handling
        // p_fut should be at Round(3+perms.len())
        assert_eq!(
            state.pacemaker.get_current_round(),
            Round(3 + perms.len() as u64)
        );
        assert_eq!(state.pending_block_tree.size(), 3);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(state.handle_proposal_message(
                author,
                verified_message,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics,
                version,
            ));
        }

        // next proposal will trigger everything to be committed if there is
        // a consecutive chain as expected
        // last proposal arrvies
        let p_last = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p_last.destructure();
        let _ = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        assert_eq!(state.pending_block_tree.size(), 2);
        assert_eq!(
            state.pacemaker.get_current_round(),
            Round(perms.len() as u64 + 4),
            "order of proposals {:?}",
            perms
        );
    }

    #[test]
    fn test_commit_rule_consecutive() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let state = &mut states[0];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, _>::new();

        // round 1 proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let p1_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        let p1_votes = extract_vote_msgs(p1_cmds);
        assert!(p1_votes.len() == 1);
        assert!(p1_votes[0].vote.ledger_commit_info.is_commitable());

        // round 2 proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_none());

        let p2_votes = extract_vote_msgs(p2_cmds);
        assert!(p2_votes.len() == 1);
        // csh is some: the proposal and qc have consecutive rounds
        assert!(p2_votes[0].vote.ledger_commit_info.is_commitable());

        let p3 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p3.destructure();

        let p2_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());
    }

    #[test]
    fn test_commit_rule_non_consecutive() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let state = &mut states[0];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, _>::new();

        // round 1 proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let _ = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        // round 2 proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        let p2_votes = extract_vote_msgs(p2_cmds);
        assert!(p2_votes.len() == 1);
        assert!(p2_votes[0].vote.ledger_commit_info.is_commitable());

        // round 2 timeout
        let pacemaker_cmds = state
            .pacemaker
            .handle_event(&mut state.safety, &state.high_qc);

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

        let valset = val_epoch_map.get_val_set(&Epoch(1)).unwrap();
        let val_cert_pubkeys = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();
        let _ = propgen.next_tc(&keys, &certkeys, valset, val_cert_pubkeys);

        // round 3 proposal, has qc(1)
        let p3 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        assert_eq!(p3.block.0.qc.get_round(), Round(1));
        assert_eq!(p3.block.0.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let p3_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        let p3_votes = extract_vote_msgs(p3_cmds);
        assert!(p3_votes.len() == 1);
        // proposal and qc have non-consecutive rounds
        assert!(!p3_votes[0].vote.ledger_commit_info.is_commitable());
    }

    // this test checks that a malicious proposal sent only to the next leader is
    // not incorrectly committed
    #[test]
    fn test_malicious_proposal_and_block_recovery() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let valset = val_epoch_map.get_val_set(&Epoch(1)).unwrap();
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let version = "TEST";
        let (first_state, xs) = states.split_first_mut().unwrap();
        let (second_state, xs) = xs.split_first_mut().unwrap();
        let (third_state, xs) = xs.split_first_mut().unwrap();
        let fourth_state = &mut xs[0];

        let mut metrics: Vec<Metrics> = (0..4).map(|_| Metrics::default()).collect();

        // first_state will send 2 different proposals, A and B. A will be sent to
        // the next leader, all other nodes get B.
        // effect is that nodes send votes for B to the next leader who thinks that
        // the "correct" proposal is A.
        let mut correct_proposal_gen = ProposalGen::<SignatureType, _>::new();
        let mut mal_proposal_gen = ProposalGen::<SignatureType, _>::new();

        let cp1 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let mp1 = mal_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![5].into()),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp1.destructure();
        let block_1 = verified_message.block.clone();
        let cmds1 = first_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );
        let p1_votes = extract_vote_msgs(cmds1)[0];

        let cmds3 = third_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );
        let p3_votes = extract_vote_msgs(cmds3)[0];

        let cmds4 = fourth_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[3],
            version,
        );
        let p4_votes = extract_vote_msgs(cmds4)[0];

        let (author, _, verified_message) = mp1.destructure();
        let cmds2 = second_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );
        let p2_votes = extract_vote_msgs(cmds2)[0];

        assert_eq!(p1_votes.vote, p3_votes.vote);
        assert_eq!(p1_votes.vote, p4_votes.vote);
        assert_ne!(p1_votes.vote, p2_votes.vote);
        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];
        // temp sub with a key that doesn't exists.
        let mut routing_target = NodeId::new(get_key::<SignatureType>(100).pubkey());
        // We Collected 4 votes, 3 of which are valid, 1 of which is not caused by byzantine leader.
        // First 3 (including a false vote) submitted would not cause a qc to form
        // but the last vote would cause a qc to form locally at second_state, thus causing
        // second state to realize its missing a block.
        for i in 0..4 {
            let v = Verified::<SignatureType, VoteMessage<_>>::new(votes[i], &keys[i]);
            let cmds2 = second_state.handle_vote_message(
                *v.author(),
                *v,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics[1],
                version,
            );
            let res = cmds2
                .into_iter()
                .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
            if i < 3 {
                assert!(res.is_none());
            } else {
                assert!(res.is_some());
                let (target, sync_bid) = match res.unwrap() {
                    ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                    _ => None,
                }
                .unwrap();
                assert!(sync_bid == block_1.0.get_id());
                routing_target = target;
            }
        }
        assert_ne!(
            routing_target,
            NodeId::new(get_key::<SignatureType>(100).pubkey())
        );
        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(second_state.high_qc.info.vote, votes[0].vote);

        // use the correct proposal gen to make next proposal and send it to second_state
        // this should cause it to emit the RequestBlockSync Message because the the QC parent
        // points to a different proposal (second_state has the malicious proposal in its blocktree)
        let cp2 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author_2, _, verified_message_2) = cp2.destructure();
        let block_2 = verified_message_2.block.clone();
        let _ = second_state.handle_proposal_message(
            author_2,
            verified_message_2.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = first_state.handle_proposal_message(
            author_2,
            verified_message_2.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );
        let res = cmds1
            .into_iter()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        assert!(res.is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = cp3.destructure();
        let block_3 = verified_message.block.clone();

        let cmds2 = second_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );
        let cmds1 = first_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );

        // second_state has the malicious block in the blocktree, so it will not be able to
        // commit anything
        assert_eq!(second_state.pending_block_tree.size(), 3);
        let res = cmds2
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_none());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(first_state.pending_block_tree.size(), 2);
        let res = cmds1
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());

        let msg = BlockSyncResponseMessage::BlockFound(block_1);
        // a block sync request arrived, helping second state to recover
        let _ = second_state.handle_block_sync(
            routing_target,
            msg,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );

        // in the next round, second_state should recover and able to commit
        let cp4 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = cp4.destructure();
        let cmds2 = second_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );
        // new block added should allow path_to_root properly, thus no more request sync
        let res = cmds2
            .iter()
            .clone()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        assert!(res.is_none());

        let cmds1 = first_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );

        // second_state has the correct blocks, so expect to see a commit
        assert_eq!(second_state.pending_block_tree.size(), 2);
        let res = cmds2
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(first_state.pending_block_tree.size(), 2);
        let res = cmds1
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());

        // third_state only received proposal for round 1, and is missing proposal for round 2, 3, 4
        // feeding third_state with a proposal from round 4 should trigger a recursive behaviour to ask for blocks

        let cmds3 = third_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );

        assert_eq!(third_state.pending_block_tree.size(), 2);
        let res = cmds3
            .iter()
            .clone()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        let Some(ConsensusCommand::RequestSync { peer, block_id }) = res else {
            panic!("request sync is not found")
        };

        let mal_sync = BlockSyncResponseMessage::NotAvailable(block_2.0.get_id());
        // BlockSyncMessage on blocks that were not requested should be ignored.
        let cmds3 = third_state.handle_block_sync(
            author_2,
            mal_sync,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );

        assert_eq!(third_state.pending_block_tree.size(), 2);
        let res = cmds3
            .iter()
            .clone()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        assert!(res.is_none());

        let sync = BlockSyncResponseMessage::BlockFound(block_3);

        let cmds3 = third_state.handle_block_sync(
            *peer,
            sync.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );

        assert_eq!(third_state.pending_block_tree.size(), 3);
        let res = cmds3
            .iter()
            .clone()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        let Some(ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        }) = res
        else {
            panic!("request sync is not found")
        };
        // repeated handling of the requested block should be ignored
        let cmds3 = third_state.handle_block_sync(
            *peer,
            sync,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );

        assert_eq!(third_state.pending_block_tree.size(), 3);
        let res = cmds3
            .iter()
            .clone()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        assert!(res.is_none());

        //arrival of proposal should also prevent block_sync_request from modifying the tree
        let cmds2 = third_state.handle_proposal_message(
            author_2,
            verified_message_2,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );
        assert_eq!(third_state.pending_block_tree.size(), 4);
        let res = cmds2
            .into_iter()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        assert!(res.is_none());

        let sync = BlockSyncResponseMessage::BlockFound(block_2);
        // request sync which did not arrive in time should be ignored.
        let cmds3 = third_state.handle_block_sync(
            *peer_2,
            sync,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );

        assert_eq!(third_state.pending_block_tree.size(), 4);
        let res = cmds3
            .iter()
            .clone()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        assert!(res.is_none());
    }

    #[traced_test]
    #[test]
    fn test_receive_empty_block() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRoot>(4, || {
                StateRoot::new(SeqNum(1))
            });
        let election = SimpleRoundRobin::default();
        let (state, _) = states.split_first_mut().unwrap();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new();

        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = p1.destructure();

        let cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: _,
                    message: _
                }
            )
        });
        assert!(result.is_some());
        assert_eq!(metrics.consensus_events.rx_empty_block, 1);
    }

    /// Test the behaviour of consensus when execution is lagging. This is tested
    /// by handling a non-empty proposal which is missing the state root hash (lagging execution)
    #[traced_test]
    #[test]
    fn test_lagging_execution() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRoot>(4, || {
                StateRoot::new(SeqNum(1))
            });
        let election = SimpleRoundRobin::default();
        let (state, _) = states.split_first_mut().unwrap();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new();

        proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts::zero(),
        );

        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = p1.destructure();
        let cmds: Vec<ConsensusCommand<NopSignature, MultiSig<NopSignature>>> = state
            .handle_proposal_message(
                author,
                verified_message,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics,
                version,
            );

        // the proposal still gets processed: the node enters a new round, and
        // issues a request for the block it skipped over
        assert_eq!(cmds.len(), 3);
        assert!(matches!(
            cmds[0],
            ConsensusCommand::Schedule {
                duration: _,
                on_timeout: TimeoutVariant::Pacemaker
            }
        ));
        assert!(matches!(cmds[1], ConsensusCommand::RequestSync { .. }));
        assert!(matches!(
            cmds[2],
            ConsensusCommand::Schedule {
                duration: _,
                on_timeout: TimeoutVariant::BlockSync(_)
            }
        ));
        assert_eq!(metrics.consensus_events.rx_execution_lagging, 1);
    }

    /// Test consensus behavior when a state root hash is missing. This is
    /// tested by only updating consensus with one state root, then handling a
    /// proposal with state root lower than that
    #[test]
    fn test_missing_state_root() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRoot>(4, || {
                StateRoot::new(SeqNum(5))
            });
        let election = SimpleRoundRobin::default();
        let (state, _) = states.split_first_mut().unwrap();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new();

        // prepare 10 blocks
        for _ in 0..10 {
            let p = proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts {
                    state_root: INITIAL_DELAY_STATE_ROOT_HASH,
                    ..ExecutionArtifacts::zero()
                },
            );
            let (author, _, p) = p.destructure();
            let _cmds = state.handle_proposal_message(
                author,
                p,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics,
                version,
            );
        }

        assert_eq!(metrics.consensus_events.rx_empty_block, 10);
        assert_eq!(state.get_current_round(), Round(10));

        // only execution update for block 8 comes
        state.handle_state_root_update(SeqNum(8), StateRootHash(Hash([0x08_u8; 32])));

        // Block 11 carries the state root hash from executing block 6 the state
        // root hash is missing. The certificates are processed - consensus enters new round and commit blocks, but it doesn't vote
        let p = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts {
                state_root: StateRootHash(Hash([0x06_u8; 32])),
                ..ExecutionArtifacts::zero()
            },
        );
        let (author, _, p) = p.destructure();
        let cmds = state.handle_proposal_message(
            author,
            p,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        assert_eq!(state.get_current_round(), Round(11));
        assert_eq!(metrics.consensus_events.rx_missing_state_root, 1);
        println!("{:?}", cmds);
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds[0], ConsensusCommand::StateRootHash(_)));
        assert!(matches!(cmds[1], ConsensusCommand::LedgerCommit(_)));
        assert!(matches!(
            cmds[2],
            ConsensusCommand::Schedule {
                duration: _,
                on_timeout: TimeoutVariant::Pacemaker
            }
        ));
    }

    /// Test consensus behaviour of a leader who is supposed to propose
    /// the next round but does not have a recent enough state root hash
    #[traced_test]
    #[test]
    fn test_unavailable_state_root_during_proposal() {
        // MissingNextStateRoot forces the proposer's state root hash
        // to be unavailable
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, MissingNextStateRoot>(4, || {
                MissingNextStateRoot::default()
            });
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let version = "TEST";
        let (first_state, xs) = states.split_first_mut().unwrap();
        let (second_state, xs) = xs.split_first_mut().unwrap();
        let (third_state, xs) = xs.split_first_mut().unwrap();
        let fourth_state = &mut xs[0];
        let mut metrics: Vec<Metrics> = (0..4).map(|_| Metrics::default()).collect();

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new();
        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = p1.destructure();
        let cmds1 = first_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );
        let p1_votes = extract_vote_msgs(cmds1)[0];

        let cmds2 = second_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );
        let p2_votes = extract_vote_msgs(cmds2)[0];

        let cmds3 = third_state.handle_proposal_message(
            author,
            verified_message.clone(),
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[2],
            version,
        );
        let p3_votes = extract_vote_msgs(cmds3)[0];

        let cmds4 = fourth_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[3],
            version,
        );
        let p4_votes = extract_vote_msgs(cmds4)[0];

        let (next_leader, leader_metrics) = {
            let node_id = *proposal_gen
                .next_proposal(
                    &keys,
                    &certkeys,
                    &epoch_manager,
                    &val_epoch_map,
                    &election,
                    FullTransactionList::empty(),
                    ExecutionArtifacts::zero(),
                )
                .author();
            if node_id == first_state.nodeid {
                (first_state, &mut metrics[0])
            } else if node_id == second_state.nodeid {
                (second_state, &mut metrics[1])
            } else if node_id == third_state.nodeid {
                (third_state, &mut metrics[2])
            } else if node_id == fourth_state.nodeid {
                (fourth_state, &mut metrics[3])
            } else {
                unreachable!("next leader should be one of the 4 nodes")
            }
        };

        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];
        for i in 0..4 {
            let v = Verified::<SignatureType, VoteMessage<_>>::new(votes[i], &keys[i]);
            let cmds = next_leader.handle_vote_message(
                *v.author(),
                *v,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                leader_metrics,
                version,
            );

            // after 2f + 1 votes, we expect that an empty proposal is created
            if i == 2 {
                let p = extract_proposal_broadcast(cmds);
                assert_eq!(p.block.0.payload.txns, FullTransactionList::empty());
                assert_eq!(
                    p.block.0.payload.header.state_root,
                    StateRootHash(Hash([0; 32]))
                );
                assert_eq!(
                    leader_metrics
                        .consensus_events
                        .creating_empty_block_proposal,
                    1
                );
            }
        }
    }

    #[test]
    fn test_state_root_updates() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRoot>(4, || {
                StateRoot::new(SeqNum(1))
            });
        let election = SimpleRoundRobin::default();
        let (state, _) = states.split_first_mut().unwrap();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";

        // delay gap in setup is 1

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new();
        let p0 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p0.destructure();
        // p0 should have seqnum 1 and therefore only require state_root 0
        // the state_root 0's hash should be Hash([0x00; 32])
        state.handle_state_root_update(SeqNum(0), StateRootHash(Hash([0x00; 32])));
        let _ = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: StateRootHash(Hash([0x99; 32])),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Gas(0),
            },
        );
        let (author, _, verified_message) = p1.destructure();
        // p1 should have seqnum 2 and therefore only require state_root 1
        state.handle_state_root_update(SeqNum(1), StateRootHash(Hash([0x99; 32])));
        let _ = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );

        // commit some blocks and confirm cleanup of state root hashes happened

        let p2 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: StateRootHash(Hash([0xbb; 32])),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Gas(0),
            },
        );

        let (author, _, verified_message) = p2.destructure();
        state.handle_state_root_update(SeqNum(2), StateRootHash(Hash([0xbb; 32])));
        let p2_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());

        let p3 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: StateRootHash(Hash([0xcc; 32])),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Gas(0),
            },
        );

        let (author, _, verified_message) = p3.destructure();
        state.handle_state_root_update(SeqNum(3), StateRootHash(Hash([0xcc; 32])));
        let p3_cmds = state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let lc = p3_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());

        // Delay gap is 1 and we have received proposals with seq num 0, 1, 2, 3
        // state_root_validator had updates for 0, 1, 2, 3
        // Proposals with seq num 1 and 2 are committed, so expect 2 and 3 to remain
        // in the state_root_validator
        assert_eq!(2, state.state_root_validator.root_hashes.len());
        assert!(state
            .state_root_validator
            .root_hashes
            .contains_key(&SeqNum(2)));
        assert!(state
            .state_root_validator
            .root_hashes
            .contains_key(&SeqNum(3)));
    }

    #[test]
    fn test_fetch_uncommitted_block() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let (first_state, _) = states.split_first_mut().unwrap();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics = Metrics::default();
        let version = "TEST";

        let mut correct_proposal_gen = ProposalGen::<SignatureType, _>::new();
        let mut branch_off_proposal_gen = ProposalGen::<SignatureType, _>::new();

        let cp1 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp1.destructure();
        let block_1 = verified_message.block.clone();
        let bid_correct = block_1.0.get_id();
        // requesting a block that's doesn't exists should yield None
        assert_eq!(first_state.fetch_uncommitted_block(&bid_correct), None);
        // assuming a proposal comes in, should allow it to be fetched as it is within pending block tree
        let _ = first_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let full_block = first_state.fetch_uncommitted_block(&bid_correct).unwrap();
        assert_eq!(full_block.get_id(), bid_correct);

        // you can also receive a branch, which would cause pending block tree retrieval to also be valid
        let bp1 = branch_off_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(vec![13, 32].into()),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = bp1.destructure();
        let block_1 = verified_message.block.clone();
        let bid_branch = block_1.0.get_id();
        assert_eq!(first_state.fetch_uncommitted_block(&bid_branch), None);

        let _ = first_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        let full_block = first_state.fetch_uncommitted_block(&bid_branch).unwrap();
        assert_eq!(full_block.get_id(), bid_branch);

        let mut ledger_blocks = Vec::new();
        // if a certain commit is triggered, then fetching block would fail
        for _ in 0..3 {
            let cp = correct_proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            let block = verified_message.block.clone();
            let bid = block.0.get_id();
            // requesting a block that's doesn't exists should yield None
            assert_eq!(first_state.fetch_uncommitted_block(&bid), None);
            // assuming a proposal comes in, should allow it to be fetched as it is within pending block tree

            let cmds = first_state.handle_proposal_message(
                author,
                verified_message,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics,
                version,
            );

            for cmd in cmds {
                if let ConsensusCommand::LedgerCommit(blocks) = cmd {
                    ledger_blocks.extend(blocks);
                }
            }
            let full_block = first_state.fetch_uncommitted_block(&bid).unwrap();
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
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_state as u32,
                || NopStateRoot,
            );

        let election = SimpleRoundRobin::default();
        let mut correct_proposal_gen = ProposalGen::<SignatureType, _>::new();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_state).map(|_| Metrics::default()).collect();
        let version = "TEST";

        for i in 0..8 {
            let cp = correct_proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (j, state) in states.iter_mut().enumerate() {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    &mut epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[j],
                    version,
                );
                let bsync_reqest = cmds
                    .iter()
                    .find(|&c| matches!(c, ConsensusCommand::RequestSync { .. }));
                // observing a qc that link to root should not trigger anything
                assert!(bsync_reqest.is_none());
                assert_eq!(state.get_current_round(), Round(i + 1))
            }
        }
        for state in states.iter() {
            assert_eq!(state.get_current_round(), Round(8));
        }

        // determine the leader of round 11 because we are going to skip sending them the round 9
        // proposal while the other nodes get it and send their votes
        let epoch = epoch_manager.get_epoch(Round(11));
        let next_leader = election.get_leader(
            Round(11),
            epoch,
            val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        let mut leader_index = 0;

        // generate proposals 9 and 10 and collect the votes for 10 so that we can deliver them to
        // the leader of 11
        let mut votes = vec![];
        let mut proposal_10_blockid = BlockId(Hash::default());
        for j in 0..2 {
            let cp = correct_proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );
            let (author, _, verified_message) = cp.destructure();
            for (i, state) in states.iter_mut().enumerate() {
                if state.nodeid != next_leader {
                    let cmds = state.handle_proposal_message(
                        author,
                        verified_message.clone(),
                        &mut empty_txpool,
                        &mut epoch_manager,
                        &val_epoch_map,
                        &election,
                        &mut metrics[i],
                        version,
                    );

                    if j == 1 {
                        proposal_10_blockid = verified_message.block.0.get_id();
                        let v = extract_vote_msgs(cmds);
                        assert_eq!(v.len(), 1);
                        votes.push((state.nodeid, v[0]));
                    }
                } else {
                    leader_index = i;
                }
            }
        }
        for (i, (author, v)) in votes.iter().enumerate() {
            let cmds = states[leader_index].handle_vote_message(
                *author,
                *v,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics[leader_index],
                version,
            );

            if i == (num_state * 2 / 3) {
                let req: Vec<(NodeId<_>, BlockId)> = cmds
                    .into_iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                        _ => None,
                    })
                    .collect();
                assert_eq!(req.len(), 1);
                assert_eq!(req[0].1, proposal_10_blockid);
            } else {
                let req: Vec<_> = cmds
                    .into_iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
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
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_state as u32,
                || NopStateRoot,
            );
        let mut metrics: Vec<Metrics> = (0..num_state).map(|_| Metrics::default()).collect();
        let version = "TEST";
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, state) in states.iter_mut().enumerate().skip(1) {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    &mut epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                let bsync_reqest = cmds
                    .iter()
                    .find(|&c| matches!(c, ConsensusCommand::RequestSync { .. }));
                // observing a qc that link to root should not trigger anything
                assert!(bsync_reqest.is_none());
            }
            blocks.push(verified_message.block);
        }

        // now timeout someone
        let cmds = states[1].handle_timeout_expiry(&mut metrics[1]);
        let tmo: Vec<&Timeout<SignatureCollectionType>> = cmds
            .iter()
            .filter_map(|cmd| match cmd {
                PacemakerCommand::PrepareTimeout(tmo) => Some(tmo),
                _ => None,
            })
            .collect();

        assert_eq!(tmo.len(), 1);
        assert_eq!(tmo[0].tminfo.round, Round(4));
        let author = states[1].nodeid;
        let timeout_msg = TimeoutMessage::new(tmo[0].clone(), &certkeys[1]);
        let cmds = states[0].handle_timeout_message(
            author,
            timeout_msg,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );

        let req: Vec<(NodeId<_>, BlockId)> = cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                _ => None,
            })
            .collect();
        assert_eq!(req.len(), 1);
        assert_eq!(req[0].1, blocks[2].0.get_id());
    }

    #[test]
    fn test_observe_qc_through_proposal() {
        let num_state = 5;
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_state as u32,
                || NopStateRoot,
            );
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_state).map(|_| Metrics::default()).collect();
        let version = "TEST";
        let election = SimpleRoundRobin::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (_, _, verified_message) = cp.destructure();
            blocks.push(verified_message.block);
        }
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        let cmds = states[1].handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[1],
            version,
        );
        let req: Vec<(NodeId<_>, BlockId)> = cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                _ => None,
            })
            .collect();
        assert_eq!(req.len(), 1);
        assert_eq!(req[0].1, blocks[3].0.get_id());
    }

    // Expected behaviour when leader N+2 receives the votes for N+1 before receiving the proposal
    // for N+1 is that it creates the QC, but does not send the proposal until N+1 arrives.
    #[test]
    fn test_votes_with_missing_parent_block() {
        let num_state = 4;
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_state as u32,
                || NopStateRoot,
            );

        let election = SimpleRoundRobin::default();
        let mut correct_proposal_gen = ProposalGen::<SignatureType, _>::new();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_state).map(|_| Metrics::default()).collect();
        let version = "TEST";

        let missing_round = 9;

        for i in 0..missing_round - 1 {
            let cp = correct_proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (j, state) in states.iter_mut().enumerate() {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    &mut epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[j],
                    version,
                );
                let bsync_reqest = cmds
                    .iter()
                    .find(|&c| matches!(c, ConsensusCommand::RequestSync { .. }));
                // observing a qc that link to root should not trigger anything
                assert!(bsync_reqest.is_none());
                assert_eq!(state.get_current_round(), Round(i + 1))
            }
        }
        for state in states.iter() {
            assert_eq!(state.get_current_round(), Round(missing_round - 1));
        }

        // determine the leader of round after missing round so we can skip sending them missing
        // round and then send the votes for missing round
        let epoch = epoch_manager.get_epoch(Round(missing_round + 1));
        let next_leader = election.get_leader(
            Round(missing_round + 1),
            epoch,
            val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        let mut leader_index = 0;
        let mut votes = vec![];
        let cp = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        // get the votes for missing round
        let (author, _, verified_message) = cp.destructure();
        for (i, state) in states.iter_mut().enumerate() {
            if state.nodeid != next_leader {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    &mut epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );

                let v = extract_vote_msgs(cmds);
                assert_eq!(v.len(), 1);
                votes.push((state.nodeid, v[0]));
            } else {
                leader_index = i;
            }
        }

        // deliver the votes for missing round
        for (i, (author, v)) in votes.iter().enumerate() {
            let cmds = states[leader_index].handle_vote_message(
                *author,
                *v,
                &mut empty_txpool,
                &mut epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics[leader_index],
                version,
            );
            // make sure that after super majority of votes, we are not prodcuing any proposal
            // commands
            if i >= (num_state * 2 / 3) {
                let proposal_exists = cmds.into_iter().any(|c| match c {
                    ConsensusCommand::Publish {
                        target: RouterTarget::Broadcast,
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
        let cmds = states[leader_index].handle_proposal_message(
            author,
            verified_message, // this was the missing proposal message
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[leader_index],
            version,
        );

        assert_eq!(
            states[leader_index].pacemaker.get_current_round(),
            Round(missing_round + 1)
        );
        let p = extract_proposal_broadcast(cmds);
        assert_eq!(Round(missing_round + 1), p.block.0.get_round());
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
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_state as u32,
                || NopStateRoot,
            );

        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_state).map(|_| Metrics::default()).collect();
        let version = "TEST";
        let election = SimpleRoundRobin::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();

        let verified_p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (_, _, p1) = verified_p1.destructure();

        let _ = states[0].handle_proposal_message(
            p1.block.0.author,
            p1,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );
        assert_eq!(states[0].blocktree().size(), 1);

        let verified_p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (_, _, p2) = verified_p2.destructure();

        let epoch = epoch_manager.get_epoch(Round(4));
        let invalid_author = election.get_leader(
            Round(4),
            epoch,
            val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        assert!(invalid_author != NodeId::new(states[0].get_keypair().pubkey()));
        assert!(invalid_author != p2.block.0.author);
        let invalid_b2 = Block::new(
            invalid_author,
            p2.block.0.round,
            &p2.block.0.payload,
            &p2.block.0.qc,
        );
        let invalid_p2 = ProposalMessage {
            block: UnverifiedBlock(invalid_b2),
            last_round_tc: None,
        };

        let _ = states[0].handle_proposal_message(
            invalid_p2.block.0.author,
            invalid_p2,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );

        // p2 is not added because author is not the round leader
        assert_eq!(states[0].blocktree().size(), 1);

        assert_eq!(metrics[0].consensus_events.invalid_proposal_round_leader, 1);
    }

    #[test]
    fn test_schedule_next_epoch() {
        let num_states = 2;
        let (keys, certkeys, epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let election = SimpleRoundRobin::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let version = "TEST";

        // Sequence number of the block which updates the validator set
        let update_block = epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block.0);

        // handle update_block + 2 proposals to commit the update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert!(current_epoch == Epoch(1));

            let expected_epoch_start_round = update_block_round + epoch_manager.epoch_start_delay;
            // verify that the start of next epoch is scheduled correctly
            assert!(epoch_manager.get_epoch(expected_epoch_start_round - Round(1)) == Epoch(1));
            assert!(epoch_manager.get_epoch(expected_epoch_start_round) == Epoch(2));
        }
    }

    #[test]
    fn test_advance_epoch_through_proposal_qc() {
        let num_states = 2;
        let (keys, certkeys, epoch_manager, mut val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let val_stakes = val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members()
            .iter()
            .map(|(p, s)| (*p, *s))
            .collect();
        let val_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&Epoch(1))
            .unwrap()
            .map
            .clone();
        val_epoch_map.insert(
            Epoch(2),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let mut propgen_epoch_manager = epoch_manager;
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = propgen_epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block.0);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        let expected_epoch_start_round =
            update_block_round + propgen_epoch_manager.epoch_start_delay;

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round),
                Epoch(2)
            );
        }

        propgen_epoch_manager.schedule_epoch_start(update_block, update_block_round);

        // handle proposals until the last round of the epoch
        for _ in (update_block_round.0 + 2)..(expected_epoch_start_round.0 - 1) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));
        }

        // proposal for Round(expected_epoch_start_round)
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        // observe QC to advance round and epoch
        let _ = states[0].handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_managers[0],
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );
        let current_epoch = epoch_managers[0].get_epoch(states[0].get_current_round());
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_advance_epoch_through_proposal_tc() {
        let num_states = 2;
        let (keys, certkeys, epoch_manager, mut val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let val_stakes = val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members()
            .iter()
            .map(|(p, s)| (*p, *s))
            .collect();
        let val_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&Epoch(1))
            .unwrap()
            .map
            .clone();
        val_epoch_map.insert(
            Epoch(2),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let mut propgen_epoch_manager = epoch_manager;
        let election = SimpleRoundRobin::default();
        let version = "TEST";
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = propgen_epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block.0);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        let expected_epoch_start_round =
            update_block_round + propgen_epoch_manager.epoch_start_delay;

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round),
                Epoch(2)
            );
        }

        propgen_epoch_manager.schedule_epoch_start(update_block, update_block_round);

        let val_set = val_epoch_map.get_val_set(&Epoch(1)).unwrap();
        let val_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();
        // generate TCs until the last round of the epoch
        for _ in (update_block_round.0 + 1)..(expected_epoch_start_round.0 - 1) {
            let _ = propgen.next_tc(&keys, &certkeys, val_set, val_mapping);
        }

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));
        }

        // proposal for Round(expected_epoch_start_round)
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        // observe TC to advance round and epoch
        let _ = states[0].handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            &mut epoch_managers[0],
            &val_epoch_map,
            &election,
            &mut metrics[0],
            version,
        );
        let current_epoch = epoch_managers[0].get_epoch(states[0].get_current_round());
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_advance_epoch_through_local_tc() {
        let num_states = 4;
        let (keys, certkeys, epoch_manager, mut val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let val_stakes = val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members()
            .iter()
            .map(|(p, s)| (*p, *s))
            .collect();
        let val_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&Epoch(1))
            .unwrap()
            .map
            .clone();
        val_epoch_map.insert(
            Epoch(2),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let mut propgen_epoch_manager = epoch_manager;
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let election = SimpleRoundRobin::default();
        let version = "TEST";
        let mut empty_txpool = MockTxPool::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = propgen_epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block.0);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> = state
                    .handle_proposal_message(
                        author,
                        verified_message.clone(),
                        &mut empty_txpool,
                        epoch_manager,
                        &val_epoch_map,
                        &election,
                        &mut metrics[i],
                        version,
                    );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        let expected_epoch_start_round =
            update_block_round + propgen_epoch_manager.epoch_start_delay;

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round),
                Epoch(2)
            );
        }

        propgen_epoch_manager.schedule_epoch_start(update_block, update_block_round);

        // handle proposals until the last round of the epoch
        for _ in (update_block_round.0 + 2)..(expected_epoch_start_round.0 - 1) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));
        }

        let val_set = val_epoch_map.get_val_set(&Epoch(1)).unwrap();
        let val_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();
        // generate TC for the last round of the epoch
        let tmo_msgs = propgen.next_tc(&keys, &certkeys, val_set, val_mapping);

        let (_, tmo_msgs) = tmo_msgs.split_first().unwrap();

        // handle the three timeout messages from other nodes
        for tmo_msg in tmo_msgs {
            let (author, _, tm) = tmo_msg.clone().destructure();

            let _ = states[0].handle_timeout_message(
                author,
                tm,
                &mut empty_txpool,
                &mut epoch_managers[0],
                &val_epoch_map,
                &election,
                &mut metrics[0],
                version,
            );
        }

        let current_epoch = epoch_managers[0].get_epoch(states[0].get_current_round());
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_schedule_epoch_on_blocksync() {
        let num_states = 2;
        let (keys, certkeys, epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let propgen_epoch_manager = epoch_manager;
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let version = "TEST";
        let election = SimpleRoundRobin::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block_num = propgen_epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block_num.0);

        // handle blocks until update_block - 1
        for _ in 0..(update_block_round.0 - 1) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> = state
                    .handle_proposal_message(
                        author,
                        verified_message.clone(),
                        &mut empty_txpool,
                        epoch_manager,
                        &val_epoch_map,
                        &election,
                        &mut metrics[i],
                        version,
                    );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        let (state_1, xs) = states.split_first_mut().unwrap();
        let (epoch_manager_1, xs_epoch_manager) = epoch_managers.split_first_mut().unwrap();
        let state_2 = &mut xs[0];
        let epoch_manager_2 = &mut xs_epoch_manager[0];

        let mut block_sync_blocks = Vec::new();
        // handle 3 blocks for only state 1
        for _ in (update_block_round.0 - 1)..(update_block_round.0 + 2) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();

            let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> = state_1
                .handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager_1,
                    &val_epoch_map,
                    &election,
                    &mut metrics[0],
                    version,
                );
            // state should not request blocksync
            let bsync_cmds: Vec<_> = cmds
                .iter()
                .filter_map(|c| match c {
                    ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                    _ => None,
                })
                .collect();
            assert!(bsync_cmds.is_empty());

            block_sync_blocks.push(verified_message.block);
        }

        let expected_epoch_start_round =
            update_block_round + propgen_epoch_manager.epoch_start_delay;

        // state 1 should have committed update_block and scheduled next epoch
        // verify state 1 is still in epoch 1
        let state_1_epoch = epoch_manager_1.get_epoch(state_1.get_current_round());
        assert_eq!(state_1_epoch, Epoch(1));

        // verify state 1 scheduled the next epoch correctly
        assert_eq!(
            epoch_manager_1.get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            epoch_manager_1.get_epoch(expected_epoch_start_round),
            Epoch(2)
        );

        // state 2 should still not have scheduled next epoch since it didn't commit update_block
        let state_2_epoch = epoch_manager_2.get_epoch(state_2.get_current_round());
        assert_eq!(state_2_epoch, Epoch(1));
        assert_eq!(
            epoch_manager_2.get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            epoch_manager_2.get_epoch(expected_epoch_start_round),
            Epoch(1)
        ); // STILL EPOCH 1

        // generate proposal for update_block + 3
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> = state_2
            .handle_proposal_message(
                author,
                verified_message.clone(),
                &mut empty_txpool,
                epoch_manager_2,
                &val_epoch_map,
                &election,
                &mut metrics[1],
                version,
            );
        // state 2 should request blocksync
        let bsync_cmds: Vec<_> = cmds
            .iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                _ => None,
            })
            .collect();
        assert!(bsync_cmds.len() == 1);

        blocks.push(verified_message.block);

        let val_set = val_epoch_map.get_val_set(&Epoch(1)).unwrap();
        for block in block_sync_blocks.into_iter().rev() {
            let msg = BlockSyncResponseMessage::BlockFound(block);
            // blocksync response for state 2
            let _ = state_2.handle_block_sync(
                state_1.nodeid,
                msg,
                &mut empty_txpool,
                epoch_manager_2,
                &val_epoch_map,
                &election,
                &mut metrics[1],
                version,
            );
        }

        // blocks aren't committed immediately after blocksync is finished
        // state 2 should not have scheduled the next epoch yet
        let state_2_epoch = epoch_manager_2.get_epoch(state_2.get_current_round());
        assert_eq!(state_2_epoch, Epoch(1));
        assert_eq!(
            epoch_manager_2.get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            epoch_manager_2.get_epoch(expected_epoch_start_round),
            Epoch(1)
        ); // STILL EPOCH 1

        // generate proposal for update_block + 4
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> = state_2
            .handle_proposal_message(
                author,
                verified_message,
                &mut empty_txpool,
                epoch_manager_2,
                &val_epoch_map,
                &election,
                &mut metrics[1],
                version,
            );
        // state 2 should not request blocksync
        let bsync_cmds: Vec<_> = cmds
            .iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                _ => None,
            })
            .collect();
        assert!(bsync_cmds.is_empty());

        // state 2 should have committed the update block and scheduled the next epoch
        let state_2_epoch = epoch_manager_2.get_epoch(state_2.get_current_round());
        assert_eq!(state_2_epoch, Epoch(1));
        assert_eq!(
            epoch_manager_2.get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            epoch_manager_2.get_epoch(expected_epoch_start_round),
            Epoch(2)
        ); // NOW EPOCH 2
    }

    #[test]
    fn test_advance_epoch_with_blocksync() {
        let num_states = 2;
        let (keys, certkeys, epoch_manager, mut val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let val_stakes = val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members()
            .iter()
            .map(|(p, s)| (*p, *s))
            .collect();
        let val_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&Epoch(1))
            .unwrap()
            .map
            .clone();
        val_epoch_map.insert(
            Epoch(2),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let mut propgen_epoch_manager = epoch_manager;
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let version = "TEST";
        let election = SimpleRoundRobin::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = propgen_epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block.0);

        let expected_epoch_start_round =
            update_block_round + propgen_epoch_manager.epoch_start_delay;

        // handle proposals until expected_epoch_start_round - 1
        for _ in 0..(expected_epoch_start_round.0 - 1) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        for (state, epoch_manager) in states.iter().zip(epoch_managers.iter()) {
            // verify all states are still in epoch 1
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                epoch_manager.get_epoch(expected_epoch_start_round),
                Epoch(2)
            );
        }

        propgen_epoch_manager.schedule_epoch_start(update_block, update_block_round);

        // skip block on round expected_epoch_start_round
        let _unused_proposal = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        // generate proposal for expected_epoch_start_round + 1
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        for (i, (state, epoch_manager)) in
            states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
        {
            let cmds = state.handle_proposal_message(
                author,
                verified_message.clone(),
                &mut empty_txpool,
                epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics[i],
                version,
            );
            // state should not request blocksync
            let bsync_cmds: Vec<_> = cmds
                .iter()
                .filter_map(|c| match c {
                    ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                    _ => None,
                })
                .collect();
            // state should have requested blocksync
            assert!(bsync_cmds.len() == 1);

            // verify state is now in epoch 2
            let current_epoch = epoch_manager.get_epoch(state.get_current_round());
            assert_eq!(current_epoch, Epoch(2));
        }
    }

    #[test]
    fn test_vote_sent_to_leader_in_next_epoch() {
        let num_states = 2;
        let version = "TEST";
        let (keys, certkeys, epoch_manager, mut val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );

        // generate a random key as a validator in epoch 2
        let epoch_2_leader = NodeId::new(get_key::<SignatureType>(100).pubkey());
        val_epoch_map.insert(
            Epoch(2),
            vec![(epoch_2_leader, Stake(1))],
            ValidatorMapping::new(vec![(epoch_2_leader, epoch_2_leader.pubkey())]),
        );

        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let mut propgen_epoch_manager = epoch_manager;
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let mut propgen = ProposalGen::<SignatureType, _>::new();

        // Sequence number of the block which updates the validator set
        let update_block = propgen_epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = Round(update_block.0);

        let expected_epoch_start_round =
            update_block_round + propgen_epoch_manager.epoch_start_delay;

        propgen_epoch_manager.schedule_epoch_start(update_block, update_block_round);

        // commit blocks until the last round of the epoch
        for _ in 0..(expected_epoch_start_round.0 - 2) {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &propgen_epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in
                states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }
        }

        // handle proposal on the last round of the epoch
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &propgen_epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        for (i, (state, epoch_manager)) in
            states.iter_mut().zip(epoch_managers.iter_mut()).enumerate()
        {
            let cmds = state.handle_proposal_message(
                author,
                verified_message.clone(),
                &mut empty_txpool,
                epoch_manager,
                &val_epoch_map,
                &election,
                &mut metrics[i],
                version,
            );
            // state should send vote to the leader in epoch 2
            let vote_messages = extract_vote_msgs(cmds);
            assert!(vote_messages.len() == 1);
        }
    }

    #[test]
    fn test_remove_old_blocksync_requests() {
        let (keys, certkeys, mut epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4, || {
                NopStateRoot
            });
        let election = SimpleRoundRobin::default();
        let mut empty_txpool = MockTxPool::default();
        let state = &mut states[0];
        let mut metrics = Metrics::default();
        let version = "TEST";
        let mut propgen = ProposalGen::<SignatureType, _>::new();

        // round 1 proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author_1, _, verified_message_1) = p1.destructure();
        let block_1_id = verified_message_1.block.0.get_id();

        // round 2 proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author_2, _, verified_message_2) = p2.destructure();
        let _ = state.handle_proposal_message(
            author_2,
            verified_message_2,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        // should be in round 2 and request block 1
        assert!(state.get_current_round() == Round(2));
        assert!(state
            .block_sync_requester()
            .requests()
            .contains_key(&block_1_id));

        // handle proposal for block 1
        let _ = state.handle_proposal_message(
            author_1,
            verified_message_1,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        // should still be in round 2 and block 1 request should exist
        assert!(state.get_current_round() == Round(2));
        assert!(state
            .block_sync_requester()
            .requests()
            .contains_key(&block_1_id));

        // round 3 proposal
        let p3 = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author_3, _, verified_message_3) = p3.destructure();
        let _ = state.handle_proposal_message(
            author_3,
            verified_message_3,
            &mut empty_txpool,
            &mut epoch_manager,
            &val_epoch_map,
            &election,
            &mut metrics,
            version,
        );
        // should be in round 3 and remove block 1 request
        assert!(state.get_current_round() == Round(3));
        assert!(!state
            .block_sync_requester()
            .requests()
            .contains_key(&block_1_id));
        assert!(state.block_sync_requester().requests().is_empty());
    }

    #[test]
    fn test_request_over_state_sync_threshold() {
        let num_states = 4;
        let (keys, certkeys, epoch_manager, val_epoch_map, mut states) =
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(
                num_states as u32,
                || NopStateRoot,
            );
        let mut epoch_managers = vec![epoch_manager.clone(); num_states];
        let election = SimpleRoundRobin::default();
        let mut propgen = ProposalGen::<SignatureType, _>::new();
        let mut blocks = vec![];
        let mut empty_txpool = MockTxPool::default();
        let mut metrics: Vec<Metrics> = (0..num_states).map(|_| Metrics::default()).collect();
        let version = "TEST";

        // Sequence number of the block after which state sync should be triggered
        let state_sync_threshold_block = states[0].config.state_sync_threshold;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let state_sync_threshold_round = Round(state_sync_threshold_block.0);

        let (first_state, other_states) = states.split_first_mut().unwrap();
        let (first_state_epoch_manager, other_epoch_managers) =
            epoch_managers.split_first_mut().unwrap();
        let (first_state_metrics, other_metrics) = metrics.split_first_mut().unwrap();

        // handle proposals for 3 states until state_sync_threshold
        for _ in 0..state_sync_threshold_round.0 {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &epoch_manager,
                &val_epoch_map,
                &election,
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for (i, (state, epoch_manager)) in other_states
                .iter_mut()
                .zip(other_epoch_managers.iter_mut())
                .enumerate()
            {
                let cmds = state.handle_proposal_message(
                    author,
                    verified_message.clone(),
                    &mut empty_txpool,
                    epoch_manager,
                    &val_epoch_map,
                    &election,
                    &mut other_metrics[i],
                    version,
                );
                // state should not request blocksync
                let bsync_cmds: Vec<_> = cmds
                    .iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
                        _ => None,
                    })
                    .collect();
                assert!(bsync_cmds.is_empty());
            }

            blocks.push(verified_message.block);
        }

        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = cp.destructure();
        let _cmds = first_state.handle_proposal_message(
            author,
            verified_message,
            &mut empty_txpool,
            first_state_epoch_manager,
            &val_epoch_map,
            &election,
            first_state_metrics,
            version,
        );

        // Should trigger state sync (only metric assertion for now)
        assert_eq!(first_state_metrics.consensus_events.trigger_state_sync, 1);
    }
}
