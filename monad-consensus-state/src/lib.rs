use std::{collections::HashSet, marker::PhantomData, time::Duration};

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
    block::{Block, BlockPolicy, BlockType},
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
pub struct ConsensusState<ST, SCT, BPT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
{
    /// This nodes public NodeId; what other nodes see in the validator set
    nodeid: NodeId<SCT::NodeIdPubKey>,
    /// Parameters for consensus algorithm behaviour
    config: ConsensusConfig,
    /// Prospective blocks are stored here while they wait to be
    /// committed
    pending_block_tree: BlockTree<SCT, BPT>,
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
    block_validator: BVT,
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

impl<ST, SCT, BPT, BVT, SVT> PartialEq for ConsensusState<ST, SCT, BPT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
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
impl<ST, SCT, BPT, BVT, SVT> Eq for ConsensusState<ST, SCT, BPT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
{
}

impl<ST, SCT, BPT, BVT, SVT> std::fmt::Debug for ConsensusState<ST, SCT, BPT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
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
pub enum ConsensusAction<SCT: SignatureCollection, BPT: BlockPolicy<SCT>> {
    /// Create a proposal with this state-root-hash and txn hash list
    Propose(
        StateRootHash,
        HashSet<<BPT::ValidatedBlock as BlockType<SCT>>::TxnHash>,
    ),
    /// Create an empty block proposal
    ProposeEmpty,
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

pub struct NodeState<'a, ST, SCT, VTF, LT, TT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub tx_pool: &'a mut TT,
    pub epoch_manager: &'a mut EpochManager,
    pub val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    pub election: &'a LT,
    pub metrics: &'a mut Metrics,
    pub version: &'a str,

    pub _phantom: PhantomData<ST>,
}

impl<ST, SCT, BPT, BVT, SVT> ConsensusState<ST, SCT, BPT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
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
    pub fn handle_timeout_expiry(
        &mut self,
        epoch_manager: &EpochManager,
        metrics: &mut Metrics,
    ) -> Vec<PacemakerCommand<SCT>> {
        metrics.consensus_events.local_timeout += 1;
        debug!(
            "local timeout: round={:?}",
            self.pacemaker.get_current_round()
        );
        self.pacemaker
            .handle_event(&mut self.safety, &self.high_qc, epoch_manager)
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
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        let _handle_proposal_span = tracing::info_span!("handle_proposal_span", ?author).entered();
        debug!("Proposal Message: {:?}", p);
        node.metrics.consensus_events.handle_proposal += 1;
        let mut cmds = Vec::new();

        let epoch = node.epoch_manager.get_epoch(p.block.round);
        let validator_set = node
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("proposal message was verified");

        let state_root_action = self.state_root_hash_validation(&p, node.metrics);

        if matches!(state_root_action, StateRootAction::Reject) {
            return cmds;
        }

        cmds.extend(self.proposal_certificate_handling(
            &p,
            node.epoch_manager,
            validator_set,
            node.metrics,
            node.version,
        ));

        // author, leader, round checks
        let round = self.pacemaker.get_current_round();
        let block_round_leader = node
            .election
            .get_leader(p.block.get_round(), validator_set.get_members());
        if p.block.get_round() > round
            || author != block_round_leader
            || p.block.get_author() != block_round_leader
        {
            debug!(
                "Invalid proposal: expected-round={:?} \
                round={:?} \
                expected-leader={:?} \
                author={:?} \
                block-author={:?}",
                round,
                p.block.get_round(),
                block_round_leader,
                author,
                p.block.get_author()
            );
            node.metrics.consensus_events.invalid_proposal_round_leader += 1;
            return cmds;
        }

        let Some(block) = self.block_validator.validate(p.block) else {
            warn!("Transaction validation failed");
            node.metrics.consensus_events.failed_txn_validation += 1;
            return cmds;
        };

        let author_pubkey = node
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("proposal message was verified")
            .map
            .get(&author)
            .expect("proposal author exists in validator_mapping");
        if !self.block_validator.other_validation(&block, author_pubkey) {
            node.metrics
                .consensus_events
                .failed_verify_randao_reveal_sig += 1;
            return cmds;
        }

        // at this point, block is valid and can be added to the blocktree
        cmds.extend(self.try_add_blocktree(&block, Some(state_root_action), node));

        // out-of-order proposals are possible if some round R+1 proposal arrives
        // before R because of network conditions. The proposals are still valid
        if block.get_round() != round {
            debug!(
                "Out-of-order proposal: expected-round={:?} \
                round={:?}",
                round,
                block.get_round(),
            );
            node.metrics.consensus_events.out_of_order_proposals += 1;
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
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        debug!("Vote Message: {:?}", vote_msg);
        if vote_msg.vote.vote_info.round < self.pacemaker.get_current_round() {
            node.metrics.consensus_events.old_vote_received += 1;
            return Default::default();
        }
        node.metrics.consensus_events.vote_received += 1;

        let mut cmds = Vec::new();

        let epoch = node.epoch_manager.get_epoch(vote_msg.vote.vote_info.round);
        let validator_set = node
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("vote message was verified");
        let validator_mapping = node
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("vote message was verified");
        let (maybe_qc, vote_state_cmds) =
            self.vote_state
                .process_vote(&author, &vote_msg, validator_set, validator_mapping);
        cmds.extend(vote_state_cmds.into_iter().map(Into::into));

        if let Some(qc) = maybe_qc {
            debug!("Created QC {:?}", qc);
            node.metrics.consensus_events.created_qc += 1;

            cmds.extend(self.process_certificate_qc(
                &qc,
                node.epoch_manager,
                validator_set,
                node.metrics,
                node.version,
            ));

            cmds.extend(self.try_propose(node));
        };

        cmds
    }

    /// handling remote timeout messages from other nodes
    #[must_use]
    pub fn handle_timeout_message<VTF, LT, TT>(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        tmo_msg: TimeoutMessage<SCT>,
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        let tm = &tmo_msg.timeout;
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            node.metrics.consensus_events.old_remote_timeout += 1;
            return cmds;
        }

        debug!("Remote timeout msg: {:?}", tm);
        node.metrics.consensus_events.remote_timeout_msg += 1;

        let epoch = node.epoch_manager.get_epoch(tm.tminfo.round);
        let validator_set = node
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("timeout message was verified");
        let validator_mapping = node
            .val_epoch_map
            .get_cert_pubkeys(&epoch)
            .expect("timeout message was verified");

        let process_certificate_cmds = self.process_certificate_qc(
            &tm.tminfo.high_qc,
            node.epoch_manager,
            validator_set,
            node.metrics,
            node.version,
        );
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            node.metrics.consensus_events.remote_timeout_msg_with_tc += 1;
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc, node.metrics)
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        &self.keypair,
                        &self.cert_keypair,
                        node.version,
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
                node.epoch_manager,
                &mut self.safety,
                &self.high_qc,
                author,
                tmo_msg,
            );

        cmds.extend(remote_timeout_cmds.into_iter().map(|cmd| {
            ConsensusCommand::from_pacemaker_command(
                &self.keypair,
                &self.cert_keypair,
                node.version,
                cmd,
            )
        }));
        if let Some(tc) = tc {
            debug!("Created TC: {:?}", tc);
            node.metrics.consensus_events.created_tc += 1;
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(&tc, node.metrics)
                .into_iter()
                .map(|cmd| {
                    ConsensusCommand::from_pacemaker_command(
                        &self.keypair,
                        &self.cert_keypair,
                        node.version,
                        cmd,
                    )
                });
            cmds.extend(advance_round_cmds);
            cmds.extend(self.try_propose(node));
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
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        let mut cmds = vec![];

        let current_epoch = node.epoch_manager.get_epoch(self.get_current_round());
        let val_set = node
            .val_epoch_map
            .get_val_set(&current_epoch)
            .expect("current validator set should be in the map");

        let bid = msg.get_block_id();
        let block_sync_result = self.block_sync_requester.handle_response(
            &author,
            msg,
            val_set,
            &self.block_validator,
            node.metrics,
        );
        block_sync_result.log(bid, node.metrics);

        match block_sync_result {
            BlockSyncResult::Success(block) => {
                if self.pending_block_tree.is_valid_to_insert(&block) {
                    cmds.extend(self.request_block_if_missing_ancestor(
                        block.get_qc(),
                        val_set,
                        node.metrics,
                    ));

                    cmds.extend(self.try_add_blocktree(&block, None, node));
                }
            }
            BlockSyncResult::Failed(retry_cmd) => cmds.extend(retry_cmd),
            BlockSyncResult::UnexpectedResponse => {
                // In an all honest environment, this is likely due to block
                // sync taking too long. The first block sync request times out,
                // and the state issues another request, wiping the record for
                // the first request. When the first response comes back, it
                // triggers this warning
                warn!("Block sync unexpected response: author={:?}", author);
            }
        }
        cmds
    }

    /// a blocksync request could be for a block that is not yet committed so we
    /// try and fetch it from the blocktree
    pub fn fetch_uncommitted_block(&self, bid: &BlockId) -> Option<&Block<SCT>> {
        self.pending_block_tree
            .get_block(bid)
            .map(|b| b.get_unvalidated_block_ref())
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
                .is_coherent(&qc.info.vote.vote_info.parent_id)
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
                    if block.is_txn_list_empty() {
                        metrics.consensus_events.commit_empty_block += 1;
                    }
                    metrics.consensus_events.committed_bytes += block.get_txn_list_len() as u64;
                }

                cmds.extend(
                    blocks_to_commit.iter().map(|b| {
                        ConsensusCommand::StateRootHash(b.clone().get_unvalidated_block())
                    }),
                );
                let unvalidated_blocks = blocks_to_commit
                    .into_iter()
                    .map(|b| b.get_unvalidated_block())
                    .collect();
                cmds.push(ConsensusCommand::<ST, SCT>::LedgerCommit(
                    unvalidated_blocks,
                ));
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
        block: &BPT::ValidatedBlock,
        state_root_action: Option<StateRootAction>,
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        self.pending_block_tree
            .add(block.clone())
            .expect("Failed to add block to blocktree");

        let mut cmds = Vec::new();

        // if the current round is the same as block round, try to vote. else, try to propose
        if block.get_round() == self.pacemaker.get_current_round() {
            // TODO: This block could be received via blocksync. Retrieve the block received
            // this round and try to vote for that
            let state_root_action =
                state_root_action.expect("should exist if proposal was handled this round");
            cmds.extend(self.try_vote(block, state_root_action, node));
        } else {
            cmds.extend(self.try_propose(node));
        }

        cmds
    }

    #[must_use]
    fn try_vote<VTF, LT, TT>(
        &mut self,
        validated_block: &BPT::ValidatedBlock,
        state_root_action: StateRootAction,
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        let mut cmds = Vec::new();
        let round = self.pacemaker.get_current_round();

        // check that the block is coherent
        if !self
            .pending_block_tree
            .is_coherent(&validated_block.get_id())
        {
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
        let last_tc = self.pacemaker.get_last_round_tc();
        let vote = self.safety.make_vote::<SCT, BPT>(validated_block, last_tc);

        if let Some(v) = vote {
            let vote_msg = VoteMessage::<SCT>::new(v, &self.cert_keypair);

            // TODO this grouping should be enforced by epoch_manager/val_epoch_map to be less
            // error-prone
            let (next_round, next_validator_set) = {
                let next_round = round + Round(1);
                let next_epoch = node.epoch_manager.get_epoch(next_round);
                let Some(next_validator_set) = node.val_epoch_map.get_val_set(&next_epoch) else {
                    todo!("handle non-existent validatorset for next round epoch");
                };
                (next_round, next_validator_set.get_members())
            };
            let next_leader = node.election.get_leader(next_round, next_validator_set);
            let msg = ConsensusMessage {
                version: node.version.into(),
                message: ProtocolMessage::Vote(vote_msg),
            }
            .sign(&self.keypair);
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(next_leader),
                message: msg,
            };
            debug!("Created Vote: vote={:?} next_leader={:?}", v, next_leader);
            node.metrics.consensus_events.created_vote += 1;
            cmds.push(send_cmd);
        }

        cmds
    }

    #[must_use]
    fn try_propose<VTF, LT, TT>(
        &mut self,
        node: &mut NodeState<ST, SCT, VTF, LT, TT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        let mut cmds = Vec::new();

        let (round, validator_set) = {
            let round = self.pacemaker.get_current_round();
            // TODO this grouping should be enforced by epoch_manager/val_epoch_map to be less
            // error-prone
            let epoch = node.epoch_manager.get_epoch(round);
            let Some(validator_set) = node.val_epoch_map.get_val_set(&epoch) else {
                todo!("handle non-existent validatorset for next round epoch");
            };
            (round, validator_set)
        };

        // check that self is leader
        if self.nodeid != node.election.get_leader(round, validator_set.get_members()) {
            return cmds;
        }

        // make sure we haven't proposed in this round
        if round <= self.last_proposed_round {
            return cmds;
        }

        // check that we have path to root and block is coherent
        if !self
            .pending_block_tree
            .is_coherent(&self.high_qc.get_block_id())
        {
            return cmds;
        }

        // we passed all try_propose guards, so begin proposing
        self.last_proposed_round = round;

        let last_round_tc = self.pacemaker.get_last_round_tc().clone();
        cmds.extend(self.process_new_round_event(
            node.tx_pool,
            validator_set,
            last_round_tc,
            node.epoch_manager.get_epoch(round),
            node.metrics,
            node.version,
        ));
        cmds
    }

    /// called when the node is entering a new round and is the leader for that round
    /// TODO this function can be folded into try_propose; it's only called there
    #[must_use]
    fn process_new_round_event<VT, TT: TxPool<SCT, BPT>>(
        &mut self,
        txpool: &mut TT,
        validators: &VT,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
        epoch: Epoch,
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
                    epoch,
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
                    block: b,
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
                let _create_proposal_span =
                    tracing::info_span!("create_proposal_span", ?round).entered();
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
    fn proposal_policy(
        &self,
        parent_bid: &BlockId,
        proposed_seq_num: SeqNum,
    ) -> ConsensusAction<SCT, BPT> {
        // TODO when statesync ready - Never propose while syncing

        // Can't propose txs without state root hash
        let Some(h) = self
            .state_root_validator
            .get_next_state_root(proposed_seq_num)
        else {
            return ConsensusAction::ProposeEmpty;
        };

        // Propose when there's a path to root
        let pending_blocktree_tx_hashes = self
            .pending_block_tree
            .get_tx_hashes_on_path_to_root(parent_bid)
            .expect("there should be a path to root");

        ConsensusAction::Propose(h, pending_blocktree_tx_hashes)
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
                // TODO: This should trigger statesync.
                warn!(
                    "Lagging over {:?} blocks behind. Trigger statesync",
                    self.config.state_sync_threshold
                );

                metrics.consensus_events.trigger_state_sync += 1;

                // Remove older blocks from the blocktree
                self.pending_block_tree
                    .remove_old_blocks(qc.get_seq_num() - self.config.state_sync_threshold);

                vec![]
            } else {
                self.block_sync_requester
                    .request::<VT>(&qc, validators, metrics)
            }
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
        if p.block.payload.txns == FullTransactionList::empty()
            && p.block.payload.header.state_root == INITIAL_DELAY_STATE_ROOT_HASH
        {
            debug!("Received empty block: block={:?}", p.block);
            metrics.consensus_events.rx_empty_block += 1;
            return StateRootAction::Proceed;
        }
        match self
            .state_root_validator
            .validate(p.block.payload.seq_num, p.block.payload.header.state_root)
        {
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
            self.process_certificate_qc(&p.block.qc, epoch_manager, validators, metrics, version);
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

    pub fn blocktree(&self) -> &BlockTree<SCT, BPT> {
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
    use std::{marker::PhantomData, ops::Deref, time::Duration};

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
        block::{Block, BlockPolicy, BlockType, PassthruBlockPolicy},
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
        txpool::{MockTxPool, TxPool},
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
        },
        hasher::Hash,
        NopPubKey, NopSignature,
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
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use test_case::test_case;
    use tracing_test::traced_test;

    use crate::{ConsensusCommand, ConsensusConfig, ConsensusState, NodeState};

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateRootValidatorType = NopStateRoot;

    struct NodeContext<'a, ST, SCT, BPT, VTF, SVT, LT, TT>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<SCT, ValidatedBlock = Block<SCT>>,
        SVT: StateRootValidator,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        epoch_manager: EpochManager,
        val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
        txpool: TT,
        election: LT,
        metrics: Metrics,
        version: &'a str,

        consensus_state: ConsensusState<ST, SCT, BPT, MockValidator, SVT>,

        phantom: PhantomData<ST>,
    }

    impl<'a, ST, SCT, BPT, VTF, SVT, LT, TT> NodeContext<'a, ST, SCT, BPT, VTF, SVT, LT, TT>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<SCT, ValidatedBlock = Block<SCT>>,
        SVT: StateRootValidator,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        TT: TxPool<SCT, BPT>,
    {
        fn get_state(
            &'a mut self,
        ) -> (
            &mut ConsensusState<ST, SCT, BPT, MockValidator, SVT>,
            NodeState<'a, ST, SCT, VTF, LT, TT>,
        ) {
            (
                &mut self.consensus_state,
                NodeState {
                    epoch_manager: &mut self.epoch_manager,
                    val_epoch_map: &self.val_epoch_map,
                    election: &self.election,
                    tx_pool: &mut self.txpool,
                    metrics: &mut self.metrics,
                    version: self.version,
                    _phantom: PhantomData,
                },
            )
        }

        fn handle_proposal_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: ProposalMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            let mut n = NodeState {
                epoch_manager: &mut self.epoch_manager,
                val_epoch_map: &self.val_epoch_map,
                election: &self.election,
                tx_pool: &mut self.txpool,
                metrics: &mut self.metrics,
                version: self.version,
                _phantom: PhantomData,
            };

            self.consensus_state
                .handle_proposal_message(author, p, &mut n)
        }

        fn handle_timeout_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: TimeoutMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            let mut n = NodeState {
                epoch_manager: &mut self.epoch_manager,
                val_epoch_map: &self.val_epoch_map,
                election: &self.election,
                tx_pool: &mut self.txpool,
                metrics: &mut self.metrics,
                version: self.version,
                _phantom: PhantomData,
            };

            self.consensus_state
                .handle_timeout_message(author, p, &mut n)
        }

        fn handle_vote_message(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: VoteMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            let mut n = NodeState {
                epoch_manager: &mut self.epoch_manager,
                val_epoch_map: &self.val_epoch_map,
                election: &self.election,
                tx_pool: &mut self.txpool,
                metrics: &mut self.metrics,
                version: self.version,
                _phantom: PhantomData,
            };

            self.consensus_state.handle_vote_message(author, p, &mut n)
        }

        fn handle_block_sync(
            &mut self,
            author: NodeId<SCT::NodeIdPubKey>,
            p: BlockSyncResponseMessage<SCT>,
        ) -> Vec<ConsensusCommand<ST, SCT>> {
            let mut n = NodeState {
                epoch_manager: &mut self.epoch_manager,
                val_epoch_map: &self.val_epoch_map,
                election: &self.election,
                tx_pool: &mut self.txpool,
                metrics: &mut self.metrics,
                version: self.version,
                _phantom: PhantomData,
            };

            self.consensus_state.handle_block_sync(author, p, &mut n)
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
                FullTransactionList::empty(),
                ExecutionArtifacts::zero(),
            )
        }

        fn next_proposal(
            &mut self,
            txn_list: FullTransactionList,
            execution_hdr: ExecutionArtifacts,
        ) -> Verified<ST, ProposalMessage<SCT>> {
            self.proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                txn_list,
                execution_hdr,
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
                FullTransactionList::new(vec![5].into()),
                ExecutionArtifacts::zero(),
            )
        }

        // TODO come up with better API for making mal proposals relative to state of proposal_gen
        fn branch_proposal(
            &mut self,
            txn_list: FullTransactionList,
            execution_hdr: ExecutionArtifacts,
        ) -> Verified<ST, ProposalMessage<SCT>> {
            self.malicious_proposal_gen.next_proposal(
                &self.keys,
                &self.cert_keys,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
                txn_list,
                execution_hdr,
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
        BPT: BlockPolicy<SCT, ValidatedBlock = Block<SCT>>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        SVT: StateRootValidator,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
        TT: TxPool<SCT, BPT> + Clone,
    >(
        num_states: u32,
        valset_factory: VTF,
        election: LT,
        txpool: TT,
        state_root: impl Fn() -> SVT,
    ) -> (
        EnvContext<ST, SCT, VTF, LT>,
        Vec<NodeContext<'static, ST, SCT, BPT, VTF, SVT, LT, TT>>,
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

        let ctxs: Vec<NodeContext<_, _, _, _, _, _, _>> = (0..num_states)
            .map(|i| {
                let mut val_epoch_map = ValidatorsEpochMapping::new(valset_factory.clone());
                val_epoch_map.insert(
                    Epoch(1),
                    val_stakes.clone(),
                    ValidatorMapping::new(val_cert_pubkeys.clone()),
                );
                let epoch_manager = EpochManager::new(SeqNum(100), Round(20));

                let default_key =
                    <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut [127; 32]).unwrap();
                let default_cert_key =
                    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(
                        &mut [127; 32],
                    )
                    .unwrap();
                let cs = ConsensusState::<ST, SCT, BPT, _, SVT>::new(
                    MockValidator,
                    state_root(),
                    keys[i as usize].pubkey(),
                    ConsensusConfig {
                        proposal_txn_limit: 5000,
                        proposal_gas_limit: 8_000_000,
                        delta: Duration::from_secs(1),
                        max_blocksync_retries: 5,
                        state_sync_threshold: SeqNum(100),
                    },
                    EthAddress::default(),
                    std::mem::replace(&mut dupkeys[i as usize], default_key),
                    std::mem::replace(&mut dupcertkeys[i as usize], default_cert_key),
                );

                NodeContext {
                    epoch_manager,
                    val_epoch_map,
                    txpool: txpool.clone(),
                    election: election.clone(),
                    metrics: Metrics::default(),
                    version: "TEST",
                    consensus_state: cs,
                    phantom: PhantomData,
                }
            })
            .collect();

        let mut val_epoch_map = ValidatorsEpochMapping::new(valset_factory.clone());
        val_epoch_map.insert(
            Epoch(1),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        let epoch_manager = EpochManager::new(SeqNum(100), Round(20));

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

    fn extract_blocksync_requests<ST, SCT>(cmds: Vec<ConsensusCommand<ST, SCT>>) -> Vec<BlockId>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        cmds.into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
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

    // genesis_qc start with "-1" sequence number and Round(0)
    // hence round == seqnum + 1 if no round times out
    fn seqnum_to_round_no_tc(seq_num: SeqNum) -> Round {
        Round(seq_num.0 + 1)
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );

        let (consensus_state, mut node_state) = ctx[0].get_state();
        assert_eq!(consensus_state.high_qc.get_round(), Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
            seq_num: SeqNum(0),
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

        let _ = consensus_state.handle_vote_message(*v1.author(), *v1, &mut node_state);
        let _ = consensus_state.handle_vote_message(*v2.author(), *v2, &mut node_state);

        // less than 2f+1, so expect not locked
        assert_eq!(consensus_state.high_qc.get_round(), Round(0));

        let _ = consensus_state.handle_vote_message(*v3.author(), *v3, &mut node_state);
        assert_eq!(consensus_state.high_qc.get_round(), expected_qc_high_round);
        assert_eq!(node_state.metrics.consensus_events.vote_received, 3);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (consensus_state, mut node_state) = ctx[0].get_state();
        let p1 = env.next_proposal_empty();

        // local timeout for state in Round 1
        assert_eq!(consensus_state.pacemaker.get_current_round(), Round(1));
        let _ = consensus_state.pacemaker.handle_event(
            &mut consensus_state.safety,
            &consensus_state.high_qc,
            &env.epoch_manager,
        );

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        let result = extract_vote_msgs(cmds);

        assert_eq!(consensus_state.pacemaker.get_current_round(), Round(1));
        assert_eq!(result.len(), 1);

        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        let result = extract_vote_msgs(cmds);

        assert_eq!(consensus_state.pacemaker.get_current_round(), Round(2));
        assert_eq!(result.len(), 1);

        for _ in 0..4 {
            env.next_proposal_empty();
        }
        let p7 = env.next_proposal_empty();
        let (author, _, verified_message) = p7.destructure();
        let _ = consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

        assert_eq!(consensus_state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn duplicate_proposals() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        let result = extract_vote_msgs(cmds);
        assert_eq!(result.len(), 1);

        // send duplicate of p1, expect it to be ignored and no output commands
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
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
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        // first proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        let result = extract_vote_msgs(cmds);

        assert_eq!(consensus_state.pacemaker.get_current_round(), Round(1));
        assert_eq!(result.len(), 1);

        // second proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        let result = extract_vote_msgs(cmds);

        assert_eq!(consensus_state.pacemaker.get_current_round(), Round(2));
        assert_eq!(result.len(), 1);

        let mut missing_proposals = Vec::new();
        for _ in 0..perms.len() {
            missing_proposals.push(env.next_proposal_empty());
        }

        // last proposal arrvies
        let p_fut = env.next_proposal_empty();
        let (author, _, verified_message) = p_fut.destructure();
        let _ = consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

        // was in Round(2) and skipped over perms.len() proposals. Handling
        // p_fut should be at Round(3+perms.len())
        assert_eq!(
            consensus_state.pacemaker.get_current_round(),
            Round(3 + perms.len() as u64)
        );
        assert_eq!(consensus_state.pending_block_tree.size(), 3);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(consensus_state.handle_proposal_message(
                author,
                verified_message,
                &mut node_state,
            ));
        }

        // next proposal will trigger everything to be committed if there is
        // a consecutive chain as expected
        // last proposal arrvies
        let p_to_validate_blocks = env.next_proposal_empty();
        let (author, _, verified_message) = p_to_validate_blocks.destructure();
        let _ = consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

        assert_eq!(consensus_state.pending_block_tree.size(), 2);
        assert_eq!(
            consensus_state.pacemaker.get_current_round(),
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        // round 1 proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let p1_cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

        let p1_votes = extract_vote_msgs(p1_cmds);
        assert!(p1_votes.len() == 1);
        assert!(p1_votes[0].vote.ledger_commit_info.is_commitable());

        // round 2 proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        assert!(find_commit_cmd(&p2_cmds).is_none());

        let p2_votes = extract_vote_msgs(p2_cmds);
        assert!(p2_votes.len() == 1);
        // csh is some: the proposal and qc have consecutive rounds
        assert!(p2_votes[0].vote.ledger_commit_info.is_commitable());

        let p3 = env.next_proposal_empty();
        let (author, _, verified_message) = p3.destructure();

        let p2_cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        assert!(find_commit_cmd(&p2_cmds).is_some());
    }
    #[test]
    fn test_commit_rule_non_consecutive() {
        let num_state = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        // round 1 proposal
        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let _ = consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

        // round 2 proposal
        let p2 = env.next_proposal_empty();
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

        let p2_votes = extract_vote_msgs(p2_cmds);
        assert!(p2_votes.len() == 1);
        assert!(p2_votes[0].vote.ledger_commit_info.is_commitable());

        // round 2 timeout
        let pacemaker_cmds = consensus_state.pacemaker.handle_event(
            &mut consensus_state.safety,
            &consensus_state.high_qc,
            &env.epoch_manager,
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
        let p3_cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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

        let cp1 = env.next_proposal_empty();
        let mp1 = env.mal_proposal_empty();

        let (author_1, _, proposal_message_1) = cp1.destructure();
        let block_1 = proposal_message_1.block.clone();
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
        // temp sub with a key that doesn't exists.
        let mut routing_target = NodeId::new(get_key::<SignatureType>(100).pubkey());
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
                let (target, sync_bid) = match res.unwrap() {
                    ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                    _ => None,
                }
                .unwrap();
                assert!(sync_bid == block_1.get_id());
                routing_target = target;
            }
        }
        assert_ne!(
            routing_target,
            NodeId::new(get_key::<SignatureType>(100).pubkey())
        );
        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(n2.consensus_state.high_qc.info.vote, votes[0].vote);

        // use the correct proposal gen to make next proposal and send it to second_state
        // this should cause it to emit the RequestBlockSync Message because the the QC parent
        // points to a different proposal (second_state has the malicious proposal in its blocktree)
        let cp2 = env.next_proposal_empty();
        let (author_2, _, proposal_message_2) = cp2.destructure();
        let block_2 = proposal_message_2.block.clone();
        let cmds2 = n2.handle_proposal_message(author_2, proposal_message_2.clone());
        // Blocksync already requested with the created QC, should not request for same
        // block again unless the first request fails
        assert!(find_blocksync_request(&cmds2).is_none());

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = n1.handle_proposal_message(author_2, proposal_message_2.clone());
        assert!(find_blocksync_request(&cmds1).is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = env.next_proposal_empty();
        let (author_3, _, proposal_message_3) = cp3.destructure();
        let block_3 = proposal_message_3.block.clone();

        let cmds2 = n2.handle_proposal_message(author_3, proposal_message_3.clone());
        let cmds1 = n1.handle_proposal_message(author_3, proposal_message_3);

        // second_state has the malicious block in the blocktree, so it will not be able to
        // commit anything
        assert_eq!(n2.consensus_state.pending_block_tree.size(), 3);
        assert!(find_commit_cmd(&cmds2).is_none());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(n1.consensus_state.pending_block_tree.size(), 2);
        assert!(find_commit_cmd(&cmds1).is_some());

        let msg = BlockSyncResponseMessage::BlockFound(block_1);
        // a block sync request arrived, helping second state to recover
        let _ = n2.handle_block_sync(routing_target, msg);

        // in the next round, second_state should recover and able to commit
        let cp4 = env.next_proposal_empty();
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
        let Some(ConsensusCommand::RequestSync { peer, block_id: _ }) = res else {
            panic!("request sync is not found")
        };

        let mal_sync = BlockSyncResponseMessage::NotAvailable(block_2.get_id());
        // BlockSyncMessage on blocks that were not requested should be ignored.
        let cmds3 = n3.handle_block_sync(author_2, mal_sync);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 2);
        assert!(find_blocksync_request(&cmds3).is_none());

        let sync = BlockSyncResponseMessage::BlockFound(block_3);

        let cmds3 = n3.handle_block_sync(*peer, sync.clone());

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 3);
        let res = find_blocksync_request(&cmds3);
        let Some(ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        }) = res
        else {
            panic!("request sync is not found")
        };
        // repeated handling of the requested block should be ignored
        let cmds3 = n3.handle_block_sync(*peer, sync);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 3);
        assert!(find_blocksync_request(&cmds3).is_none());

        // arrival of proposal should also prevent block_sync_request from modifying the tree
        let cmds2 = n3.handle_proposal_message(author_2, proposal_message_2);
        assert_eq!(n3.consensus_state.pending_block_tree.size(), 4);
        assert!(find_blocksync_request(&cmds2).is_none());

        let sync = BlockSyncResponseMessage::BlockFound(block_2);
        // request sync which did not arrive in time should be ignored.
        let cmds3 = n3.handle_block_sync(*peer_2, sync);

        assert_eq!(n3.consensus_state.pending_block_tree.size(), 4);
        assert!(find_blocksync_request(&cmds3).is_none());
    }

    #[test]
    fn test_receive_empty_block() {
        let num_state = 4;
        let (mut env, mut ctx) =
            setup::<SignatureType, SignatureCollectionType, BlockPolicyType, _, StateRoot, _, _>(
                num_state,
                ValidatorSetFactory::default(),
                SimpleRoundRobin::default(),
                MockTxPool::default(),
                || StateRoot::new(SeqNum(1)),
            );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        let p1 = env.next_proposal_empty();
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);
        let vote = extract_vote_msgs(cmds);
        assert_eq!(vote.len(), 1);
        assert_eq!(node_state.metrics.consensus_events.rx_empty_block, 1);
    }

    /// Test the behaviour of consensus when execution is lagging. This is tested
    /// by handling a non-empty proposal which is missing the state root hash (lagging execution)
    #[test]
    fn test_lagging_execution() {
        let num_state = 4;
        let (mut env, mut ctx) =
            setup::<SignatureType, SignatureCollectionType, BlockPolicyType, _, StateRoot, _, _>(
                num_state,
                ValidatorSetFactory::default(),
                SimpleRoundRobin::default(),
                MockTxPool::default(),
                || StateRoot::new(SeqNum(1)),
            );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts::zero(),
        );

        let p1 = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = p1.destructure();
        let cmds: Vec<ConsensusCommand<NopSignature, MultiSig<NopSignature>>> =
            consensus_state.handle_proposal_message(author, verified_message, &mut node_state);

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
        assert_eq!(node_state.metrics.consensus_events.rx_execution_lagging, 1);
    }

    /// Test consensus behavior when a state root hash is missing. This is
    /// tested by only updating consensus with one state root, then handling a
    /// proposal with state root lower than that
    #[test]
    fn test_missing_state_root() {
        let num_state = 4;
        let (mut env, mut ctx) =
            setup::<SignatureType, SignatureCollectionType, BlockPolicyType, _, StateRoot, _, _>(
                num_state,
                ValidatorSetFactory::default(),
                SimpleRoundRobin::default(),
                MockTxPool::default(),
                || StateRoot::new(SeqNum(5)),
            );
        let (consensus_state, mut node_state) = ctx[0].get_state();

        // prepare 10 blocks
        for _ in 0..10 {
            let p = env.next_proposal(
                FullTransactionList::empty(),
                ExecutionArtifacts {
                    state_root: INITIAL_DELAY_STATE_ROOT_HASH,
                    ..ExecutionArtifacts::zero()
                },
            );
            let (author, _, p) = p.destructure();
            let _cmds = consensus_state.handle_proposal_message(author, p, &mut node_state);
        }

        assert_eq!(node_state.metrics.consensus_events.rx_empty_block, 10);
        assert_eq!(consensus_state.get_current_round(), Round(10));

        // only execution update for block 8 comes
        consensus_state.handle_state_root_update(SeqNum(8), StateRootHash(Hash([0x08_u8; 32])));

        // Block 11 carries the state root hash from executing block 6 the state
        // root hash is missing. The certificates are processed - consensus enters new round and commit blocks, but it doesn't vote
        let p = env.next_proposal(
            FullTransactionList::new(vec![0xaa].into()),
            ExecutionArtifacts {
                state_root: StateRootHash(Hash([0x06_u8; 32])),
                ..ExecutionArtifacts::zero()
            },
        );
        let (author, _, p) = p.destructure();
        let cmds = consensus_state.handle_proposal_message(author, p, &mut node_state);

        assert_eq!(consensus_state.get_current_round(), Round(11));
        assert_eq!(node_state.metrics.consensus_events.rx_missing_state_root, 1);
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
        let num_state = 4;
        // MissingNextStateRoot forces the proposer's state root hash
        // to be unavailable
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            MissingNextStateRoot,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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
            if node_id == n1.consensus_state.get_nodeid() {
                n1
            } else if node_id == n2.consensus_state.get_nodeid() {
                n2
            } else if node_id == n3.consensus_state.get_nodeid() {
                n3
            } else if node_id == n4.consensus_state.get_nodeid() {
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
                assert_eq!(p.block.payload.txns, FullTransactionList::empty());
                assert_eq!(
                    p.block.payload.header.state_root,
                    StateRootHash(Hash([0; 32]))
                );
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
        let (mut env, mut ctx) =
            setup::<SignatureType, SignatureCollectionType, BlockPolicyType, _, StateRoot, _, _>(
                num_state,
                ValidatorSetFactory::default(),
                SimpleRoundRobin::default(),
                MockTxPool::default(),
                || StateRoot::new(SeqNum(1)),
            );
        let node = &mut ctx[0];

        // delay gap in setup is 1

        let p0 = env.next_proposal_empty();
        let (author, _, verified_message) = p0.destructure();

        let _ = node.handle_proposal_message(author, verified_message);

        let p1 = env.next_proposal(
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
        // p1 has seq_num 1 and therefore requires state_root 0
        // the state_root 0's hash should be Hash([0x99; 32])
        node.consensus_state
            .handle_state_root_update(SeqNum(0), StateRootHash(Hash([0x99; 32])));

        let _ = node.handle_proposal_message(author, verified_message);

        // commit some blocks and confirm cleanup of state root hashes happened

        let p2 = env.next_proposal(
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
        // p2 should have seqnum 2 and therefore only require state_root 1
        node.consensus_state
            .handle_state_root_update(SeqNum(1), StateRootHash(Hash([0xbb; 32])));
        let p2_cmds = node.handle_proposal_message(author, verified_message);
        assert!(find_commit_cmd(&p2_cmds).is_some());

        let p3 = env.next_proposal(
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
        node.consensus_state
            .handle_state_root_update(SeqNum(2), StateRootHash(Hash([0xcc; 32])));
        let p3_cmds = node.handle_proposal_message(author, verified_message);
        assert!(find_commit_cmd(&p3_cmds).is_some());

        // Delay gap is 1 and we have received proposals with seq num 0, 1, 2, 3
        // state_root_validator had updates for 0, 1, 2
        //
        // Proposals with seq num 1 and 2 are committed, so expect 2 to remain
        // in the state_root_validator
        assert_eq!(
            2,
            node.consensus_state.state_root_validator.root_hashes.len()
        );
        assert!(node
            .consensus_state
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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
            ExecutionArtifacts::zero(),
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );

        for i in 0..8 {
            let cp = env.next_proposal_empty();
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
        let epoch = env.epoch_manager.get_epoch(Round(11));
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
            let cp = env.next_proposal_empty();
            let (author, _, verified_message) = cp.destructure();
            for (i, node) in ctx.iter_mut().enumerate() {
                if node.consensus_state.get_nodeid() != next_leader {
                    let cmds = node.handle_proposal_message(author, verified_message.clone());

                    if j == 1 {
                        proposal_10_blockid = verified_message.block.get_id();
                        let v = extract_vote_msgs(cmds);
                        assert_eq!(v.len(), 1);
                        votes.push((node.consensus_state.get_nodeid(), v[0]));
                    }
                } else {
                    leader_index = i;
                }
            }
        }
        let (leader_cs, mut leader_ns) = ctx[leader_index].get_state();
        for (i, (author, v)) in votes.iter().enumerate() {
            let cmds = leader_cs.handle_vote_message(*author, *v, &mut leader_ns);
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
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = env.next_proposal_empty();

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
        let cmds = node1
            .consensus_state
            .handle_timeout_expiry(&env.epoch_manager, &mut node1.metrics);
        let tmo: Vec<&Timeout<SignatureCollectionType>> = cmds
            .iter()
            .filter_map(|cmd| match cmd {
                PacemakerCommand::PrepareTimeout(tmo) => Some(tmo),
                _ => None,
            })
            .collect();

        assert_eq!(tmo.len(), 1);
        assert_eq!(tmo[0].tminfo.round, Round(4));
        let author = node1.consensus_state.get_nodeid();
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = env.next_proposal_empty();
            let (_, _, verified_message) = cp.destructure();
            blocks.push(verified_message.block);
        }
        let cp = env.next_proposal_empty();

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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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
        let epoch = env.epoch_manager.get_epoch(Round(missing_round + 1));
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
            if node.consensus_state.get_nodeid() != next_leader {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                let v = extract_vote_msgs(cmds);
                assert_eq!(v.len(), 1);
                votes.push((node.consensus_state.get_nodeid(), v[0]));
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_state as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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

        let epoch = env.epoch_manager.get_epoch(Round(4));
        let invalid_author = env.election.get_leader(
            Round(4),
            env.val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
        );
        assert!(invalid_author != NodeId::new(node.consensus_state.get_keypair().pubkey()));
        assert!(invalid_author != p2.block.author);
        let invalid_b2 = Block::new(
            invalid_author,
            epoch,
            p2.block.round,
            &p2.block.payload,
            &p2.block.qc,
        );
        let invalid_p2 = ProposalMessage {
            block: invalid_b2,
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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
                .get_epoch(node.consensus_state.get_current_round());
            assert!(current_epoch == Epoch(1));

            let expected_epoch_start_round =
                update_block_round + node.epoch_manager.epoch_start_delay;
            // verify that the start of next epoch is scheduled correctly
            assert!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1))
                    == Epoch(1)
            );
            assert!(node.epoch_manager.get_epoch(expected_epoch_start_round) == Epoch(2));
        }
    }

    #[test]
    fn test_advance_epoch_through_proposal_qc() {
        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let val_stakes: Vec<(NodeId<NopPubKey>, Stake)> = env
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
        env.val_epoch_map.insert(
            Epoch(2),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map.insert(
                Epoch(2),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
        }
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal_empty();

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
                .get_epoch(node.consensus_state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager.get_epoch(expected_epoch_start_round),
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
                .get_epoch(node.consensus_state.get_current_round());
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
            .get_epoch(node.consensus_state.get_current_round());
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_advance_epoch_through_proposal_tc() {
        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let val_stakes: Vec<(NodeId<NopPubKey>, Stake)> = env
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
        env.val_epoch_map.insert(
            Epoch(2),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map.insert(
                Epoch(2),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
        }
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal_empty();
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
                .get_epoch(node.consensus_state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager.get_epoch(expected_epoch_start_round),
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
                .get_epoch(node.consensus_state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));
        }

        // proposal for Round(expected_epoch_start_round)
        let cp = env.next_proposal_empty();
        let (author, _, verified_message) = cp.destructure();
        let node = &mut ctx[0];
        // observe TC to advance round and epoch
        let _ = node.handle_proposal_message(author, verified_message);
        let current_epoch = node
            .epoch_manager
            .get_epoch(node.consensus_state.get_current_round());
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_advance_epoch_through_local_tc() {
        let num_states = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let val_stakes: Vec<(NodeId<NopPubKey>, Stake)> = env
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
        env.val_epoch_map.insert(
            Epoch(2),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map.insert(
                Epoch(2),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
        }
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        // commit blocks until update_block
        for _ in 0..(update_block_round.0 + 2) {
            let cp = env.next_proposal_empty();
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
                .get_epoch(node.consensus_state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager.get_epoch(expected_epoch_start_round),
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
                .get_epoch(node.consensus_state.get_current_round());
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
            .get_epoch(node.consensus_state.get_current_round());
        assert_eq!(current_epoch, Epoch(2));
    }

    #[test]
    fn test_schedule_epoch_on_blocksync() {
        let num_states = 2;

        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
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
            let proposal = env.next_proposal_empty();
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
            let proposal = env.next_proposal_empty();
            let (author, _, verified_message) = proposal.destructure();

            let state = &mut ctx[0];
            let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> =
                state.handle_proposal_message(author, verified_message.clone());
            // state should not request blocksync
            assert!(extract_blocksync_requests(cmds).is_empty());

            block_sync_blocks.push(verified_message.block);
        }
        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        // state 0 should have committed update_block and scheduled next epoch
        // verify state 0 is still in epoch 1
        let state_0_epoch = ctx[0]
            .epoch_manager
            .get_epoch(ctx[0].consensus_state.get_current_round());
        assert_eq!(state_0_epoch, Epoch(1));

        // verify state 0 scheduled the next epoch correctly
        assert_eq!(
            ctx[0]
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            ctx[0].epoch_manager.get_epoch(expected_epoch_start_round),
            Epoch(2)
        );

        // state 1 should still not have scheduled next epoch since it didn't commit update_block
        let state_1_epoch = ctx[1]
            .epoch_manager
            .get_epoch(ctx[1].consensus_state.get_current_round());
        assert_eq!(state_1_epoch, Epoch(1));
        assert_eq!(
            ctx[1]
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            ctx[1].epoch_manager.get_epoch(expected_epoch_start_round),
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
        let nodeid_0 = ctx[0].consensus_state.get_nodeid();
        let state1 = &mut ctx[1];
        for block in block_sync_blocks.into_iter().rev() {
            let msg = BlockSyncResponseMessage::BlockFound(block);
            // blocksync response for state 2
            let _ = state1.handle_block_sync(nodeid_0, msg);
        }

        // blocks aren't committed immediately after blocksync is finished
        // state 1 should not have scheduled the next epoch yet
        let state_1_epoch = state1
            .epoch_manager
            .get_epoch(state1.consensus_state.get_current_round());
        assert_eq!(state_1_epoch, Epoch(1));
        assert_eq!(
            state1
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            state1.epoch_manager.get_epoch(expected_epoch_start_round),
            Epoch(1)
        ); // STILL EPOCH 1

        // generate proposal for update_block + 4
        let proposal = env.next_proposal_empty();

        let (author, _, verified_message) = proposal.destructure();
        let cmds: Vec<ConsensusCommand<SignatureType, MultiSig<SignatureType>>> =
            state1.handle_proposal_message(author, verified_message);
        // state 1 should not request blocksync
        assert!(extract_blocksync_requests(cmds).is_empty());

        // state 1 should have committed the update block and scheduled the next epoch
        let state_1_epoch = state1
            .epoch_manager
            .get_epoch(state1.consensus_state.get_current_round());
        assert_eq!(state_1_epoch, Epoch(1));
        assert_eq!(
            state1
                .epoch_manager
                .get_epoch(expected_epoch_start_round - Round(1)),
            Epoch(1)
        );
        assert_eq!(
            state1.epoch_manager.get_epoch(expected_epoch_start_round),
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
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let val_stakes: Vec<(NodeId<NopPubKey>, Stake)> = env
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
        env.val_epoch_map.insert(
            Epoch(2),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        for node in ctx.iter_mut() {
            node.val_epoch_map.insert(
                Epoch(2),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
        }
        let mut blocks = vec![];

        // Sequence number of the block which updates the validator set
        let update_block = env.epoch_manager.val_set_update_interval;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let update_block_round = seqnum_to_round_no_tc(update_block);

        let expected_epoch_start_round = update_block_round + env.epoch_manager.epoch_start_delay;

        // handle proposals until expected_epoch_start_round - 1
        for _ in 0..(expected_epoch_start_round.0 - 1) {
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
                .get_epoch(node.consensus_state.get_current_round());
            assert_eq!(current_epoch, Epoch(1));

            // verify that the start of next epoch is scheduled correctly
            assert_eq!(
                node.epoch_manager
                    .get_epoch(expected_epoch_start_round - Round(1)),
                Epoch(1)
            );
            assert_eq!(
                node.epoch_manager.get_epoch(expected_epoch_start_round),
                Epoch(2)
            );
        }

        env.epoch_manager
            .schedule_epoch_start(update_block, update_block_round);

        // skip block on round expected_epoch_start_round
        let _unused_proposal = env.next_proposal_empty();
        // generate proposal for expected_epoch_start_round + 1
        let cp = env.next_proposal_empty();

        let (author, _, verified_message) = cp.destructure();
        for node in ctx.iter_mut() {
            let cmds = node.handle_proposal_message(author, verified_message.clone());
            // state should have requested blocksync
            assert_eq!(extract_blocksync_requests(cmds).len(), 1);

            // verify state is now in epoch 2
            let current_epoch = node
                .epoch_manager
                .get_epoch(node.consensus_state.get_current_round());
            assert_eq!(current_epoch, Epoch(2));
        }
    }

    #[test]
    fn test_vote_sent_to_leader_in_next_epoch() {
        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );

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

    #[test]
    fn test_remove_old_blocksync_requests() {
        let num_states = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let node = &mut ctx[0];

        // round 1 proposal
        let p1 = env.next_proposal_empty();
        let (author_1, _, verified_message_1) = p1.destructure();
        let block_1_id = verified_message_1.block.get_id();

        // round 2 proposal
        let p2 = env.next_proposal_empty();
        let (author_2, _, verified_message_2) = p2.destructure();
        let _ = node.handle_proposal_message(author_2, verified_message_2);
        // should be in round 2 and request block 1
        assert!(node.consensus_state.get_current_round() == Round(2));
        assert!(node
            .consensus_state
            .block_sync_requester()
            .requests()
            .contains_key(&block_1_id));

        // handle proposal for block 1
        let _ = node.handle_proposal_message(author_1, verified_message_1);
        // should still be in round 2 and block 1 request should exist
        assert!(node.consensus_state.get_current_round() == Round(2));
        assert!(node
            .consensus_state
            .block_sync_requester()
            .requests()
            .contains_key(&block_1_id));

        // round 3 proposal
        let p3 = env.next_proposal_empty();
        let (author_3, _, verified_message_3) = p3.destructure();
        let _ = node.handle_proposal_message(author_3, verified_message_3);
        // should be in round 3 and remove block 1 request
        assert!(node.consensus_state.get_current_round() == Round(3));
        assert!(!node
            .consensus_state
            .block_sync_requester()
            .requests()
            .contains_key(&block_1_id));
        assert!(node
            .consensus_state
            .block_sync_requester()
            .requests()
            .is_empty());
    }

    #[test]
    fn test_request_over_state_sync_threshold() {
        let num_states = 4;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );
        let mut blocks = vec![];

        // Sequence number of the block after which state sync should be triggered
        let state_sync_threshold_block = ctx[0].consensus_state.config.state_sync_threshold;
        // Round number of that block is the same as its sequence number (NO TCs in between)
        let state_sync_threshold_round = Round(state_sync_threshold_block.0);

        let (n1, other_states) = ctx.split_first_mut().unwrap();

        // handle proposals for 3 states until state_sync_threshold
        for _ in 0..state_sync_threshold_round.0 {
            let cp = env.next_proposal_empty();
            let (author, _, verified_message) = cp.destructure();
            for node in other_states.iter_mut() {
                let cmds = node.handle_proposal_message(author, verified_message.clone());
                // state should not request blocksync
                assert!(extract_blocksync_requests(cmds).is_empty());
            }

            blocks.push(verified_message.block);
        }

        let cp = env.next_proposal_empty();
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
        //      validated in blocktree, and skip voting. Block 3 should still be unvalidated in the blocktree.
        // State 1 receives Block 4. It should validate Block 3 and Block 4, and send a vote message.

        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (n2, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal_empty();
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
        let cp = env.next_proposal_empty();
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block.get_id();

        // generate block 3
        let cp = env.next_proposal_empty();
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
            let msg = BlockSyncResponseMessage::BlockFound(proposal_message_2.block);
            // blocksync response for state 2
            let _ = n1.handle_block_sync(n2.consensus_state.nodeid, msg);
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
        //      Block 3 should still be unvalidated in the blocktree.
        // State 1 receives Block 5. It should validate Block 3, 4 and 5, and send a vote message.

        let num_states = 2;
        let (mut env, mut ctx) = setup::<
            SignatureType,
            SignatureCollectionType,
            BlockPolicyType,
            _,
            StateRootValidatorType,
            _,
            _,
        >(
            num_states as u32,
            ValidatorSetFactory::default(),
            SimpleRoundRobin::default(),
            MockTxPool::default(),
            || NopStateRoot,
        );

        let (n1, other_states) = ctx.split_first_mut().unwrap();
        let (n2, _) = other_states.split_first_mut().unwrap();

        // state receives block 1
        let cp = env.next_proposal_empty();
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
        let cp = env.next_proposal_empty();
        let (author_2, _, proposal_message_2) = cp.destructure();
        let block_2_id = proposal_message_2.block.get_id();

        // generate block 3
        let cp = env.next_proposal_empty();
        let (author_3, _, proposal_message_3) = cp.destructure();
        let block_3_id = proposal_message_3.block.get_id();

        // generate block 4
        let cp = env.next_proposal_empty();
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
            let msg = BlockSyncResponseMessage::BlockFound(proposal_message_3.block);
            // blocksync response for state 3
            let cmds = n1.handle_block_sync(n2.consensus_state.nodeid, msg);
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
            let msg = BlockSyncResponseMessage::BlockFound(proposal_message_2.block);
            // blocksync response for state 2
            let _ = n1.handle_block_sync(n2.consensus_state.nodeid, msg);
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

        // block 3 should still be in the blocktree as not validated
        let block_3_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_3_id)
            .expect("should be in the blocktree");
        assert!(block_3_blocktree_entry.is_coherent);

        // block 4 should still be in the blocktree as not validated
        let block_4_blocktree_entry = n1
            .consensus_state
            .pending_block_tree
            .get_entry(&block_4_id)
            .expect("should be in the blocktree");
        assert!(block_4_blocktree_entry.is_coherent);
    }
}
