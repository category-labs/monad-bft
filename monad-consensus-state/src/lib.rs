use std::time::Duration;

use monad_blocktree::blocktree::{BlockTree, RootKind};
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, ProposalMessage, TimeoutMessage, VoteMessage},
    },
    pacemaker::{Pacemaker, PacemakerCommand},
    validation::safety::Safety,
    vote_state::VoteState,
};
use monad_consensus_types::{
    block::{BlockType, FullBlock},
    command::{FetchFullTxParams, FetchTxParams},
    payload::{FullTransactionList, StateRootResult, StateRootValidator, TransactionHashList},
    quorum_certificate::{QuorumCertificate, Rank},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
    transaction_validator::TransactionValidator,
    voting::ValidatorMapping,
};
use monad_crypto::{
    hasher::{Hash, Hasher},
    secp256k1::{KeyPair, PubKey},
};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, NodeId, Round, RouterTarget, SeqNum};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetType};
use tracing::{debug, trace, warn};

use crate::{
    blocksync::{BlockSyncRequester, BlockSyncResult},
    command::ConsensusCommand,
};

pub mod blocksync;
pub mod command;
pub mod wrapper;

pub struct ConsensusState<SCT: SignatureCollection, TV, SVT> {
    pending_block_tree: BlockTree<SCT>,
    vote_state: VoteState<SCT>,
    high_qc: QuorumCertificate<SCT>,
    state_root_validator: SVT,

    pacemaker: Pacemaker<SCT>,
    safety: Safety,

    nodeid: NodeId,
    config: ConsensusConfig,

    transaction_validator: TV,
    block_sync_requester: BlockSyncRequester<SCT>,

    // TODO-2 deprecate
    keypair: KeyPair,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
    beneficiary: EthAddress,
}

impl<SCT, TVT, SVT> PartialEq for ConsensusState<SCT, TVT, SVT>
where
    SCT: SignatureCollection,
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
impl<SCT, TVT, SVT> std::fmt::Debug for ConsensusState<SCT, TVT, SVT>
where
    SCT: SignatureCollection,
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

impl<SCT, TVT, SVT> Eq for ConsensusState<SCT, TVT, SVT> where SCT: SignatureCollection {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConsensusConfig {
    pub proposal_size: usize,
    pub state_root_delay: SeqNum,
    pub propose_with_missing_blocks: bool,
}

pub trait ConsensusProcess<SCT>
where
    SCT: SignatureCollection,
{
    type TransactionValidatorType;

    fn new(
        transaction_validator: Self::TransactionValidatorType,
        my_pubkey: PubKey,
        genesis_block: FullBlock<SCT>,
        genesis_qc: QuorumCertificate<SCT>,
        delta: Duration,
        config: ConsensusConfig,
        keypair: KeyPair,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
        beneficiary: EthAddress,
    ) -> Self;

    fn get_pubkey(&self) -> PubKey;

    fn get_cert_keypair(&self) -> &SignatureCollectionKeyPairType<SCT>;

    fn get_nodeid(&self) -> NodeId;

    fn get_beneficiary(&self) -> EthAddress;

    fn blocktree(&self) -> &BlockTree<SCT>;

    fn handle_timeout_expiry<H: Hasher>(&mut self) -> Vec<PacemakerCommand<SCT>>;

    fn handle_proposal_message<H: Hasher>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<SCT>,
    ) -> Vec<ConsensusCommand<SCT>>;

    fn handle_proposal_message_full<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<SCT>,
        txns: FullTransactionList,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<SCT>>;

    fn handle_vote_message<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        v: VoteMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<SCT>>;

    fn handle_timeout_message<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        tm: TimeoutMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<SCT>>;

    fn handle_block_sync<VT: ValidatorSetType>(
        &mut self,
        author: NodeId,
        msg: BlockSyncResponseMessage<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<SCT>>;

    fn handle_block_sync_tmo<VT: ValidatorSetType>(
        &mut self,
        bid: BlockId,
        validators: &VT,
    ) -> Vec<ConsensusCommand<SCT>>;

    fn handle_state_root_update(&mut self, seq_num: SeqNum, root_hash: Hash);

    fn get_current_round(&self) -> Round;

    fn get_keypair(&self) -> &KeyPair;

    fn fetch_uncommitted_block(&self, bid: &BlockId) -> Option<&FullBlock<SCT>>;
}

impl<SCT, TVT, SVT> ConsensusProcess<SCT> for ConsensusState<SCT, TVT, SVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
    SVT: StateRootValidator,
{
    type TransactionValidatorType = TVT;

    fn new(
        transaction_validator: Self::TransactionValidatorType,
        my_pubkey: PubKey,
        genesis_block: FullBlock<SCT>,
        genesis_qc: QuorumCertificate<SCT>,
        delta: Duration,
        config: ConsensusConfig,

        // TODO-2 deprecate
        keypair: KeyPair,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
        beneficiary: EthAddress,
    ) -> Self {
        ConsensusState {
            pending_block_tree: BlockTree::new(genesis_block),
            vote_state: VoteState::default(),
            high_qc: genesis_qc,
            state_root_validator: SVT::new(config.state_root_delay),
            pacemaker: Pacemaker::new(delta, Round(1), None),
            safety: Safety::default(),
            nodeid: NodeId(my_pubkey),
            config,

            transaction_validator,
            // timeout has to be proportional to delta, too slow/fast is bad
            // assuming 2 * delta is the duration which it takes for perfect message transmission
            // 3 * delta is a reasonable amount for timeout, (4 * delta is good too)
            // as 1 * delta for original ask, 2 * delta for reaction from peer
            block_sync_requester: BlockSyncRequester::new(NodeId(my_pubkey), delta * 3),
            keypair,
            cert_keypair,
            beneficiary,
        }
    }

    fn handle_timeout_expiry<H: Hasher>(&mut self) -> Vec<PacemakerCommand<SCT>> {
        inc_count!(local_timeout);
        debug!(
            "local timeout: round={:?}",
            self.pacemaker.get_current_round()
        );
        self.pacemaker
            .handle_event(&mut self.safety, &self.high_qc)
            .into_iter()
            .map(|cmd| match cmd {
                PacemakerCommand::PrepareTimeout(tmo) => {
                    let tmo_msg = TimeoutMessage::new::<H>(tmo, &self.cert_keypair);
                    PacemakerCommand::Broadcast(tmo_msg)
                }
                _ => cmd,
            })
            .collect()
    }

    /// Proposals can include NULL blocks which are blocks containing 0 transactions,
    /// an empty list.
    /// NULL block proposals are not required to validate the state_root field of the
    /// proposal's payload
    fn handle_proposal_message<H: Hasher>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<SCT>,
    ) -> Vec<ConsensusCommand<SCT>> {
        // NULL blocks are not required to have state root hashes
        if p.block.payload.txns == TransactionHashList::new(vec![EMPTY_RLP_TX_LIST]) {
            debug!("Received empty block: block={:?}", p.block);
            inc_count!(rx_empty_block);
            return vec![ConsensusCommand::FetchFullTxs(
                TransactionHashList::default(),
                FetchFullTxParams {
                    author,
                    p_block: p.block,
                    p_last_round_tc: p.last_round_tc,
                },
            )];
        }

        match self
            .state_root_validator
            .validate(p.block.payload.seq_num, p.block.payload.header.state_root)
        {
            // TODO-1 execution lagging too far behind should be a trigger for something
            // to try and catch up faster. For now, just wait
            StateRootResult::OutOfRange => {
                inc_count!(rx_execution_lagging);
                vec![]
            }
            // Don't vote and locally timeout if the proposed state root does not match
            // or if state root is missing
            StateRootResult::Missing | StateRootResult::Mismatch => {
                inc_count!(rx_bad_state_root);
                vec![]
            }
            StateRootResult::Success => {
                debug!(
                    "Received Proposal Message, fetching txns: txns={:?}",
                    p.block.payload.txns
                );
                inc_count!(rx_proposal);
                vec![ConsensusCommand::FetchFullTxs(
                    p.block.payload.txns.clone(),
                    FetchFullTxParams {
                        author,
                        p_block: p.block,
                        p_last_round_tc: p.last_round_tc,
                    },
                )]
            }
        }
    }

    fn handle_proposal_message_full<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<SCT>,
        full_txs: FullTransactionList,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<SCT>> {
        debug!("Fetched full proposal: {:?}", p);
        inc_count!(fetched_proposal);
        let mut cmds = vec![];

        let process_certificate_cmds = self.process_certificate_qc(&p.block.qc, validators);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            debug!("Handled proposal with TC: {:?}", last_round_tc);
            inc_count!(proposal_with_tc);
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let round = self.pacemaker.get_current_round();
        let leader = election.get_leader(round, validators.get_list());

        let author_pubkey = validator_mapping
            .map
            .get(&author)
            .expect("proposal author exists in validator_mapping");

        if let Err(e) = p
            .block
            .payload
            .randao_reveal
            .verify::<SCT::SignatureType>(p.block.get_round(), author_pubkey)
        {
            warn!("Invalid randao_reveal signature, reason: {:?}", e);
            inc_count!(failed_verify_randao_reveal_signature);
            return cmds;
        };

        let Some(full_block) =
            FullBlock::from_block(p.block, full_txs, &self.transaction_validator)
        else {
            warn!("Transaction validation failed");
            inc_count!(failed_txn_validation);
            return cmds;
        };

        self.pending_block_tree
            .add(full_block.clone())
            .expect("Failed to add block to blocktree");

        if full_block.get_round() != round || author != leader || full_block.get_author() != leader
        {
            debug!(
                "Invalid proposal: expected-round={:?} \
                round={:?} \
                expected-leader={:?} \
                author={:?} \
                block-author={:?}",
                round,
                full_block.get_round(),
                leader,
                author,
                full_block.get_author()
            );
            inc_count!(invalid_proposal_round_leader);
            return cmds;
        }

        let vote = self
            .safety
            .make_vote::<SCT, H>(full_block.get_block(), &p.last_round_tc);

        if let Some(v) = vote {
            let vote_msg = VoteMessage::<SCT>::new::<H>(v, &self.cert_keypair);

            let next_leader = election.get_leader(round + Round(1), validators.get_list());
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(NodeId(next_leader.0)),
                message: ConsensusMessage::Vote(vote_msg),
            };
            debug!("Created Vote: vote={:?} next_leader={:?}", v, next_leader);
            inc_count!(created_vote);
            cmds.push(send_cmd);
        }

        cmds
    }

    fn handle_vote_message<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        vote_msg: VoteMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<SCT>> {
        debug!("Vote Message: {:?}", vote_msg);
        if vote_msg.vote.vote_info.round < self.pacemaker.get_current_round() {
            inc_count!(old_vote_received);
            return Default::default();
        }
        inc_count!(vote_received);

        let mut cmds = Vec::new();
        let (qc, vote_state_cmds) =
            self.vote_state
                .process_vote::<H, _>(&author, &vote_msg, validators, validator_mapping);
        cmds.extend(vote_state_cmds.into_iter().map(Into::into));

        if let Some(qc) = qc {
            debug!("Created QC {:?}", qc);
            inc_count!(created_qc);
            cmds.extend(self.process_certificate_qc(&qc, validators));

            if self.nodeid
                == election.get_leader(self.pacemaker.get_current_round(), validators.get_list())
            {
                cmds.extend(self.process_new_round_event(None));
            }
        }
        cmds
    }

    fn handle_timeout_message<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        tmo_msg: TimeoutMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<SCT>> {
        let tm = &tmo_msg.timeout;
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            inc_count!(old_remote_timeout);
            return cmds;
        }

        debug!("Remote timeout msg: {:?}", tm);
        inc_count!(remote_timeout_msg);

        let process_certificate_cmds = self.process_certificate_qc(&tm.tminfo.high_qc, validators);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            inc_count!(remote_timeout_msg_with_tc);
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let (tc, remote_timeout_cmds) = self.pacemaker.process_remote_timeout::<H, VT>(
            validators,
            validator_mapping,
            &mut self.safety,
            &self.high_qc,
            author,
            tmo_msg,
        );

        // map PrepareTimeout to TimeoutMesasge
        let remote_timeout_cmds = remote_timeout_cmds.into_iter().map(|cmd| match cmd {
            PacemakerCommand::PrepareTimeout(tmo) => {
                let tmo_msg = TimeoutMessage::new::<H>(tmo, &self.cert_keypair);
                PacemakerCommand::Broadcast(tmo_msg)
            }
            _ => cmd,
        });
        cmds.extend(remote_timeout_cmds.map(Into::into));
        if let Some(tc) = tc {
            debug!("Created TC: {:?}", tc);
            inc_count!(created_tc);
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(&tc)
                .into_iter()
                .map(Into::into);
            cmds.extend(advance_round_cmds);

            if self.nodeid
                == election.get_leader(self.pacemaker.get_current_round(), validators.get_list())
            {
                cmds.extend(self.process_new_round_event(Some(tc)));
            }
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
    fn handle_block_sync<VT: ValidatorSetType>(
        &mut self,
        author: NodeId,
        msg: BlockSyncResponseMessage<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<SCT>> {
        let mut cmds = vec![];

        let bid = msg.get_block_id();
        let block_sync_result = self.block_sync_requester.handle_response(
            &author,
            msg,
            validators,
            &self.transaction_validator,
        );
        block_sync_result.log(bid);

        match block_sync_result {
            BlockSyncResult::Success(full_block) => {
                if self.pending_block_tree.is_valid_to_insert(&full_block) {
                    cmds.extend(
                        self.request_block_if_missing_ancestor(
                            &full_block.get_block().qc,
                            validators,
                        ),
                    );
                    self.pending_block_tree
                        .add(full_block)
                        .expect("failed to add block to tree during block sync");
                }
            }
            BlockSyncResult::Failed(retry_cmd) => cmds.extend(retry_cmd),
            BlockSyncResult::UnexpectedResponse => {
                debug!("Block sync unexpected response: author={:?}", author);
            }
        }
        cmds
    }

    fn fetch_uncommitted_block(&self, bid: &BlockId) -> Option<&FullBlock<SCT>> {
        self.pending_block_tree
            .tree()
            .get(bid)
            .map(|btree_block| btree_block.get_block())
    }

    fn handle_block_sync_tmo<VT: ValidatorSetType>(
        &mut self,
        bid: BlockId,
        validators: &VT,
    ) -> Vec<ConsensusCommand<SCT>> {
        self.block_sync_requester.handle_timeout(bid, validators)
    }

    fn handle_state_root_update(&mut self, seq_num: SeqNum, root_hash: Hash) {
        self.state_root_validator.add_state_root(seq_num, root_hash)
    }

    fn get_pubkey(&self) -> PubKey {
        self.nodeid.0
    }

    fn get_nodeid(&self) -> NodeId {
        self.nodeid
    }

    fn get_beneficiary(&self) -> EthAddress {
        self.beneficiary
    }

    fn blocktree(&self) -> &BlockTree<SCT> {
        &self.pending_block_tree
    }

    fn get_current_round(&self) -> Round {
        self.pacemaker.get_current_round()
    }

    fn get_keypair(&self) -> &KeyPair {
        &self.keypair
    }

    fn get_cert_keypair(&self) -> &SignatureCollectionKeyPairType<SCT> {
        &self.cert_keypair
    }
}

impl<SCT, TVT, SVT> ConsensusState<SCT, TVT, SVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
    SVT: StateRootValidator,
{
    // If the qc has a commit_state_hash, commit the parent block and prune the
    // block tree
    // Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    pub fn process_qc(&mut self, qc: &QuorumCertificate<SCT>) -> Vec<ConsensusCommand<SCT>> {
        if Rank(qc.info) <= Rank(self.high_qc.info) {
            inc_count!(process_old_qc);
            return Vec::new();
        }
        inc_count!(process_qc);

        self.high_qc = qc.clone();
        let mut cmds = Vec::new();
        if qc.info.vote.ledger_commit_info.commit_state_hash.is_some()
            && self
                .pending_block_tree
                .path_to_root(&qc.info.vote.vote_info.parent_id)
        {
            let blocks_to_commit = self
                .pending_block_tree
                .prune(&qc.info.vote.vote_info.parent_id)
                .unwrap_or_else(|_| panic!("\n{:?}", self.pending_block_tree));

            if let Some(b) = blocks_to_commit.last() {
                self.state_root_validator.remove_old_roots(b.get_seq_num());
            }

            debug!(
                "QC triggered commit: num_commits={:?}",
                blocks_to_commit.len()
            );

            if !blocks_to_commit.is_empty() {
                cmds.extend(
                    blocks_to_commit
                        .iter()
                        .map(|b| ConsensusCommand::StateRootHash(b.clone())),
                );
                cmds.push(ConsensusCommand::DrainTxs(
                    blocks_to_commit
                        .iter()
                        .map(|b| b.get_block().payload.txns.clone())
                        .collect(),
                ));
                cmds.push(ConsensusCommand::<SCT>::LedgerCommit(blocks_to_commit));
            }
        }
        cmds
    }

    #[must_use]
    fn process_certificate_qc<VT: ValidatorSetType>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<SCT>> {
        let mut cmds = Vec::new();
        cmds.extend(self.process_qc(qc));

        cmds.extend(self.pacemaker.advance_round_qc(qc).map(Into::into));

        cmds.extend(self.request_block_if_missing_ancestor(qc, validators));
        cmds
    }

    #[must_use]
    fn process_new_round_event(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
    ) -> Vec<ConsensusCommand<SCT>> {
        self.vote_state
            .start_new_round(self.pacemaker.get_current_round());

        let node_id = self.nodeid;
        let round = self.pacemaker.get_current_round();
        let high_qc = self.high_qc.clone();

        let parent_bid = high_qc.info.vote.vote_info.id;
        let seq_num_qc = high_qc.info.vote.vote_info.seq_num;
        let proposed_seq_num = seq_num_qc + SeqNum(1);
        match self.proposal_policy(&parent_bid, proposed_seq_num) {
            ConsensusAction::Propose(h, pending_blocktree_txs) => {
                inc_count!(creating_proposal);
                debug!("Creating Proposal: node_id={:?} round={:?} high_qc={:?}, seq_num={:?}, last_round_tc={:?}", 
                                node_id, round, high_qc, proposed_seq_num, last_round_tc);
                vec![ConsensusCommand::FetchTxs(
                    self.config.proposal_size,
                    pending_blocktree_txs,
                    FetchTxParams {
                        node_id,
                        round,
                        seq_num: proposed_seq_num,
                        state_root_hash: h,
                        high_qc,
                        last_round_tc,
                    },
                )]
            }
            ConsensusAction::Abstain => {
                inc_count!(abstain_proposal);
                // TODO-2: This could potentially be an empty block
                vec![]
            }
            ConsensusAction::ProposeEmpty => {
                // Don't have the necessary state root hash ready so propose
                // a NULL block
                inc_count!(creating_empty_block_proposal);
                debug!("Creating Empty Proposal: node_id={:?} round={:?} high_qc={:?}, seq_num={:?}, last_round_tc={:?}", 
                                node_id, round, high_qc, proposed_seq_num, last_round_tc);
                vec![ConsensusCommand::FetchTxs(
                    0,
                    vec![],
                    FetchTxParams {
                        node_id,
                        round,
                        seq_num: proposed_seq_num,
                        state_root_hash: Hash([0; 32]),
                        high_qc,
                        last_round_tc,
                    },
                )]
            }
        }
    }

    #[must_use]
    fn proposal_policy(&self, parent_bid: &BlockId, proposed_seq_num: SeqNum) -> ConsensusAction {
        // Never propose while syncing
        if let RootKind::Unrooted(_) = self.pending_block_tree.root {
            return ConsensusAction::Abstain;
        }

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

    fn request_block_if_missing_ancestor<VT: ValidatorSetType>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<SCT>> {
        if let Some(qc) = self.pending_block_tree.get_missing_ancestor(qc) {
            self.block_sync_requester.request::<VT>(&qc, validators)
        } else {
            vec![]
        }
    }
}

pub enum ConsensusAction {
    Propose(Hash, Vec<TransactionHashList>),
    ProposeEmpty,
    Abstain,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use itertools::Itertools;
    use monad_consensus::{
        messages::message::{BlockSyncResponseMessage, TimeoutMessage, VoteMessage},
        pacemaker::PacemakerCommand,
        validation::signing::Verified,
    };
    use monad_consensus_types::{
        block::{BlockType, UnverifiedFullBlock},
        certificate_signature::CertificateKeyPair,
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        payload::{
            Bloom, ExecutionArtifacts, FullTransactionList, Gas, NopStateRoot, StateRoot,
            StateRootValidator, TransactionHashList,
        },
        quorum_certificate::{genesis_vote_info, QuorumCertificate},
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        timeout::Timeout,
        transaction_validator::{MockValidator, TransactionValidator},
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        hasher::{Hash, HasherType},
        secp256k1::KeyPair,
        NopSignature,
    };
    use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
    use monad_testutil::{
        proposal::ProposalGen,
        signing::{create_certificate_keys, create_keys, get_genesis_config, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, NodeId, Round, RouterTarget, SeqNum};
    use monad_validator::{
        leader_election::LeaderElection,
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSet, ValidatorSetType},
    };
    use test_case::test_case;
    use tracing_test::traced_test;

    use crate::{
        ConsensusCommand, ConsensusConfig, ConsensusMessage, ConsensusProcess, ConsensusState,
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;
    type StateRootValidatorType = NopStateRoot;
    type TransactionValidatorType = MockValidator;

    fn setup<SCT: SignatureCollection, SVT: StateRootValidator, TVT: TransactionValidator>(
        num_states: u32,
    ) -> (
        Vec<KeyPair>,
        Vec<SignatureCollectionKeyPairType<SCT>>,
        ValidatorSet,
        ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        Vec<ConsensusState<SCT, MockValidator, SVT>>,
    ) {
        let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<SCT>(num_states);

        let voting_keys = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(cert_keys.iter())
            .collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<HasherType, SCT, TVT>(
            voting_keys.iter(),
            &valmap,
            &TVT::default(),
        );

        let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
            genesis_vote_info(genesis_block.get_id()),
            genesis_sigs,
        );

        let mut dupkeys = create_keys(num_states);
        let mut dupcertkeys = create_certificate_keys::<SCT>(num_states);
        let consensus_states = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let default_key = KeyPair::from_bytes([127; 32]).unwrap();
                let default_cert_key =
                    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(
                        [127; 32],
                    )
                    .unwrap();
                ConsensusState::<SCT, _, SVT>::new(
                    MockValidator,
                    k.pubkey(),
                    genesis_block.clone(),
                    genesis_qc.clone(),
                    Duration::from_secs(1),
                    ConsensusConfig {
                        proposal_size: 5000,
                        state_root_delay: SeqNum(1),
                        propose_with_missing_blocks: false,
                    },
                    std::mem::replace(&mut dupkeys[i], default_key),
                    std::mem::replace(&mut dupcertkeys[i], default_cert_key),
                    EthAddress::default(),
                )
            })
            .collect::<Vec<_>>();

        (keys, cert_keys, valset, valmap, consensus_states)
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[traced_test]
    #[test]
    fn lock_qc_high() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();

        let state = &mut states[0];
        assert_eq!(state.high_qc.info.vote.vote_info.round, Round(0));

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
            ledger_commit_info: LedgerCommitInfo::new::<HasherType>(None, &vi),
        };

        let vm1 = VoteMessage::<SignatureCollectionType>::new::<HasherType>(v, &certkeys[1]);
        let vm2 = VoteMessage::<SignatureCollectionType>::new::<HasherType>(v, &certkeys[2]);
        let vm3 = VoteMessage::<SignatureCollectionType>::new::<HasherType>(v, &certkeys[3]);

        let v1 = Verified::<SignatureType, _>::new::<HasherType>(vm1, &keys[1]);
        let v2 = Verified::<SignatureType, _>::new::<HasherType>(vm2, &keys[2]);
        let v3 = Verified::<SignatureType, _>::new::<HasherType>(vm3, &keys[3]);

        state.handle_vote_message::<HasherType, _, _>(
            *v1.author(),
            *v1,
            &valset,
            &valmap,
            &election,
        );
        state.handle_vote_message::<HasherType, _, _>(
            *v2.author(),
            *v2,
            &valset,
            &valmap,
            &election,
        );

        // less than 2f+1, so expect not locked
        assert_eq!(state.high_qc.info.vote.vote_info.round, Round(0));

        state.handle_vote_message::<HasherType, _, _>(
            *v3.author(),
            *v3,
            &valset,
            &valmap,
            &election,
        );
        assert_eq!(
            state.high_qc.info.vote.vote_info.round,
            expected_qc_high_round
        );

        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("monotonic_counter.vote_received"))
                .count()
            {
                3 => Ok(()),
                n => Err(format!("Expected count 3 got {}", n)),
            }
        });
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );

        // local timeout for state in Round 1
        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        let _ = state
            .pacemaker
            .handle_event(&mut state.safety, &state.high_qc);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert!(result.is_none());
    }

    #[test]
    fn enter_proposalmsg_round() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen =
            ProposalGen::<SignatureType, SignatureCollectionType>::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert!(result.is_some());

        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert!(result.is_some());

        for _ in 0..4 {
            propgen.next_proposal(
                &keys,
                &certkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            );
        }
        let p7 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p7.destructure();
        state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        assert_eq!(state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn duplicate_proposals() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen =
            ProposalGen::<SignatureType, SignatureCollectionType>::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });
        assert!(result.is_some());

        // send duplicate of p1, expect it to be ignored and no output commands
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_out_of_order_proposals() {
        let perms = (0..4).permutations(4).collect::<Vec<_>>();

        for perm in perms {
            out_of_order_proposals(perm);
        }
    }

    fn out_of_order_proposals(perms: Vec<usize>) {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());

        // first proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert!(result.is_some());

        // second proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let result = cmds.iter().find(|&c| {
            matches!(
                c,
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(_),
                }
            )
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert!(result.is_some());

        let mut missing_proposals = Vec::new();
        for _ in 0..perms.len() {
            missing_proposals.push(propgen.next_proposal(
                &keys,
                &certkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            ));
        }

        // last proposal arrvies
        let p_fut = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p_fut.destructure();
        state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        // confirm the size of the pending_block_tree (genesis, p1, p2, p_fut)
        assert_eq!(state.pending_block_tree.size(), 4);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(state.handle_proposal_message_full::<HasherType, _, _>(
                author,
                verified_message,
                FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
                &valset,
                &valmap,
                &election,
            ));
        }

        let _self_id = NodeId(state.nodeid.0);
        let mut more_proposals = true;

        while more_proposals {
            cmds = cmds
                .into_iter()
                .filter(|m| {
                    matches!(
                        m,
                        ConsensusCommand::Publish {
                            target: RouterTarget::PointToPoint(_),
                            message: ConsensusMessage::Proposal(_),
                        }
                    )
                })
                .collect::<Vec<_>>();

            let mut proposals = Vec::new();
            let c = if cmds.is_empty() {
                break;
            } else {
                cmds.remove(0)
            };

            match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_self_id),
                    message: ConsensusMessage::Proposal(m),
                } => {
                    proposals.extend(state.handle_proposal_message_full::<HasherType, _, _>(
                        m.block.author,
                        m.clone(),
                        FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
                        &valset,
                        &valmap,
                        &election,
                    ));
                }
                _ => more_proposals = false,
            }
            cmds.extend(proposals);
        }

        // next proposal will trigger everything to be committed if there is
        // a consecutive chain as expected
        // last proposal arrvies
        let p_last = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p_last.destructure();
        state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        assert_eq!(state.pending_block_tree.size(), 3);
        assert_eq!(
            state.pacemaker.get_current_round(),
            Round(perms.len() as u64 + 4),
            "order of proposals {:?}",
            perms
        );
    }

    #[test]
    fn test_commit_rule_consecutive() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());

        // round 1 proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        let p1_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        let p1_votes = p1_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p1_votes.len() == 1);
        assert!(p1_votes[0]
            .vote
            .ledger_commit_info
            .commit_state_hash
            .is_some());

        // round 2 proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_none());

        let p2_votes = p2_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p2_votes.len() == 1);
        // csh is some: the proposal and qc have consecutive rounds
        assert!(p2_votes[0]
            .vote
            .ledger_commit_info
            .commit_state_hash
            .is_some());

        let p3 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p3.destructure();

        let p2_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());
    }

    #[test]
    fn test_commit_rule_non_consecutive() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());

        // round 1 proposal
        let p1 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p1.destructure();
        state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        // round 2 proposal
        let p2 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        let p2_votes = p2_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p2_votes.len() == 1);
        assert!(p2_votes[0]
            .vote
            .ledger_commit_info
            .commit_state_hash
            .is_some());

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

        let _ = propgen.next_tc(&keys, &certkeys, &valset, &valmap);

        // round 3 proposal, has qc(1)
        let p3 = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        assert_eq!(p3.block.qc.info.vote.vote_info.round, Round(1));
        assert_eq!(p3.block.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let p3_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        let p3_votes = p3_cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(p3_votes.len() == 1);
        // proposal and qc have non-consecutive rounds
        assert!(p3_votes[0]
            .vote
            .ledger_commit_info
            .commit_state_hash
            .is_none());
    }

    // this test checks that a malicious proposal sent only to the next leader is
    // not incorrectly committed
    #[test]
    fn test_malicious_proposal_and_block_recovery() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let (first_state, xs) = states.split_first_mut().unwrap();
        let (second_state, xs) = xs.split_first_mut().unwrap();
        let (third_state, xs) = xs.split_first_mut().unwrap();
        let fourth_state = &mut xs[0];

        // first_state will send 2 different proposals, A and B. A will be sent to
        // the next leader, all other nodes get B.
        // effect is that nodes send votes for B to the next leader who thinks that
        // the "correct" proposal is A.
        let mut correct_proposal_gen =
            ProposalGen::<SignatureType, _>::new(first_state.high_qc.clone());
        let mut mal_proposal_gen =
            ProposalGen::<SignatureType, _>::new(first_state.high_qc.clone());

        let cp1 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::default(),
            ExecutionArtifacts::zero(),
        );
        let mp1 = mal_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![5]),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp1.destructure();
        let block_1 = UnverifiedFullBlock {
            block: verified_message.block.clone(),
            full_txs: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
        };
        let cmds1 = first_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let p1_votes = cmds1
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        let cmds3 = third_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let p3_votes = cmds3
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        let cmds4 = fourth_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let p4_votes = cmds4
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        let (author, _, verified_message) = mp1.destructure();
        let cmds2 = second_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let p2_votes = cmds2
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::Publish {
                    target: RouterTarget::PointToPoint(_),
                    message: ConsensusMessage::Vote(vote),
                } => Some(vote),
                _ => None,
            })
            .collect::<Vec<_>>()[0];

        assert_eq!(p1_votes.vote, p3_votes.vote);
        assert_eq!(p1_votes.vote, p4_votes.vote);
        assert_ne!(p1_votes.vote, p2_votes.vote);
        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];
        // temp sub with a key that doesn't exists.
        let mut routing_target = NodeId(get_key(100).pubkey());
        // We Collected 4 votes, 3 of which are valid, 1 of which is not caused by byzantine leader.
        // First 3 (including a false vote) submitted would not cause a qc to form
        // but the last vote would cause a qc to form locally at second_state, thus causing
        // second state to realize its missing a block.
        for i in 0..4 {
            let v = Verified::<NopSignature, VoteMessage<_>>::new::<HasherType>(votes[i], &keys[i]);
            let cmds2 = second_state.handle_vote_message::<HasherType, _, _>(
                *v.author(),
                *v,
                &valset,
                &valmap,
                &election,
            );
            let res = cmds2.into_iter().find(|c| {
                matches!(
                    c,
                    ConsensusCommand::RequestSync {
                        peer: NodeId(_),
                        block_id: BlockId(_)
                    },
                )
            });
            if i < 3 {
                assert!(res.is_none());
            } else {
                assert!(res.is_some());
                let (target, sync_bid) = match res.unwrap() {
                    ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                    _ => None,
                }
                .unwrap();
                assert!(sync_bid == block_1.block.get_id());
                routing_target = target;
            }
        }
        assert_ne!(routing_target, NodeId(get_key(100).pubkey()));
        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(second_state.high_qc.info.vote, votes[0].vote);

        // use the correct proposal gen to make next proposal and send it to second_state
        // this should cause it to emit the RequestBlockSync Message because the the QC parent
        // points to a different proposal (second_state has the malicious proposal in its blocktree)
        let cp2 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author_2, _, verified_message_2) = cp2.destructure();
        let block_2 = UnverifiedFullBlock {
            block: verified_message_2.block.clone(),
            full_txs: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
        };
        second_state.handle_proposal_message_full::<HasherType, _, _>(
            author_2,
            verified_message_2.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = first_state.handle_proposal_message_full::<HasherType, _, _>(
            author_2,
            verified_message_2.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let res = cmds1.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        assert!(res.is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = cp3.destructure();
        let block_3 = UnverifiedFullBlock {
            block: verified_message.block.clone(),
            full_txs: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
        };

        let cmds2 = second_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let cmds1 = first_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        // second_state has the malicious block in the blocktree, so it will not be able to
        // commit anything
        assert_eq!(second_state.pending_block_tree.size(), 4);
        let res = cmds2
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_none());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(first_state.pending_block_tree.size(), 3);
        let res = cmds1
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());

        let msg = BlockSyncResponseMessage::BlockFound(block_1);
        // a block sync request arrived, helping second state to recover
        second_state.handle_block_sync(routing_target, msg, &valset);

        // in the next round, second_state should recover and able to commit
        let cp4 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = cp4.destructure();
        let cmds2 = second_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        // new block added should allow path_to_root properly, thus no more request sync
        let res = cmds2.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        assert!(res.is_none());

        let cmds1 = first_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        // second_state has the correct blocks, so expect to see a commit
        assert_eq!(second_state.pending_block_tree.size(), 3);
        let res = cmds2
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());

        // first_state has the correct blocks, so expect to see a commit
        assert_eq!(first_state.pending_block_tree.size(), 3);
        let res = cmds1
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(res.is_some());

        // third_state only received proposal for round 1, and is missing proposal for round 2, 3, 4
        // feeding third_state with a proposal from round 4 should trigger a recursive behaviour to ask for blocks

        let cmds3 = third_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        assert_eq!(third_state.pending_block_tree.size(), 3);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        let Some(ConsensusCommand::RequestSync { peer, block_id }) = res else {
            panic!("request sync is not found")
        };

        let mal_sync = BlockSyncResponseMessage::NotAvailable(block_2.block.get_id());
        // BlockSyncMessage on blocks that were not requested should be ignored.
        let cmds3 = third_state.handle_block_sync(author_2, mal_sync, &valset);

        assert_eq!(third_state.pending_block_tree.size(), 3);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        assert!(res.is_none());

        let sync = BlockSyncResponseMessage::BlockFound(block_3);

        let cmds3 = third_state.handle_block_sync(*peer, sync.clone(), &valset);

        assert_eq!(third_state.pending_block_tree.size(), 4);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        let Some(ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        }) = res
        else {
            panic!("request sync is not found")
        };
        // repeated handling of the requested block should be ignored
        let cmds3 = third_state.handle_block_sync(*peer, sync, &valset);

        assert_eq!(third_state.pending_block_tree.size(), 4);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        assert!(res.is_none());

        //arrival of proposal should also prevent block_sync_request from modifying the tree
        let cmds2 = third_state.handle_proposal_message_full::<HasherType, _, _>(
            author_2,
            verified_message_2,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        assert_eq!(third_state.pending_block_tree.size(), 5);
        let res = cmds2.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        assert!(res.is_none());

        let sync = BlockSyncResponseMessage::BlockFound(block_2);
        // request sync which did not arrive in time should be ignored.
        let cmds3 = third_state.handle_block_sync(*peer_2, sync, &valset);

        assert_eq!(third_state.pending_block_tree.size(), 5);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        assert!(res.is_none());
    }

    #[traced_test]
    #[test]
    fn test_receive_empty_block() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRoot, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let (state, _) = states.split_first_mut().unwrap();

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());

        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::default(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = p1.destructure();

        let cmds = state.handle_proposal_message::<HasherType>(author, verified_message);
        let result = cmds
            .iter()
            .find(|&c| matches!(c, ConsensusCommand::FetchFullTxs { 0: _, 1: _ }));
        assert!(result.is_some());

        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("monotonic_counter.rx_empty_block"))
                .count()
            {
                1 => Ok(()),
                n => Err(format!("Expected count 1 got {}", n)),
            }
        });
    }

    #[traced_test]
    #[test]
    fn test_lagging_execution() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRoot, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let (state, _) = states.split_first_mut().unwrap();

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());

        let p0 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![0xaa]),
            ExecutionArtifacts::zero(),
        );

        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![0xaa]),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message::<HasherType>(author, verified_message);
        let result = cmds
            .iter()
            .find(|&c| matches!(c, ConsensusCommand::FetchFullTxs { 0: _, 1: _ }));
        assert!(result.is_none());

        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("monotonic_counter.rx_execution_lagging"))
                .count()
            {
                1 => Ok(()),
                n => Err(format!("Expected count 1 got {}", n)),
            }
        });
    }

    #[test]
    fn test_state_root_updates() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRoot, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let (state, _) = states.split_first_mut().unwrap();

        // delay gap in setup is 1

        let mut proposal_gen = ProposalGen::<SignatureType, _>::new(state.high_qc.clone());
        let p0 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = p0.destructure();
        // p0 should have seqnum 1 and therefore only require state_root 0
        // the state_root 0's hash should be Hash([0x00; 32])
        state.handle_state_root_update(SeqNum(0), Hash([0x00; 32]));
        let cmds = state.handle_proposal_message::<HasherType>(author, verified_message.clone());
        let result = cmds
            .iter()
            .find(|&c| matches!(c, ConsensusCommand::FetchFullTxs { 0: _, 1: _ }));
        assert!(result.is_some());
        state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        let p1 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![0xaa]),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: Hash([0x99; 32]),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Gas(0),
            },
        );
        let (author, _, verified_message) = p1.destructure();
        // p1 should have seqnum 2 and therefore only require state_root 1
        state.handle_state_root_update(SeqNum(1), Hash([0x99; 32]));
        let cmds = state.handle_proposal_message::<HasherType>(author, verified_message.clone());
        let result = cmds
            .iter()
            .find(|&c| matches!(c, ConsensusCommand::FetchFullTxs { 0: _, 1: _ }));
        assert!(result.is_some());

        state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );

        // commit some blocks and confirm cleanup of state root hashes happened

        let p2 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![0xaa]),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: Hash([0xbb; 32]),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Gas(0),
            },
        );

        let (author, _, verified_message) = p2.destructure();
        state.handle_state_root_update(SeqNum(2), Hash([0xbb; 32]));
        let cmds = state.handle_proposal_message::<HasherType>(author, verified_message.clone());
        let result = cmds
            .iter()
            .find(|&c| matches!(c, ConsensusCommand::FetchFullTxs { 0: _, 1: _ }));
        assert!(result.is_some());

        let p2_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());

        let p3 = proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![0xaa]),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: Hash([0xcc; 32]),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Gas(0),
            },
        );

        let (author, _, verified_message) = p3.destructure();
        state.handle_state_root_update(SeqNum(3), Hash([0xcc; 32]));
        let cmds = state.handle_proposal_message::<HasherType>(author, verified_message.clone());
        let result = cmds
            .iter()
            .find(|&c| matches!(c, ConsensusCommand::FetchFullTxs { 0: _, 1: _ }));
        assert!(result.is_some());

        let p3_cmds = state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
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
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let (first_state, xs) = states.split_first_mut().unwrap();

        let mut correct_proposal_gen =
            ProposalGen::<SignatureType, _>::new(first_state.high_qc.clone());
        let mut branch_off_proposal_gen =
            ProposalGen::<SignatureType, _>::new(first_state.high_qc.clone());

        let cp1 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::default(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp1.destructure();
        let block_1 = UnverifiedFullBlock {
            block: verified_message.block.clone(),
            full_txs: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
        };
        let bid_correct = block_1.block.get_id();
        // requesting a block that's doesn't exists should yield None
        assert_eq!(first_state.fetch_uncommitted_block(&bid_correct), None);
        // assuming a proposal comes in, should allow it to be fetched as it is within pending block tree
        first_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let full_block = first_state.fetch_uncommitted_block(&bid_correct).unwrap();
        assert_eq!(full_block.get_id(), bid_correct);

        // you can also receive a branch, which would cause pending block tree retrieval to also be valid
        let bp1 = branch_off_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(vec![13, 32]),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = bp1.destructure();
        let block_1 = UnverifiedFullBlock {
            block: verified_message.block.clone(),
            full_txs: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
        };
        let bid_branch = block_1.block.get_id();
        assert_eq!(first_state.fetch_uncommitted_block(&bid_branch), None);

        first_state.handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let full_block = first_state.fetch_uncommitted_block(&bid_branch).unwrap();
        assert_eq!(full_block.get_id(), bid_branch);

        // if a certain commit is triggered, then fetching block would fail
        for i in 0..3 {
            let cp = correct_proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            let block = UnverifiedFullBlock {
                block: verified_message.block.clone(),
                full_txs: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            };
            let bid = block.block.get_id();
            // requesting a block that's doesn't exists should yield None
            assert_eq!(first_state.fetch_uncommitted_block(&bid), None);
            // assuming a proposal comes in, should allow it to be fetched as it is within pending block tree

            first_state.handle_proposal_message_full::<HasherType, _, _>(
                author,
                verified_message,
                FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
                &valset,
                &valmap,
                &election,
            );
            let full_block = first_state.fetch_uncommitted_block(&bid).unwrap();
            assert_eq!(full_block.get_id(), bid);
            // first prune remove the blocks that are branches
            if i == 1 {
                assert_eq!(first_state.fetch_uncommitted_block(&bid_branch), None);
                assert!(first_state.fetch_uncommitted_block(&bid_correct).is_some());
            } else if i == 2 {
                assert_eq!(first_state.fetch_uncommitted_block(&bid_correct), None);
                assert_eq!(first_state.fetch_uncommitted_block(&bid_branch), None);
            }
        }
    }

    #[test_case(4; "4 participants")]
    #[test_case(5; "5 participants")]
    #[test_case(6; "6 participants")]
    #[test_case(7; "7 participants")]
    #[test_case(100; "100 participants")]
    #[test_case(523; "523 participants")]
    fn test_observing_qc_through_votes(num_state: usize) {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(
                num_state as u32,
            );

        let election = SimpleRoundRobin::new();
        let mut correct_proposal_gen =
            ProposalGen::<SignatureType, _>::new(states[0].high_qc.clone());

        for i in 0..8 {
            let cp = correct_proposal_gen.next_proposal(
                &keys,
                &certkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for state in states.iter_mut() {
                let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
                    author,
                    verified_message.clone(),
                    FullTransactionList::new(Vec::new()),
                    &valset,
                    &valmap,
                    &election,
                );
                let bsync_reqest = cmds.iter().find(|&c| {
                    matches!(
                        c,
                        ConsensusCommand::RequestSync {
                            peer: NodeId(_),
                            block_id: BlockId(_)
                        }
                    )
                });
                // observing a qc that link to root should not trigger anything
                assert!(bsync_reqest.is_none());
                assert_eq!(state.get_current_round(), Round(i + 1))
            }
        }
        for state in states.iter() {
            assert_eq!(state.get_current_round(), Round(8));
        }
        let next_leader = election.get_leader(Round(10), valset.get_list());
        let mut leader_index = 0;
        // test when observing a qc through vote message, and qc points to a block that doesn't exists yet
        let cp = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let (author, _, verified_message) = cp.destructure();
        let mut votes = vec![];
        for (i, state) in states.iter_mut().enumerate() {
            if state.nodeid != next_leader {
                let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
                    author,
                    verified_message.clone(),
                    FullTransactionList::new(Vec::new()),
                    &valset,
                    &valmap,
                    &election,
                );

                let v: Vec<VoteMessage<SignatureCollectionType>> = cmds
                    .into_iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::Publish {
                            target: RouterTarget::PointToPoint(peer),
                            message: ConsensusMessage::Vote(vote),
                        } => {
                            if peer == next_leader {
                                Some(vote)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(v.len(), 1);
                votes.push((state.nodeid, v[0]));
            } else {
                leader_index = i;
            }
        }
        for (i, (author, v)) in votes.iter().enumerate() {
            let cmds = states[leader_index]
                .handle_vote_message::<HasherType, _, _>(*author, *v, &valset, &valmap, &election);
            if i == (num_state * 2 / 3) {
                let req: Vec<(NodeId, BlockId)> = cmds
                    .into_iter()
                    .filter_map(|c| match c {
                        ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                        _ => None,
                    })
                    .collect();
                assert_eq!(req.len(), 1);
                assert_eq!(req[0].1, verified_message.block.get_id());
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
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(
                num_state as u32,
            );
        let election = SimpleRoundRobin::new();
        let mut propgen = ProposalGen::<SignatureType, _>::new(states[0].high_qc.clone());
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            );

            let (author, _, verified_message) = cp.destructure();
            for state in states.iter_mut().skip(1) {
                let cmds = state.handle_proposal_message_full::<HasherType, _, _>(
                    author,
                    verified_message.clone(),
                    FullTransactionList::new(Vec::new()),
                    &valset,
                    &valmap,
                    &election,
                );
                let bsync_reqest = cmds.iter().find(|&c| {
                    matches!(
                        c,
                        ConsensusCommand::RequestSync {
                            peer: NodeId(_),
                            block_id: BlockId(_)
                        }
                    )
                });
                // observing a qc that link to root should not trigger anything
                assert!(bsync_reqest.is_none());
            }
            blocks.push(verified_message.block);
        }

        // now timeout someone
        let cmds = states[1].handle_timeout_expiry::<HasherType>();
        let tmo: Vec<&Timeout<SignatureCollectionType>> = cmds
            .iter()
            .filter_map(|cmd| match cmd {
                PacemakerCommand::Broadcast(TimeoutMessage {
                    timeout: tmo,
                    sig: _,
                }) => Some(tmo),
                _ => None,
            })
            .collect();

        assert_eq!(tmo.len(), 1);
        assert_eq!(tmo[0].tminfo.round, Round(4));
        let author = states[1].nodeid;
        let timeout_msg = TimeoutMessage::new::<HasherType>(tmo[0].clone(), &certkeys[1]);
        let cmds = states[0].handle_timeout_message::<HasherType, _, _>(
            author,
            timeout_msg,
            &valset,
            &valmap,
            &election,
        );

        let req: Vec<(NodeId, BlockId)> = cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                _ => None,
            })
            .collect();
        assert_eq!(req.len(), 1);
        assert_eq!(req[0].1, blocks[2].get_id());
    }

    #[test]
    fn test_observe_qc_through_proposal() {
        let num_state = 5;
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<SignatureCollectionType, StateRootValidatorType, TransactionValidatorType>(
                num_state as u32,
            );
        let election = SimpleRoundRobin::new();
        let mut propgen = ProposalGen::<SignatureType, _>::new(states[0].high_qc.clone());
        let mut blocks = vec![];
        for _ in 0..4 {
            let cp = propgen.next_proposal(
                &keys,
                &certkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            );

            let (_, _, verified_message) = cp.destructure();
            blocks.push(verified_message.block);
        }
        let cp = propgen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp.destructure();
        let cmds = states[1].handle_proposal_message_full::<HasherType, _, _>(
            author,
            verified_message,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &valset,
            &valmap,
            &election,
        );
        let req: Vec<(NodeId, BlockId)> = cmds
            .into_iter()
            .filter_map(|c| match c {
                ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
                _ => None,
            })
            .collect();
        assert_eq!(req.len(), 1);
        assert_eq!(req[0].1, blocks[3].get_id());
    }
}
