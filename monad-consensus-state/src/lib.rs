use std::time::Duration;

use log::{debug, warn};
use monad_blocktree::blocktree::{BlockTree, RootKind};
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncMessage, ProposalMessage, TimeoutMessage, VoteMessage},
    },
    pacemaker::{Pacemaker, PacemakerCommand, PacemakerTimerExpire},
    validation::safety::Safety,
    vote_state::VoteState,
};
use monad_consensus_types::{
    block::{Block, BlockType},
    message_signature::MessageSignature,
    payload::{FullTransactionList, StateRootResult, StateRootValidator, TransactionList},
    quorum_certificate::{QuorumCertificate, Rank},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
    transaction_validator::TransactionValidator,
    validation::Hasher,
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_executor::{PeerId, RouterTarget};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, Hash, NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetType};
use tracing::trace;

use crate::{
    blocksync::{BlockSyncManager, BlockSyncResult},
    command::{ConsensusCommand, FetchedFullTxs, FetchedTxs},
};

pub mod blocksync;
pub mod command;
pub mod wrapper;

pub struct ConsensusState<ST, SCT: SignatureCollection, TV, SVT> {
    pending_block_tree: BlockTree<SCT>,
    vote_state: VoteState<SCT>,
    high_qc: QuorumCertificate<SCT>,
    state_root_validator: SVT,

    pacemaker: Pacemaker<ST, SCT>,
    safety: Safety,

    nodeid: NodeId,
    config: ConsensusConfig,

    transaction_validator: TV,
    block_sync_manager: BlockSyncManager<SCT>,

    // TODO deprecate
    keypair: KeyPair,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
}

pub struct ConsensusConfig {
    pub proposal_size: usize,
    pub state_root_delay: u64,
    pub propose_with_missing_blocks: bool,
}

pub trait ConsensusProcess<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type TransactionValidatorType;

    fn new(
        transaction_validator: Self::TransactionValidatorType,
        my_pubkey: PubKey,
        genesis_block: Block<SCT>,
        genesis_qc: QuorumCertificate<SCT>,
        delta: Duration,
        config: ConsensusConfig,
        keypair: KeyPair,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
    ) -> Self;

    fn get_pubkey(&self) -> PubKey;

    fn get_nodeid(&self) -> NodeId;

    fn blocktree(&self) -> &BlockTree<SCT>;

    fn handle_timeout_expiry(
        &mut self,
        event: PacemakerTimerExpire,
    ) -> Vec<PacemakerCommand<ST, SCT>>;

    fn handle_proposal_message<H: Hasher>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<ST, SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>>;

    fn handle_proposal_message_full<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<ST, SCT>,
        txns: FullTransactionList,
        validators: &VT,
        election: &LT,
    ) -> Vec<ConsensusCommand<ST, SCT>>;

    fn handle_vote_message<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        v: VoteMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        election: &LT,
    ) -> Vec<ConsensusCommand<ST, SCT>>;

    fn handle_timeout_message<VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: ST,
        tm: TimeoutMessage<ST, SCT>,
        validators: &VT,
        election: &LT,
    ) -> Vec<ConsensusCommand<ST, SCT>>;

    fn handle_block_sync<VT: ValidatorSetType>(
        &mut self,
        author: NodeId,
        msg: BlockSyncMessage<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>>;

    fn handle_state_update(&mut self, seq_num: u64, root_hash: Hash);

    fn get_current_round(&self) -> Round;

    fn get_keypair(&self) -> &KeyPair;

    fn request_sync<VT: ValidatorSetType>(
        &mut self,
        qc: QuorumCertificate<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>>;
}

impl<ST, SCT, TVT, SVT> ConsensusProcess<ST, SCT> for ConsensusState<ST, SCT, TVT, SVT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
    TVT: TransactionValidator,
    SVT: StateRootValidator,
{
    type TransactionValidatorType = TVT;

    fn new(
        transaction_validator: Self::TransactionValidatorType,
        my_pubkey: PubKey,
        genesis_block: Block<SCT>,
        genesis_qc: QuorumCertificate<SCT>,
        delta: Duration,
        config: ConsensusConfig,

        // TODO deprecate
        keypair: KeyPair,
        cert_keypair: SignatureCollectionKeyPairType<SCT>,
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
            block_sync_manager: BlockSyncManager::new(),
            keypair,
            cert_keypair,
        }
    }

    fn handle_timeout_expiry(
        &mut self,
        event: PacemakerTimerExpire,
    ) -> Vec<PacemakerCommand<ST, SCT>> {
        inc_count!(local_timeout);
        debug!(
            "local timeout: round={:?}",
            self.pacemaker.get_current_round()
        );
        self.pacemaker
            .handle_event(&mut self.safety, &self.high_qc, event)
    }

    fn handle_proposal_message<H: Hasher>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<ST, SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        // NULL blocks are not required to have state root hashes
        if p.block.payload.txns.0.is_empty() {
            debug!("Received empty block: block={:?}", p.block);
            inc_count!(rx_empty_block);
            return vec![ConsensusCommand::FetchFullTxs(
                TransactionList::default(),
                Box::new(move |txns| FetchedFullTxs { author, p, txns }),
            )];
        }

        match self
            .state_root_validator
            .validate(p.block.payload.seq_num, p.block.payload.header.state_root)
        {
            // TODO execution lagging too far behind should be a trigger for something
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
                    Box::new(move |txns| FetchedFullTxs { author, p, txns }),
                )]
            }
        }
    }

    fn handle_proposal_message_full<H: Hasher, VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<ST, SCT>,
        txns: FullTransactionList,
        validators: &VT,
        election: &LT,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        debug!("Fetched full proposal: {:?}", p);
        inc_count!(fetched_proposal);
        let mut cmds = vec![];

        if !self.transaction_validator.validate(&txns) {
            warn!("Transaction validation failed");
            debug!("failed txns: {:?}", txns);
            inc_count!(failed_txn_validation);
            return cmds;
        }

        let process_certificate_cmds = self.process_certificate_qc(&p.block.qc);
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

        self.pending_block_tree
            .add(p.block.clone())
            .expect("Failed to add block to blocktree");
        if let Some(qc) = self.pending_block_tree.get_missing_ancestor(&p.block.qc) {
            debug!("Block sync request: blockid={:?}", qc);
            cmds.extend(self.request_sync::<VT>(qc, validators));
        }

        if p.block.round != round || author != leader || p.block.author != leader {
            debug!(
                "Invalid proposal: expected-round={:?} \
                round={:?} \
                expected-leader={:?} \
                author={:?} \
                block-author={:?}",
                round, p.block.round, leader, author, p.block.author
            );
            inc_count!(invalid_proposal_round_leader);
            return cmds;
        }

        let vote = self
            .safety
            .make_vote::<ST, SCT, H>(&p.block, &p.last_round_tc);

        if let Some(v) = vote {
            let vote_msg = VoteMessage::<SCT>::new::<H>(v, &self.cert_keypair);

            let next_leader = election.get_leader(round + Round(1), validators.get_list());
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(PeerId(next_leader.0)),
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
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        debug!("Vote Message: {:?}", vote_msg);
        if vote_msg.vote.vote_info.round < self.pacemaker.get_current_round() {
            inc_count!(old_vote_received);
            return Default::default();
        }
        inc_count!(vote_received);

        let qc: Option<QuorumCertificate<SCT>> =
            self.vote_state
                .process_vote::<H, _>(&author, &vote_msg, validators, validator_mapping);

        let mut cmds = Vec::new();
        if let Some(qc) = qc {
            debug!("Created QC {:?}", qc);
            inc_count!(created_qc);
            cmds.extend(self.process_certificate_qc(&qc));

            if self.nodeid
                == election.get_leader(self.pacemaker.get_current_round(), validators.get_list())
            {
                cmds.extend(self.process_new_round_event(None));
            }
        }
        cmds
    }

    fn handle_timeout_message<VT: ValidatorSetType, LT: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: ST,
        tm: TimeoutMessage<ST, SCT>,
        validators: &VT,
        election: &LT,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            inc_count!(old_remote_timeout);
            return cmds;
        }

        debug!("Remote timeout msg: {:?}", tm);
        inc_count!(remote_timeout_msg);

        let process_certificate_cmds = self.process_certificate_qc(&tm.tminfo.high_qc);
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

        let (tc, remote_timeout_cmds) = self.pacemaker.process_remote_timeout(
            validators,
            &mut self.safety,
            &self.high_qc,
            author,
            signature,
            tm,
        );
        cmds.extend(remote_timeout_cmds.into_iter().map(Into::into));
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

    /**
     * Handle_block_sync only respond to blocks that was previously requested.
     * Once successfully removed from requested dict, it then checks if its valid to add to
     * pending_block_tree.
     *
     * Note, that it is possible that a requested block failed to be added to the tree
     * due to the original proposal arrived before requested block is able to be returned,
     * or the requested block is no longer relevant due to prune
     *
     */
    fn handle_block_sync<VT: ValidatorSetType>(
        &mut self,
        author: NodeId,
        msg: BlockSyncMessage<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        let mut cmds = vec![];

        match self
            .block_sync_manager
            .handle_retrieval(&author, msg, validators)
        {
            BlockSyncResult::Success(block) => {
                debug!("Block sync response: bid={:?}", block.get_id());
                inc_count!(block_sync_response);
                if self.pending_block_tree.is_valid(&block) {
                    // check if this block will extend into root
                    if let Some(qc) = self.pending_block_tree.get_missing_ancestor(&block.qc) {
                        cmds.extend(self.request_sync(qc, validators));
                    }
                    self.pending_block_tree
                        .add(block)
                        .expect("failed to add block to tree during block sync");
                }
            }
            BlockSyncResult::Failed(retry_cmd) => cmds.push(retry_cmd),
            BlockSyncResult::IllegalResponse => {
                debug!("Block sync illegal response: author={:?}", author);
            }
        }
        cmds
    }

    fn request_sync<VT: ValidatorSetType>(
        &mut self,
        qc: QuorumCertificate<SCT>,
        validators: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.block_sync_manager.request(&qc, validators)
    }

    fn handle_state_update(&mut self, seq_num: u64, root_hash: Hash) {
        self.state_root_validator.add_state_root(seq_num, root_hash)
    }

    fn get_pubkey(&self) -> PubKey {
        self.nodeid.0
    }

    fn get_nodeid(&self) -> NodeId {
        self.nodeid
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
}

impl<ST, SCT, TVT, SVT> ConsensusState<ST, SCT, TVT, SVT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
    TVT: TransactionValidator,
    SVT: StateRootValidator,
{
    // If the qc has a commit_state_hash, commit the parent block and prune the
    // block tree
    // Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    pub fn process_qc(&mut self, qc: &QuorumCertificate<SCT>) -> Vec<ConsensusCommand<ST, SCT>> {
        if Rank(qc.info) <= Rank(self.high_qc.info) {
            inc_count!(process_old_qc);
            return Vec::new();
        }
        inc_count!(process_qc);

        self.high_qc = qc.clone();
        let mut cmds = Vec::new();
        if qc.info.ledger_commit.commit_state_hash.is_some()
            && self
                .pending_block_tree
                .path_to_root(&qc.info.vote.parent_id)
        {
            let blocks_to_commit = self
                .pending_block_tree
                .prune(&qc.info.vote.parent_id)
                .unwrap_or_else(|_| panic!("\n{:?}", self.pending_block_tree));

            if let Some(b) = blocks_to_commit.last() {
                self.state_root_validator
                    .remove_old_roots(b.payload.seq_num);
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
                cmds.push(ConsensusCommand::<ST, SCT>::LedgerCommit(blocks_to_commit));
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

        cmds.extend(self.pacemaker.advance_round_qc(qc).map(Into::into));
        cmds
    }

    #[must_use]
    fn process_new_round_event(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<ST>>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.vote_state
            .start_new_round(self.pacemaker.get_current_round());

        let node_id = self.nodeid;
        let round = self.pacemaker.get_current_round();
        let high_qc = self.high_qc.clone();

        let parent_bid = high_qc.info.vote.id;
        let seq_num_qc = high_qc.info.vote.seq_num;
        let proposed_seq_num = seq_num_qc + 1;
        match self.proposal_policy(&parent_bid, proposed_seq_num) {
            ConsensusAction::Propose(h) => {
                inc_count!(creating_proposal);
                debug!("Creating Proposal: node_id={:?} round={:?} high_qc={:?}, seq_num={:?}, last_round_tc={:?}", 
                                node_id, round, high_qc, proposed_seq_num, last_round_tc);
                vec![ConsensusCommand::FetchTxs(
                    self.config.proposal_size,
                    Box::new(move |txns| FetchedTxs {
                        node_id,
                        round,
                        seq_num: proposed_seq_num,
                        state_root_hash: h,
                        high_qc,
                        last_round_tc,
                        txns,
                    }),
                )]
            }
            ConsensusAction::Abstain => {
                inc_count!(abstain_proposal);
                // TODO: This could potentially be an empty block
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
                    Box::new(move |_txns| FetchedTxs {
                        node_id,
                        round,
                        seq_num: proposed_seq_num,
                        state_root_hash: Hash([0; 32]),
                        high_qc,
                        last_round_tc,
                        txns: TransactionList::default(),
                    }),
                )]
            }
        }
    }

    #[must_use]
    fn proposal_policy(&self, parent_bid: &BlockId, proposed_seq_num: u64) -> ConsensusAction {
        match self.pending_block_tree.root {
            RootKind::Unrooted(_) => return ConsensusAction::Abstain,
            RootKind::Rooted(_) => (),
        }

        if !self.pending_block_tree.tree().contains_key(parent_bid)
            && !self.config.propose_with_missing_blocks
        {
            return ConsensusAction::Abstain;
        }

        if self.config.propose_with_missing_blocks
            || self.pending_block_tree.path_to_root(parent_bid)
        {
            match self
                .state_root_validator
                .get_next_state_root(proposed_seq_num)
            {
                Some(h) => ConsensusAction::Propose(h),
                None => ConsensusAction::ProposeEmpty,
            }
        } else {
            ConsensusAction::Abstain
        }
    }
}

pub enum ConsensusAction {
    Propose(Hash),
    ProposeEmpty,
    Abstain,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use itertools::Itertools;
    use monad_consensus::{
        messages::message::{BlockSyncMessage, TimeoutMessage, VoteMessage},
        pacemaker::PacemakerTimerExpire,
        validation::signing::Verified,
    };
    use monad_consensus_types::{
        block::BlockType,
        certificate_signature::CertificateKeyPair,
        ledger::LedgerCommitInfo,
        message_signature::MessageSignature,
        multi_sig::MultiSig,
        payload::{
            ExecutionArtifacts, FullTransactionList, NopStateRoot, StateRootValidator,
            TransactionList,
        },
        quorum_certificate::{genesis_vote_info, QuorumCertificate},
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        timeout::TimeoutInfo,
        transaction_validator::MockValidator,
        validation::Sha256Hash,
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{secp256k1::KeyPair, NopSignature};
    use monad_executor::{PeerId, RouterTarget};
    use monad_testutil::{
        proposal::ProposalGen,
        signing::{create_certificate_keys, create_keys, get_genesis_config},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Hash, NodeId, Round};
    use monad_validator::{
        leader_election::LeaderElection, simple_round_robin::SimpleRoundRobin,
        validator_set::ValidatorSet,
    };
    use tracing_test::traced_test;

    use crate::{
        ConsensusCommand, ConsensusConfig, ConsensusMessage, ConsensusProcess, ConsensusState,
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;
    type StateRootValidatorType = NopStateRoot;

    fn setup<ST: MessageSignature, SCT: SignatureCollection, SVT: StateRootValidator>(
        num_states: u32,
    ) -> (
        Vec<KeyPair>,
        Vec<SignatureCollectionKeyPairType<SCT>>,
        ValidatorSet,
        ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        Vec<ConsensusState<ST, SCT, MockValidator, SVT>>,
    ) {
        let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<SCT>(num_states);

        let voting_keys = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(cert_keys.iter())
            .collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) =
            get_genesis_config::<Sha256Hash, SCT>(voting_keys.iter(), &valmap);

        let genesis_qc = QuorumCertificate::genesis_qc::<Sha256Hash>(
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
                ConsensusState::<ST, SCT, _, SVT>::new(
                    MockValidator,
                    k.pubkey(),
                    genesis_block.clone(),
                    genesis_qc.clone(),
                    Duration::from_secs(1),
                    ConsensusConfig {
                        proposal_size: 5000,
                        state_root_delay: 0,
                        propose_with_missing_blocks: false,
                    },
                    std::mem::replace(&mut dupkeys[i], default_key),
                    std::mem::replace(&mut dupcertkeys[i], default_cert_key),
                )
            })
            .collect::<Vec<_>>();

        (keys, cert_keys, valset, valmap, consensus_states)
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[traced_test]
    #[test_log::test]
    fn lock_qc_high() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<NopSignature, SignatureCollectionType, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();

        let state = &mut states[0];
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
            seq_num: 0,
        };
        let v = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(None, &vi),
        };

        let vm1 = VoteMessage::<SignatureCollectionType>::new::<Sha256Hash>(v, &certkeys[1]);
        let vm2 = VoteMessage::<SignatureCollectionType>::new::<Sha256Hash>(v, &certkeys[2]);
        let vm3 = VoteMessage::<SignatureCollectionType>::new::<Sha256Hash>(v, &certkeys[3]);

        let v1 = Verified::<SignatureType, _>::new::<Sha256Hash>(vm1, &keys[1]);
        let v2 = Verified::<SignatureType, _>::new::<Sha256Hash>(vm2, &keys[2]);
        let v3 = Verified::<SignatureType, _>::new::<Sha256Hash>(vm3, &keys[3]);

        state.handle_vote_message::<Sha256Hash, _, _>(
            *v1.author(),
            *v1,
            &valset,
            &valmap,
            &election,
        );
        state.handle_vote_message::<Sha256Hash, _, _>(
            *v2.author(),
            *v2,
            &valset,
            &valmap,
            &election,
        );

        // less than 2f+1, so expect not locked
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        state.handle_vote_message::<Sha256Hash, _, _>(
            *v3.author(),
            *v3,
            &valset,
            &valmap,
            &election,
        );
        assert_eq!(state.high_qc.info.vote.round, expected_qc_high_round);

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
            setup::<SignatureType, SignatureCollectionType, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());
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
        let _ =
            state
                .pacemaker
                .handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );

        assert_eq!(state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn old_qc_in_timeout_message() {
        let (keys, cerkeys, valset, valmap, mut states) =
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let mut qc2 = state.high_qc.clone();

        for i in 1..5 {
            let p = propgen.next_proposal(
                &keys,
                &cerkeys,
                &valset,
                &election,
                &valmap,
                Default::default(),
                ExecutionArtifacts::zero(),
            );
            let (author, _, verified_message) = p.clone().destructure();
            let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
                author,
                verified_message,
                FullTransactionList(Vec::new()),
                &valset,
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

            if i == 3 {
                qc2 = p.block.qc.clone();
                assert_eq!(qc2.info.vote.round, Round(2));
            }

            assert_eq!(state.pacemaker.get_current_round(), Round(i));
            assert!(result.is_some());
        }

        let byzantine_tm = TimeoutMessage {
            tminfo: TimeoutInfo {
                round: state.pacemaker.get_current_round(),
                high_qc: qc2,
            },
            last_round_tc: None,
        };
        let signed_byzantine_tm = Verified::new::<Sha256Hash>(byzantine_tm, &keys[1]);
        let (author, signature, tm) = signed_byzantine_tm.destructure();
        state.handle_timeout_message::<_, _>(author, signature, tm, &valset, &election);
    }

    #[test]
    fn duplicate_proposals() {
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );

        // confirm the size of the pending_block_tree (genesis, p1, p2, p_fut)
        assert_eq!(state.pending_block_tree.size(), 4);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(state.handle_proposal_message_full::<Sha256Hash, _, _>(
                author,
                verified_message,
                FullTransactionList(Vec::new()),
                &valset,
                &election,
            ));
        }

        let _self_id = PeerId(state.nodeid.0);
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
                    proposals.extend(state.handle_proposal_message_full::<Sha256Hash, _, _>(
                        m.block.author,
                        m.clone(),
                        FullTransactionList(Vec::new()),
                        &valset,
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
        state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

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
        let p1_cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let p2_cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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

        let p2_cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());
    }

    #[test]
    fn test_commit_rule_non_consecutive() {
        use monad_consensus::pacemaker::PacemakerCommand::Broadcast;
        let (keys, certkeys, valset, valmap, mut states) =
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

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
        state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let p2_cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let pacemaker_cmds =
            state
                .pacemaker
                .handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);

        let broadcast_cmd = pacemaker_cmds
            .iter()
            .find(|cmd| matches!(cmd, Broadcast(_)))
            .unwrap();

        let tmo_msg = if let Broadcast(tmo_msg) = broadcast_cmd {
            tmo_msg
        } else {
            panic!()
        };
        assert_eq!(tmo_msg.tminfo.round, Round(2));

        let _ = propgen.next_tc(&keys, &valset);

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
        assert_eq!(p3.block.qc.info.vote.round, Round(1));
        assert_eq!(p3.block.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let p3_cmds = state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
            setup::<NopSignature, MultiSig<NopSignature>, StateRootValidatorType>(4);
        let election = SimpleRoundRobin::new();
        let (first_state, xs) = states.split_first_mut().unwrap();
        let (second_state, xs) = xs.split_first_mut().unwrap();
        let (third_state, xs) = xs.split_first_mut().unwrap();
        let fourth_state = &mut xs[0];

        // first_state will send 2 different proposals, A and B. A will be sent to
        // the next leader, all other nodes get B.
        // effect is that nodes send votes for B to the next leader who thinks that
        // the "correct" proposal is A.
        let mut correct_proposal_gen = ProposalGen::new(first_state.high_qc.clone());
        let mut mal_proposal_gen = ProposalGen::new(first_state.high_qc.clone());

        let cp1 = correct_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            Default::default(),
            ExecutionArtifacts::zero(),
        );
        let mp1 = mal_proposal_gen.next_proposal(
            &keys,
            &certkeys,
            &valset,
            &election,
            &valmap,
            TransactionList(vec![5]),
            ExecutionArtifacts::zero(),
        );

        let (author, _, verified_message) = cp1.destructure();
        let block_1 = verified_message.block.clone();
        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &valset,
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

        let cmds3 = third_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &valset,
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

        let cmds4 = fourth_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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
        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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

        // Deliver all the votes to second_state, who is the leader for the next round
        for i in 0..4 {
            let v = Verified::<NopSignature, VoteMessage<_>>::new::<Sha256Hash>(votes[i], &keys[i]);
            second_state.handle_vote_message::<Sha256Hash, _, _>(
                *v.author(),
                *v,
                &valset,
                &valmap,
                &election,
            );
        }

        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(second_state.high_qc.info.vote, votes[0].vote.vote_info);

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
        let block_2 = verified_message_2.block.clone();
        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author_2,
            verified_message_2.clone(),
            FullTransactionList(Vec::new()),
            &valset,
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
        assert!(res.is_some());
        let (req_target, sync_bid) = match res.unwrap() {
            ConsensusCommand::RequestSync { peer, block_id } => Some((peer, block_id)),
            _ => None,
        }
        .unwrap();

        assert!(sync_bid == block_1.get_id());

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author_2,
            verified_message_2.clone(),
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );
        let res = cmds1.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
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
        let block_3 = verified_message.block.clone();

        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );
        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
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

        let msg = BlockSyncMessage::BlockFound(block_1);
        // a block sync request arrived, helping second state to recover
        second_state.handle_block_sync(req_target, msg, &valset);

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
        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );
        // new block added should allow path_to_root properly, thus no more request sync
        let res = cmds2.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
                },
            )
        });
        assert!(res.is_none());

        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &valset,
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

        let cmds3 = third_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );

        assert_eq!(third_state.pending_block_tree.size(), 3);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
                },
            )
        });
        let Some(ConsensusCommand::RequestSync { peer, block_id }) = res else {
            panic!("request sync is not found")
        };

        let mal_sync = BlockSyncMessage::NotAvailable(block_2.get_id());
        // BlockSyncMessage on blocks that were not requested should be ignored.
        let cmds3 = third_state.handle_block_sync(author_2, mal_sync, &valset);

        assert_eq!(third_state.pending_block_tree.size(), 3);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
                },
            )
        });
        assert!(res.is_none());

        let sync = BlockSyncMessage::BlockFound(block_3);

        let cmds3 = third_state.handle_block_sync(*peer, sync.clone(), &valset);

        assert_eq!(third_state.pending_block_tree.size(), 4);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
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
                    block_id: BlockId(_)
                },
            )
        });
        assert!(res.is_none());

        //arrival of proposal should also prevent block_sync_request from modifying the tree
        let cmds2 = third_state.handle_proposal_message_full::<Sha256Hash, _, _>(
            author_2,
            verified_message_2,
            FullTransactionList(Vec::new()),
            &valset,
            &election,
        );
        assert_eq!(third_state.pending_block_tree.size(), 5);
        let res = cmds2.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
                },
            )
        });
        assert!(res.is_none());

        let sync = BlockSyncMessage::BlockFound(block_2);
        // request sync which did not arrive in time should be ignored.
        let cmds3 = third_state.handle_block_sync(*peer_2, sync, &valset);

        assert_eq!(third_state.pending_block_tree.size(), 5);
        let res = cmds3.iter().clone().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_)
                },
            )
        });
        assert!(res.is_none());
    }
}
