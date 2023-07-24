use std::time::Duration;

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::consensus_message::ConsensusMessage,
    messages::message::{ProposalMessage, TimeoutMessage, VoteMessage},
    pacemaker::Pacemaker,
    validation::safety::Safety,
    vote_state::VoteState,
};
use monad_consensus_types::{
    block::{Block, FullTransactionList},
    quorum_certificate::{QuorumCertificate, Rank},
    signature::SignatureCollection,
    timeout::TimeoutCertificate,
    transaction_validator::TransactionValidator,
    validation::Hasher,
};
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    Signature,
};
use monad_executor::{PeerId, RouterTarget};
use monad_types::{NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSet};

use crate::command::{ConsensusCommand, FetchedFullTxs, FetchedTxs};

pub mod command;
pub mod wrapper;

pub struct ConsensusState<S, SC, TV> {
    pub pending_block_tree: BlockTree<SC>,
    pub vote_state: VoteState<SC>,
    pub high_qc: QuorumCertificate<SC>,

    pub pacemaker: Pacemaker<S, SC>,
    pub safety: Safety,

    pub nodeid: NodeId,

    pub transaction_validator: TV,

    // TODO deprecate
    pub keypair: KeyPair,
}

impl<S, SC, TV> ConsensusState<S, SC, TV>
where
    S: Signature,
    SC: SignatureCollection,
    TV: TransactionValidator,
{
    pub fn new(
        transaction_validator: TV,

        my_pubkey: PubKey,
        genesis_block: Block<SC>,
        genesis_qc: QuorumCertificate<SC>,
        delta: Duration,

        // TODO deprecate
        keypair: KeyPair,
    ) -> Self {
        ConsensusState {
            pending_block_tree: BlockTree::new(genesis_block),
            vote_state: VoteState::default(),
            high_qc: genesis_qc,
            pacemaker: Pacemaker::new(delta, Round(1), None),
            safety: Safety::default(),
            nodeid: NodeId(my_pubkey),

            transaction_validator,

            keypair,
        }
    }

    pub fn handle_proposal_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<S, SC>,
    ) -> Vec<ConsensusCommand<S, SC>> {
        vec![ConsensusCommand::FetchFullTxs(Box::new(move |txns| {
            FetchedFullTxs { author, p, txns }
        }))]
    }

    pub fn handle_proposal_message_full<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<S, SC>,
        txns: FullTransactionList,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, SC>> {
        let mut cmds = vec![];

        if !self.transaction_validator.validate(&txns) {
            return cmds;
        }

        let process_certificate_cmds = self.process_certificate_qc(&p.block.qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let round = self.pacemaker.get_current_round();
        let leader = *validators.get_leader(round);

        self.pending_block_tree
            .add(p.block.clone())
            .expect("Failed to add block to blocktree");

        if !self.pending_block_tree.has_parent(&p.block) {
            cmds.push(ConsensusCommand::RequestSync {
                blockid: p.block.get_parent_id(),
            });
        }

        if p.block.round != round || author != leader || p.block.author != leader {
            return cmds;
        }

        let vote_msg = self
            .safety
            .make_vote::<S, SC, H>(&p.block, &p.last_round_tc);

        if let Some(v) = vote_msg {
            let next_leader = validators.get_leader(round + Round(1));
            let send_cmd = ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(PeerId(next_leader.0)),
                message: ConsensusMessage::Vote(v),
            };
            cmds.push(send_cmd);
        }

        cmds
    }

    pub fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: SC::SignatureType,
        v: VoteMessage,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, SC>> {
        if v.vote_info.round < self.pacemaker.get_current_round() {
            return Default::default();
        }

        let qc: Option<QuorumCertificate<SC>> = self
            .vote_state
            .process_vote::<V, H>(&author, &signature, &v, validators);

        let mut cmds = Vec::new();
        if let Some(qc) = qc {
            cmds.extend(self.process_certificate_qc(&qc));

            if self.nodeid == *validators.get_leader(self.pacemaker.get_current_round()) {
                cmds.extend(self.process_new_round_event(None));
            }
        }
        cmds
    }

    pub fn handle_timeout_message<V: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: S,
        tm: TimeoutMessage<S, SC>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, SC>> {
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            return cmds;
        }

        let process_certificate_cmds = self.process_certificate_qc(&tm.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
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
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(&tc)
                .into_iter()
                .map(Into::into);
            cmds.extend(advance_round_cmds);

            if self.nodeid == *validators.get_leader(self.pacemaker.get_current_round()) {
                cmds.extend(self.process_new_round_event(Some(tc)));
            }
        }

        cmds
    }

    // If the qc has a commit_state_hash, commit the parent block and prune the
    // block tree
    // Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    #[must_use]
    pub fn process_qc(&mut self, qc: &QuorumCertificate<SC>) -> Vec<ConsensusCommand<S, SC>> {
        if Rank(qc.info) <= Rank(self.high_qc.info) {
            return Vec::new();
        }

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

            if !blocks_to_commit.is_empty() {
                cmds.extend(
                    blocks_to_commit
                        .into_iter()
                        .map(ConsensusCommand::<S, SC>::LedgerCommit),
                );
            }
        }
        cmds
    }

    // TODO consider changing return type to Option<T>
    #[must_use]
    pub fn process_certificate_qc(
        &mut self,
        qc: &QuorumCertificate<SC>,
    ) -> Vec<ConsensusCommand<S, SC>> {
        let mut cmds = Vec::new();
        cmds.extend(self.process_qc(qc));

        cmds.extend(
            self.pacemaker
                .advance_round_qc(qc)
                .map(Into::into)
                .into_iter(),
        );
        cmds
    }

    // TODO consider changing return type to Option<T>
    #[must_use]
    pub fn process_new_round_event(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<S>>,
    ) -> Vec<ConsensusCommand<S, SC>> {
        self.vote_state
            .start_new_round(self.pacemaker.get_current_round());

        let node_id = self.nodeid;
        let round = self.pacemaker.get_current_round();
        let high_qc = self.high_qc.clone();

        vec![ConsensusCommand::FetchTxs(Box::new(move |txns| {
            FetchedTxs {
                node_id,
                round,
                high_qc,
                last_round_tc,
                txns,
            }
        }))]
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use monad_consensus_types::block::{FullTransactionList, TransactionList};
    use monad_consensus_types::transaction_validator::MockValidator;
    use std::time::Duration;

    use monad_consensus::messages::message::{TimeoutMessage, VoteMessage};
    use monad_consensus::pacemaker::PacemakerTimerExpire;
    use monad_consensus::validation::signing::Verified;
    use monad_consensus_types::ledger::LedgerCommitInfo;
    use monad_consensus_types::multi_sig::MultiSig;
    use monad_consensus_types::quorum_certificate::{genesis_vote_info, QuorumCertificate};
    use monad_consensus_types::signature::SignatureCollection;
    use monad_consensus_types::timeout::TimeoutInfo;
    use monad_consensus_types::validation::Sha256Hash;
    use monad_consensus_types::voting::VoteInfo;
    use monad_crypto::secp256k1::KeyPair;
    use monad_crypto::{NopSignature, Signature};
    use monad_executor::{PeerId, RouterTarget};
    use monad_testutil::proposal::ProposalGen;
    use monad_testutil::signing::{create_keys, get_genesis_config};
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};
    use monad_validator::{validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin};

    use crate::{ConsensusCommand, ConsensusMessage, ConsensusState};

    fn setup<ST: Signature, SCT: SignatureCollection<SignatureType = ST>>(
        num_states: u32,
    ) -> (
        Vec<KeyPair>,
        ValidatorSet<WeightedRoundRobin>,
        Vec<ConsensusState<ST, SCT, MockValidator>>,
    ) {
        let keys = create_keys(num_states);
        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SCT>(keys.iter());

        let validator_list = pubkeys
            .into_iter()
            .map(|pubkey| (NodeId(pubkey), Stake(1)))
            .collect::<Vec<_>>();

        let val_set: ValidatorSet<WeightedRoundRobin> =
            ValidatorSet::new(validator_list).expect("initial validator set init failed");

        let genesis_qc = QuorumCertificate::genesis_qc::<Sha256Hash>(
            genesis_vote_info(genesis_block.get_id()),
            genesis_sigs,
        );

        let mut dupkeys = create_keys(num_states);
        let consensus_states = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let default_key = KeyPair::from_bytes([127; 32]).unwrap();
                ConsensusState::<ST, SCT, _>::new(
                    MockValidator,
                    k.pubkey(),
                    genesis_block.clone(),
                    genesis_qc.clone(),
                    Duration::from_secs(1),
                    std::mem::replace(&mut dupkeys[i], default_key),
                )
            })
            .collect::<Vec<_>>();

        (keys, val_set, consensus_states)
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[test]
    fn lock_qc_high() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
        };
        let vm = VoteMessage {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(None, &vi),
        };

        let v1 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[1]);
        let v2 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[2]);
        let v3 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[3]);

        state.handle_vote_message::<Sha256Hash, _>(
            *v1.author(),
            *v1.author_signature(),
            *v1,
            &mut valset,
        );
        state.handle_vote_message::<Sha256Hash, _>(
            *v2.author(),
            *v2.author_signature(),
            *v2,
            &mut valset,
        );

        // less than 2f+1, so expect not locked
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        state.handle_vote_message::<Sha256Hash, _>(
            *v3.author(),
            *v3.author_signature(),
            *v3,
            &mut valset,
        );
        assert_eq!(state.high_qc.info.vote.round, expected_qc_high_round);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());

        // local timeout for state in Round 1
        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        let _ =
            state
                .pacemaker
                .handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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

        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
            propgen.next_proposal(&keys, &mut valset, &Default::default());
        }
        let p7 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p7.destructure();
        state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
        );

        assert_eq!(state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn old_qc_in_timeout_message() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let mut qc2 = state.high_qc.clone();

        for i in 1..5 {
            let p = propgen.next_proposal(&keys, &mut valset, &Default::default());
            let (author, _, verified_message) = p.clone().destructure();
            let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
                author,
                verified_message,
                FullTransactionList(Vec::new()),
                &mut valset,
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
        state.handle_timeout_message::<_>(author, signature, tm, &mut valset);
    }

    #[test]
    fn duplicate_proposals() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);
        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        // first proposal
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
            missing_proposals.push(propgen.next_proposal(&keys, &mut valset, &Default::default()));
        }

        // last proposal arrvies
        let p_fut = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p_fut.destructure();
        state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
        );

        // confirm the size of the pending_block_tree (genesis, p1, p2, p_fut)
        assert_eq!(state.pending_block_tree.size(), 4);

        // missed proposals now arrive
        let mut cmds = Vec::new();
        for i in &perms {
            let (author, _, verified_message) = missing_proposals[*i].clone().destructure();
            cmds.extend(state.handle_proposal_message_full::<Sha256Hash, _>(
                author,
                verified_message,
                FullTransactionList(Vec::new()),
                &mut valset,
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
                    proposals.extend(state.handle_proposal_message_full::<Sha256Hash, _>(
                        m.block.author,
                        m.clone(),
                        FullTransactionList(Vec::new()),
                        &mut valset,
                    ));
                }
                _ => more_proposals = false,
            }
            cmds.extend(proposals);
        }

        // next proposal will trigger everything to be committed if there is
        // a consecutive chain as expected
        // last proposal arrvies
        let p_last = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p_last.destructure();
        state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        // round 1 proposal
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        let p1_cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        assert!(p1_votes[0].ledger_commit_info.commit_state_hash.is_some());

        // round 2 proposal
        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        assert!(p2_votes[0].ledger_commit_info.commit_state_hash.is_some());

        let p3 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p3.destructure();

        let p2_cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
        );
        let lc = p2_cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerCommit(_)));
        assert!(lc.is_some());
    }

    #[test]
    fn test_commit_rule_non_consecutive() {
        use monad_consensus::pacemaker::PacemakerCommand::Broadcast;
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

        let state = &mut states[0];
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        // round 1 proposal
        let p1 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p1.destructure();
        state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
        );

        // round 2 proposal
        let p2 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = p2.destructure();
        let p2_cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        assert!(p2_votes[0].ledger_commit_info.commit_state_hash.is_some());

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

        let _ = propgen.next_tc(&keys, &mut valset);

        // round 3 proposal, has qc(1)
        let p3 = propgen.next_proposal(&keys, &mut valset, &Default::default());
        assert_eq!(p3.block.qc.info.vote.round, Round(1));
        assert_eq!(p3.block.round, Round(3));
        let (author, _, verified_message) = p3.destructure();
        let p3_cmds = state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        assert!(p3_votes[0].ledger_commit_info.commit_state_hash.is_none());
    }

    // this test checks that a malicious proposal sent only to the next leader is
    // not incorrectly committed
    #[test]
    fn test_malicious_proposal_to_next_leader() {
        let (keys, mut valset, mut states) = setup::<NopSignature, MultiSig<NopSignature>>(4);

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

        let cp1 = correct_proposal_gen.next_proposal(&keys, &mut valset, &Default::default());
        let mp1 = mal_proposal_gen.next_proposal(&keys, &mut valset, &TransactionList(vec![5]));

        let (author, _, verified_message) = cp1.destructure();
        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &mut valset,
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

        let cmds3 = third_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &mut valset,
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

        let cmds4 = fourth_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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

        assert_eq!(p1_votes, p3_votes);
        assert_eq!(p1_votes, p4_votes);
        assert_ne!(p1_votes, p2_votes);

        let votes = vec![p1_votes, p2_votes, p3_votes, p4_votes];

        // Deliver all the votes to second_state, who is the leader for the next round
        for i in 0..4 {
            let v = Verified::<NopSignature, VoteMessage>::new::<Sha256Hash>(votes[i], &keys[i]);
            second_state.handle_vote_message::<Sha256Hash, _>(
                *v.author(),
                *v.author_signature(),
                *v,
                &mut valset,
            );
        }

        // confirm that the votes lead to a QC forming (which leads to high_qc update)
        assert_eq!(second_state.high_qc.info.vote, votes[0].vote_info);

        // use the correct proposal gen to make next proposal and send it to second_state
        // this should cause it to emit the RequestSync command because the the QC parent
        // points to a different proposal (second_state has the malicious proposal in its blocktree)
        let cp2 = correct_proposal_gen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = cp2.destructure();
        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &mut valset,
        );
        let res = cmds2.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    blockid: BlockId(_),
                }
            )
        });
        assert!(res.is_some());

        // first_state has the correct block in its blocktree, so it should not request anything
        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
        );
        let res = cmds1.into_iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    blockid: BlockId(_),
                }
            )
        });
        assert!(res.is_none());

        // next correct proposal is created and we send it to the first two states.
        let cp3 = correct_proposal_gen.next_proposal(&keys, &mut valset, &Default::default());
        let (author, _, verified_message) = cp3.destructure();
        let cmds2 = second_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message.clone(),
            FullTransactionList(Vec::new()),
            &mut valset,
        );
        let cmds1 = first_state.handle_proposal_message_full::<Sha256Hash, _>(
            author,
            verified_message,
            FullTransactionList(Vec::new()),
            &mut valset,
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
    }
}
