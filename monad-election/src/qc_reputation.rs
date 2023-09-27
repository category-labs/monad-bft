use std::collections::{btree_map::Entry, BTreeMap, HashSet};

use monad_consensus_types::{
    block::FullBlock,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validation::Hasher,
    voting::ValidatorMapping,
};
use monad_types::{NodeId, Round, Stake};
use monad_validator::validator_set::ValidatorSetType;
use rand::distributions::{Distribution, WeightedIndex};
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};

use super::leader_election::{ElectionInfo, LeaderElection};

struct QCReputationCache {
    // TODO: convert to bit map
    participants: HashSet<NodeId>,
    parent_round: Round,
    author: NodeId,
}

pub struct QCReputation {
    // consist of Block, and a potentially cached set of participants.
    qc_cache: BTreeMap<Round, QCReputationCache>,
    leader_cache: BTreeMap<Round, Option<NodeId>>,
    total_stake: Stake,

    // MUST requirements
    /// multiple of (f) leader that should be banned.
    /// for example, 1.0 means minimum of f out of 3f+1
    /// stake worth of leader must be banned
    leader_ban_stake: i64,
    // amount of block we should process in order to select recent participants
    window_size: u64,
    // amount of round to delay before leader election comes in effect.
    leader_delay_round: Round,

    // OPTIONAL requirements
    /// multiple of (f) candidate needed to form a reputable candidate
    /// for example, 1.0 means minimum of f out of 3f+1
    /// stake worth of candidate must exists to trigger QC based leader election
    min_candidate_stake: Option<i64>,
    // maximum amount of round before observed qc is no longer valid.
    max_observation_round: Option<Round>,
}
enum QCElection {
    CanTry,  // MUST req are satisfied
    Unknown, // leader unknown due to lack of info
    Failed,  // MUST req are not satisfied
}

// record the environment for qc election
struct QCElectionEnv {
    observed_qc_round: Round,
    // TODO: convert to bit map
    banned_leader: HashSet<NodeId>,
    pre_ban_candidates: HashSet<NodeId>,
}

impl QCReputation {
    pub fn new<SCT, VT>(
        leader_ban_stake: i64,
        window_size: u64,
        leader_delay_round: Round,
        min_candidate_stake: Option<i64>,
        max_observation_round: Option<Round>,
        validator_set: &VT,
    ) -> Self
    where
        SCT: SignatureCollection,
        VT: ValidatorSetType,
    {
        Self {
            total_stake: validator_set.get_total_stake(),
            qc_cache: BTreeMap::new(),
            leader_cache: BTreeMap::new(),
            leader_ban_stake,
            window_size,
            leader_delay_round,
            min_candidate_stake,
            max_observation_round,
        }
    }

    fn cache_commits<H, SCT>(
        &mut self,
        blocks: &[FullBlock<SCT>],
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) where
        H: Hasher,
        SCT: SignatureCollection,
    {
        for full_b in blocks {
            let b = full_b.get_block();
            let participants = b.qc.get_participants::<H>(validator_mapping);
            if let Entry::Vacant(entry) = self.qc_cache.entry(b.round) {
                entry.insert(QCReputationCache {
                    participants,
                    author: b.author,
                    parent_round: b.qc.info.vote.round,
                });
            }
        }
    }

    // the property of sliding window (and 2 pointer)
    // allow us to prune extra info
    fn prune_qc_cache(&mut self, r: Round) {
        self.qc_cache.retain(|k, v| k >= &r)
    }

    /**
     * There is an inherit problem with qc based leader election,
     * producer of qc naturally have the power to censor nodes by not adding
     * certain node's vote within their QC.
     *
     * Following measures are taken to reduce the impact of such temper,
     * some where already mentioned in diem, while others are additional information
     *
     * MUST req before reaching a valid world view
     * 1.   the most recent QC for leader election is ignored, to avoid active tempering of next leader
     * 2.   We ban leader up to LEADER_BAN_STAKE * f stake
     * 3.   At least WINDOW_SIZE block needs to be looked at
     *
     * OPTIONAL req such that valid world view can evaluate to a leader
     * 1. you need leader candidates to be at least MIN_CANDIDATE_STAKE * f + 1 in stake
     * 2. earliest observed block must be enclosed within MAX_OBSERVE_ROUND_LIMIT of the current round
     *
     *
     * round number is used as seed for random selection, potentially with VRF to prevent tempering (but not collusion)
     */

    fn qc_elect<H, SCT, VT>(
        &self,
        qc: &QuorumCertificate<SCT>,
        validator_set: &VT,
    ) -> (QCElection, QCElectionEnv)
    where
        H: Hasher,
        SCT: SignatureCollection,
        VT: ValidatorSetType,
    {
        // NOTE: Diem's paper has a potential bug here where they keep jumping
        // using parent_round, but you only really need to start with parent_round
        // to ensure such block is committed. thus current_qc_round is updated using
        // only qc.info.vote.round in the loop
        let mut current_qc_round = qc.info.vote.parent_round;
        let mut observed_qc_round = current_qc_round;

        let mut banned_leader: HashSet<NodeId> = HashSet::new();
        let mut banned_stake = Stake(0);

        let mut pre_ban_candidates = HashSet::new();
        let mut block_cnt = 0;

        let f = (self.total_stake.0 - 1) / 3;

        while banned_stake < Stake(f * self.leader_ban_stake) || block_cnt < self.window_size {
            if current_qc_round == Round(0) {
                // genesis, no more update possible, election fall back to round robin
                return (
                    QCElection::Failed,
                    QCElectionEnv {
                        observed_qc_round,
                        banned_leader,
                        pre_ban_candidates,
                    },
                );
            }
            let Some(QCReputationCache {
                participants,
                author,
                parent_round,
            }) = self.qc_cache.get(&current_qc_round)
            else {
                // system is aware there exists a reputation leader, but not enough info to determine it.
                return (
                    QCElection::Unknown,
                    QCElectionEnv {
                        observed_qc_round,
                        banned_leader,
                        pre_ban_candidates,
                    },
                );
            };

            if block_cnt < self.window_size {
                pre_ban_candidates.extend(participants.iter());
            }

            // loop transition update
            if banned_stake < Stake(f * self.leader_ban_stake) && banned_leader.insert(*author) {
                banned_stake += validator_set.get_stake(author);
            }

            observed_qc_round = current_qc_round;
            current_qc_round = *parent_round;
            block_cnt += 1;
        }
        (
            QCElection::CanTry,
            QCElectionEnv {
                observed_qc_round,
                banned_leader,
                pre_ban_candidates,
            },
        )
    }

    fn attempt_reputation_election<VT>(
        &mut self,
        pre_ban_candidates: HashSet<NodeId>,
        banned_leader: HashSet<NodeId>,
        validator_set: &VT,
        current_round: Round,
        observed_round: Round,
    ) where
        VT: ValidatorSetType,
    {
        let f = (self.total_stake.0 - 1) / 3;
        let (candidates, weights, candidates_total_stake) =
            pre_ban_candidates
                .iter()
                .fold((vec![], vec![], Stake(0)), |acc, addr| {
                    if !banned_leader.contains(addr) {
                        let (mut nodes, mut weights, mut total_stake) = acc;
                        nodes.push(addr);
                        let stake = validator_set.get_stake(addr);
                        weights.push(stake.0);
                        total_stake += stake;
                        return (nodes, weights, total_stake);
                    }
                    acc
                });

        // optional filters
        if let Some(min_candidate_stake) = self.min_candidate_stake {
            if candidates_total_stake < Stake(f * min_candidate_stake + 1) {
                return;
            }
        }

        if let Some(max_observation_round) = self.max_observation_round {
            if (observed_round + max_observation_round) > current_round {
                return;
            }
        }

        let leader = candidates[self.generate_weighted_leader(current_round, &weights)];
        self.insert_leader(current_round, Some(*leader));
    }

    fn generate_weighted_leader(&self, round: Round, weights: &Vec<i64>) -> usize {
        // TODO: maybe add VRF to prevent recent proposer from tempering? but it doesn't stop collusion
        let mut rng = ChaChaRng::seed_from_u64(round.0);
        let dist = WeightedIndex::new(weights).unwrap();
        dist.sample(&mut rng)
    }

    fn insert_leader(&mut self, current_round: Round, maybe_leader: Option<NodeId>) {
        self.leader_cache
            .insert(current_round + self.leader_delay_round, maybe_leader);
    }

    fn is_leader_determined(&mut self, current_round: Round) -> bool {
        match self
            .leader_cache
            .get(&(current_round + self.leader_delay_round))
        {
            Some(entry) => entry.is_some(),
            None => false,
        }
    }
}

impl LeaderElection for QCReputation {
    fn submit_election_info<H: Hasher, SCT, VT>(&mut self, info: ElectionInfo<SCT, VT>)
    where
        H: Hasher,
        SCT: SignatureCollection,
        VT: ValidatorSetType,
    {
        match info {
            ElectionInfo::Commits(blocks, validator_mapping) => {
                self.cache_commits::<H, SCT>(blocks, validator_mapping)
            }
            ElectionInfo::QC(qc, current_round, validator_set) => {
                let qc_r = qc.info.vote.round;
                let qc_parent_r = qc.info.vote.parent_round;
                if !(self.is_leader_determined(current_round))
                    && (qc_parent_r + Round(1) == qc_r)
                    && (qc_r + Round(1) == current_round)
                {
                    let (elect, env) = self.qc_elect::<H, SCT, VT>(qc, validator_set);
                    match elect {
                        QCElection::CanTry => {
                            self.prune_qc_cache(env.observed_qc_round);
                            self.attempt_reputation_election::<VT>(
                                env.pre_ban_candidates,
                                env.banned_leader,
                                validator_set,
                                current_round,
                                env.observed_qc_round,
                            );
                        }
                        QCElection::Unknown => {
                            self.insert_leader(current_round, None);
                        }
                        QCElection::Failed => {}
                    }
                }
            }
        };
    }

    fn get_leader<VT>(&self, round: Round, valset: &VT) -> Option<NodeId>
    where
        VT: ValidatorSetType,
    {
        if let Some(leader) = self.leader_cache.get(&round) {
            return *leader;
        }
        let index = self.generate_weighted_leader(
            round,
            &valset.get_stakes().iter().map(|s| s.0).collect::<Vec<_>>(),
        );

        Some(valset.get_list()[index])
    }
}

#[cfg(test)]
mod test {
    // some default test constant
    const LEADER_BAN_STAKE: i64 = 0;
    const WINDOW_SIZE: u64 = 1;
    const LEADER_DELAY_ROUND: Round = Round(1);
    use core::panic;
    use std::collections::HashSet;

    use monad_consensus_types::{
        block::{Block, BlockType, FullBlock},
        ledger::LedgerCommitInfo,
        payload::{ExecutionArtifacts, FullTransactionList, Payload, TransactionList},
        quorum_certificate::{QcInfo, QuorumCertificate},
        transaction_validator::MockValidator,
        validation::Sha256Hash,
        voting::{ValidatorMapping, VoteInfo},
    };
    use monad_crypto::secp256k1::{KeyPair, PubKey};
    use monad_testutil::{
        signing::{get_genesis_config, MockSignatures},
        validators::create_keys_w_validators,
    };
    use monad_types::{NodeId, Round, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use super::QCReputation;
    use crate::{
        leader_election::{ElectionInfo, LeaderElection},
        qc_reputation::{QCElection, QCReputationCache},
    };

    type H = Sha256Hash;
    type SC = MockSignatures;
    type QC = QuorumCertificate<SC>;
    type VT = ValidatorSet;
    type TV = MockValidator;

    fn prepare_block(
        prev_block: &FullBlock<SC>,
        author: NodeId,
        r: Round,
        pubkeys: &[PubKey],
    ) -> FullBlock<SC> {
        // prepare a block with qc pointing to previous
        let payload = Payload {
            txns: TransactionList(vec![]),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
        };

        let prev_block = prev_block.get_block();
        let qc = QC::new::<Sha256Hash>(
            QcInfo {
                vote: VoteInfo {
                    id: prev_block.get_id(),
                    round: prev_block.round,
                    parent_id: prev_block.get_parent_id(),
                    parent_round: prev_block.qc.info.vote.round,
                    seq_num: 0, // sequence number is irrelevant for this module
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(pubkeys),
        );
        FullBlock::from_block(
            Block::new::<Sha256Hash>(author, r, &payload, &qc),
            FullTransactionList::default(),
            &TV::default(),
        )
        .unwrap()
    }

    fn setup(
        num_validators: u32,
    ) -> (
        Vec<KeyPair>,
        Vec<KeyPair>,
        ValidatorSet,
        ValidatorMapping<KeyPair>,
        FullBlock<MockSignatures>,
        MockSignatures,
    ) {
        let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<SC>(num_validators);
        let voting_keys = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(cert_keys.iter())
            .collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) =
            get_genesis_config::<Sha256Hash, SC, TV>(voting_keys.iter(), &valmap, &TV::default());
        (keys, cert_keys, valset, valmap, genesis_block, genesis_sigs)
    }

    fn verify_leader_is_dft(qc_election: &QCReputation, round: Round, valset: &ValidatorSet) {
        let maybe_leader = qc_election.get_leader(round, valset);
        assert!(maybe_leader.is_some());
        assert!(valset.is_member(&maybe_leader.unwrap()));
        assert!(!qc_election.leader_cache.contains_key(&round));
    }

    fn verify_leader_is_none(qc_election: &QCReputation, round: Round, valset: &ValidatorSet) {
        let maybe_leader = qc_election.get_leader(round, valset);
        assert!(maybe_leader.is_none());
        assert!(qc_election.leader_cache.contains_key(&round));
    }

    fn verify_leader_is_some(qc_election: &QCReputation, round: Round, valset: &ValidatorSet) {
        let maybe_leader = qc_election.get_leader(round, valset);
        assert!(maybe_leader.is_some());
        assert!(valset.is_member(&maybe_leader.unwrap()));
        assert!(qc_election.leader_cache.contains_key(&round));
    }

    fn verify_qc_elect_failed(elect: &QCElection) {
        match elect {
            QCElection::CanTry => panic!("qc_elect is CanTry instead of Failed"),
            QCElection::Unknown => panic!("qc_elect is Unknown instead of Failed"),
            _ => {}
        };
    }

    fn verify_qc_elect_can_try(elect: &QCElection) {
        match elect {
            QCElection::Failed => panic!("qc_elect is Failed instead of CanTry"),
            QCElection::Unknown => panic!("qc_elect is Unknown instead of CanTry"),
            _ => {}
        };
    }

    fn verify_qc_elect_unknown(elect: &QCElection) {
        match elect {
            QCElection::CanTry => panic!("qc_elect is CanTry instead of Unknown"),
            QCElection::Failed => panic!("qc_elect is Failed instead of Unknown"),
            _ => {}
        };
    }

    fn recompute_banned_stack(banned_leader: &HashSet<NodeId>, valset: &ValidatorSet) -> Stake {
        banned_leader.iter().fold(Stake(0), |mut stake, leader| {
            stake += valset.get_stake(leader);
            stake
        })
    }

    fn get_f(total_stake: Stake) -> Stake {
        Stake((total_stake.0 - 1) / 3)
    }

    #[test]
    fn test_repeat_block_insert() {
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);

        let mut qc_election =
            QCReputation::new::<SC, VT>(1, 1, LEADER_DELAY_ROUND, None, None, &valset);
        let pubkeys = keys.iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let mut blocks = vec![genesis_block.clone()];

        for i in 1..101 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                valset.get_list()[i - 1],
                Round(i as u64),
                &pubkeys,
            ))
        }

        (qc_election).submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));
        assert!(qc_election.qc_cache.len() == 101); // genesis + 100 blocks afterward

        let mut blocks_2 = vec![genesis_block];

        for i in 1..101 {
            blocks_2.push(prepare_block(
                blocks_2.last().unwrap(),
                valset.get_list()[33],
                Round(i as u64),
                &pubkeys,
            ))
        }

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks_2, &valmap));
        assert!(qc_election.qc_cache.len() == 101); // genesis + 100 blocks afterward

        for block in blocks.iter().skip(1) {
            let Some(cache) = qc_election.qc_cache.get(&block.get_block().round) else {
                panic!("block cannot be found");
            };

            assert_eq!(
                cache.participants,
                block.get_block().qc.get_participants::<H>(&valmap)
            );
            assert_eq!(cache.author, block.get_block().author);
        }
    }

    #[test]
    fn test_submit_blocks() {
        let (_, _, valset, valmap, genesis_block, _) = setup(100);

        let mut qc_election = QCReputation::new::<SC, VT>(
            LEADER_BAN_STAKE,
            WINDOW_SIZE,
            LEADER_DELAY_ROUND,
            None,
            None,
            &valset,
        );
        let mut blocks = vec![genesis_block];
        for i in 1..1001 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                valset.get_list()[0],
                Round(i),
                &[],
            ))
        }

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));
        let Some(entry) = qc_election.qc_cache.first_entry() else {
            panic!("empty block cache not suppose to happen")
        };

        let QCReputationCache {
            participants: _,
            parent_round,
            author: _,
        } = entry.get();

        assert!(*entry.key() == Round(0));
        assert_eq!(*parent_round, Round(0));

        let Some(entry) = qc_election.qc_cache.last_entry() else {
            panic!("empty block cache not suppose to happen")
        };

        let QCReputationCache {
            participants,
            parent_round,
            author: _,
        } = entry.get();

        assert!(*entry.key() == Round(1000));
        assert_eq!(*parent_round, Round(999));
    }

    #[test]
    fn test_lack_of_history() {
        // no leader ban stake requirement, only need to observe 2 round of qc
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);
        let mut qc_election =
            QCReputation::new::<SC, VT>(0, 2, LEADER_DELAY_ROUND, None, None, &valset);
        let pubkeys = keys.iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];
        for i in 1..10 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                valset.get_list()[0],
                Round(i),
                &pubkeys,
            ))
        }

        // record G, R1, into history, but nothing else.

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(
            &blocks[0..2].to_vec(),
            &valmap,
        ));
        assert!(qc_election.qc_cache.len() == 2);

        // now providing the qc for block at round 4 should not have
        // triggered any leader election
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[4].get_block().qc,
            Round(4),
            &valset,
        ));

        verify_leader_is_none(&qc_election, Round(5), &valset);

        // now we make the system aware of block 2;
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(
            &vec![blocks[2].clone()],
            &valmap,
        ));

        // resubmit again
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[4].get_block().qc,
            Round(4),
            &valset,
        ));

        verify_leader_is_some(&qc_election, Round(5), &valset);

        // repeated get will be fine
        verify_leader_is_some(&qc_election, Round(5), &valset);

        // there is no reputation leader for Round(4, 3, 2, 1, 0) because not enough qc can be observed,
        // thus falling back to round robin
        verify_leader_is_dft(&qc_election, Round(4), &valset);
        verify_leader_is_dft(&qc_election, Round(3), &valset);
        verify_leader_is_dft(&qc_election, Round(2), &valset);
        verify_leader_is_dft(&qc_election, Round(1), &valset);
        verify_leader_is_dft(&qc_election, Round(0), &valset);

        // submitting with a gap in between (missing Round(3) block)
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(
            &vec![blocks[4].clone()],
            &valmap,
        ));

        // submit should result in non, even though, first block is found.
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[6].get_block().qc,
            Round(6),
            &valset,
        ));

        verify_leader_is_none(&qc_election, Round(7), &valset);

        // submit should result in non, even though, first block is found.
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[5].get_block().qc,
            Round(5),
            &valset,
        ));

        verify_leader_is_none(&qc_election, Round(6), &valset);

        // now fix the block in between
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(
            &vec![blocks[3].clone()],
            &valmap,
        ));

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[6].get_block().qc,
            Round(6),
            &valset,
        ));

        verify_leader_is_some(&qc_election, Round(7), &valset);
        verify_leader_is_none(&qc_election, Round(6), &valset);
        // now the interesting bit, because we no longer cares about leader of round <= 7,
        // submitting qc for round 5 for example, result in unknown.

        // the cache rn only contains 2
        assert_eq!(qc_election.qc_cache.len(), 2);

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[5].get_block().qc,
            Round(5),
            &valset,
        ));
        verify_leader_is_none(&qc_election, Round(6), &valset);
        // all future leader is default
        for i in 8..1000 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }
    }

    #[test]
    fn test_leader_banning() {
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);

        // extreme case, history look back only 1 qc worth, but need to ban f stakes leader
        let mut qc_election =
            QCReputation::new::<SC, VT>(1, 1, LEADER_DELAY_ROUND, None, None, &valset);
        let pubkeys = keys.iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let candidates = keys.iter().map(|k| NodeId(k.pubkey())).collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];

        for i in 1..1001 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                candidates[(i - 1) % candidates.len()],
                Round(i as u64),
                &pubkeys,
            ))
        }

        // Cannot accumulate enough ban state within the first 33 blocks,
        // so all result should be default
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(
            &blocks[0..101].to_vec(),
            &valmap,
        ));
        assert_eq!(qc_election.qc_cache.len(), 101); // genesis + 100 blocks afterward

        for (i, block) in blocks.iter().enumerate().take(35) {
            let (res, env) = qc_election.qc_elect::<H, SC, VT>(&block.get_block().qc, &valset);
            verify_qc_elect_failed(&res);
            assert!(
                recompute_banned_stack(&env.banned_leader, &valset)
                    < get_f(qc_election.total_stake)
            );
            if i <= 2 {
                assert!(env.observed_qc_round == Round(0));
                assert!(env.pre_ban_candidates == HashSet::new());
            } else {
                assert!(env.observed_qc_round == Round(1));
                assert!(
                    env.pre_ban_candidates == HashSet::from_iter(candidates.clone().into_iter())
                );
            }
        }

        for i in 35..103_usize {
            let (res, env) = qc_election.qc_elect::<H, SC, VT>(&blocks[i].get_block().qc, &valset);
            verify_qc_elect_can_try(&res);
            assert!(
                recompute_banned_stack(&env.banned_leader, &valset)
                    >= get_f(qc_election.total_stake)
            );
            let ban_set = HashSet::from_iter(candidates[(i - 35)..(i - 2)].iter().copied());
            assert!(env.banned_leader == ban_set);
            assert!(env.pre_ban_candidates == HashSet::from_iter(candidates.clone().into_iter()));
        }
        for block in blocks.iter().skip(103) {
            let (res, _) = qc_election.qc_elect::<H, SC, VT>(&block.get_block().qc, &valset);
            verify_qc_elect_unknown(&res);
        }
    }

    #[test]
    fn test_leader_stuck_on_banning() {
        let (keys, _, valset, valmap, genesis_block, _) = setup(13);

        // if you are unable to get enough stake, you should stuck on banning
        let mut qc_election =
            QCReputation::new::<SC, VT>(1, 1, LEADER_DELAY_ROUND, None, None, &valset);
        let pubkeys = keys.iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];

        for i in 1..101 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                // only the first 3 nodes participate
                valset.get_list()[i % 3],
                Round(i as u64),
                &pubkeys,
            ))
        }

        // Cannot accumulate enough ban state within the first 33 blocks,
        // so all result should be default
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));
        assert!(qc_election.qc_cache.len() == 101); // genesis + 100 blocks afterward

        for (i, b) in blocks.iter().enumerate().take(101) {
            qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
                &b.get_block().qc,
                Round(i as u64),
                &valset,
            ));
        }

        for i in 0..300 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }
        // introduce more author
        for i in 101..111 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                valset.get_list()[i % 3 + 3],
                Round(i as u64),
                &pubkeys,
            ))
        }

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));

        for (i, block) in blocks.iter().enumerate().take(111).skip(101) {
            qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
                &block.get_block().qc,
                Round(i as u64),
                &valset,
            ));
        }
        for i in 101..103 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }
        for i in 104..112 {
            verify_leader_is_some(&qc_election, Round(i), &valset);
        }
    }

    #[test]
    fn test_leader_stuck_on_optional_min_stake() {
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);
        // if candidate doesn't reach enough stake, you cannot proceed.
        let mut qc_election =
            QCReputation::new::<SC, VT>(1, 10, LEADER_DELAY_ROUND, Some(1), None, &valset);
        // only the first 66 keys participates in qc
        // note this is obviously not a valid qc, but its for testing purpose
        let pubkeys = keys[0..66].iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let candidates = keys[0..66]
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];

        for i in 1..300 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                // only the first 3 nodes participate
                candidates[i % candidates.len()],
                Round(i as u64),
                &pubkeys,
            ))
        }

        // Cannot accumulate enough ban state within the first 33 blocks,
        // so all result should be default
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));
        assert!(qc_election.qc_cache.len() == 300); // genesis + 100 blocks afterward

        for block in blocks.iter().take(103_usize).skip(35) {
            let (res, env) = qc_election.qc_elect::<H, SC, VT>(&block.get_block().qc, &valset);
            verify_qc_elect_can_try(&res);
            assert!(
                recompute_banned_stack(&env.banned_leader, &valset)
                    >= get_f(qc_election.total_stake)
            );
            assert!(env.pre_ban_candidates == HashSet::from_iter(candidates.clone().into_iter()));
        }

        for (i, block) in blocks.iter().enumerate().take(101) {
            qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
                &blocks[i].get_block().qc,
                Round(i as u64),
                &valset,
            ));
        }

        for i in 0..300 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }
    }

    #[test]
    fn test_leader_stuck_on_optional_min_round() {
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);
        // if candidate doesn't reach enough stake, you cannot proceed.
        let mut qc_election =
            QCReputation::new::<SC, VT>(1, 10, LEADER_DELAY_ROUND, None, Some(Round(25)), &valset);
        // only the first 66 keys participates in qc
        // note this is obviously not a valid qc, but its for testing purpose
        let pubkeys = keys[0..66].iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let candidates = keys[0..66]
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];

        for i in 1..40 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                // only the first 3 nodes participate
                candidates[i % candidates.len()],
                Round(i as u64),
                &pubkeys,
            ))
        }

        for i in 100..201 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                // only the first 3 nodes participate
                candidates[i % candidates.len()],
                Round(i as u64),
                &pubkeys,
            ))
        }

        // Cannot accumulate enough ban state within the first 33 blocks,
        // so all result should be default
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));
        assert!(qc_election.qc_cache.len() == 141);

        for b in blocks.iter().take(141_usize).skip(35) {
            let (res, env) = qc_election.qc_elect::<H, SC, VT>(&b.get_block().qc, &valset);
            verify_qc_elect_can_try(&res);
            assert!(
                recompute_banned_stack(&env.banned_leader, &valset)
                    >= get_f(qc_election.total_stake)
            );
            assert!(env.pre_ban_candidates == HashSet::from_iter(candidates.clone().into_iter()));
        }

        for (i, b) in blocks.iter().enumerate().take(141) {
            qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
                &b.get_block().qc,
                Round(i as u64),
                &valset,
            ));
        }
        // min round is still fine, so you will receive result
        for i in 36..42 {
            verify_leader_is_some(&qc_election, Round(i), &valset);
        }
        // min round no longer fine, so you no longer receive result
        for i in 42..300 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }
    }
    #[test]
    fn test_round_diff_cause_qc_to_be_ignored() {
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);

        let mut qc_election =
            QCReputation::new::<SC, VT>(0, 2, LEADER_DELAY_ROUND, None, None, &valset);
        let pubkeys = keys.iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];
        for i in 1..10 {
            blocks.push(prepare_block(
                blocks.last().unwrap(),
                valset.get_list()[0],
                Round(i),
                &pubkeys,
            ))
        }

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));

        // now providing the qc for block at round 4 should not have
        // triggered any leader election
        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
            &blocks[4].get_block().qc,
            Round(6),
            &valset,
        ));
        for i in 0..40 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }

        let mut qc_election =
            QCReputation::new::<SC, VT>(0, 1, LEADER_DELAY_ROUND, None, None, &valset);
        let (keys, _, valset, valmap, genesis_block, _) = setup(100);
        let pubkeys = keys.iter().map(|k| k.pubkey()).collect::<Vec<_>>();
        let mut blocks = vec![genesis_block];
        blocks.push(prepare_block(
            blocks.last().unwrap(),
            valset.get_list()[1],
            Round(1),
            &pubkeys,
        ));
        blocks.push(prepare_block(
            blocks.last().unwrap(),
            valset.get_list()[1],
            Round(2),
            &pubkeys,
        ));
        for i in 2..10 {
            blocks.push(prepare_block(
                &blocks[1],
                valset.get_list()[0],
                Round(i),
                &pubkeys,
            ))
        }

        qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::Commits(&blocks, &valmap));

        // now providing the qc for block at round 4 should not have
        // triggered any leader election
        for (i, b) in blocks.iter().enumerate().take(10).skip(1) {
            qc_election.submit_election_info::<H, SC, VT>(ElectionInfo::QC(
                &b.get_block().qc,
                Round(i as u64),
                &valset,
            ));
        }

        for i in 0..40 {
            verify_leader_is_dft(&qc_election, Round(i), &valset);
        }
    }
}
