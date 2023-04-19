use std::collections::{BTreeMap, BTreeSet, HashMap};

use monad_types::{Hash, NodeId, Round};
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSet};

use crate::{
    types::{
        message::VoteMessage,
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature::SignatureCollection,
    },
    validation::{hashing::Hasher, signing::Verified},
};

// accumulate votes and create a QC if enough votes are received
// only one QC should be created in a round using the first supermajority of votes received
// At the end of a round, older rounds can be cleaned up
pub struct VoteState<T> {
    pending_votes: BTreeMap<Round, HashMap<Hash, (T, Vec<NodeId>)>>,
    qc_created: BTreeSet<Round>,
}

impl<T> Default for VoteState<T> {
    fn default() -> Self {
        VoteState {
            pending_votes: BTreeMap::new(),
            qc_created: BTreeSet::new(),
        }
    }
}

impl<T> VoteState<T>
where
    T: SignatureCollection,
{
    #[must_use]
    pub fn process_vote<V: LeaderElection, H: Hasher>(
        &mut self,
        v: &Verified<T::SignatureType, VoteMessage>,
        validators: &ValidatorSet<V>,
    ) -> Option<QuorumCertificate<T>> {
        let round = v.vote_info.round;

        if self.qc_created.contains(&round) {
            return None;
        }

        let vote_idx = H::hash_object(&v.ledger_commit_info);

        let round_pending_votes = self.pending_votes.entry(round).or_insert(HashMap::new());
        let pending_entry = round_pending_votes
            .entry(vote_idx)
            .or_insert((T::new(), Vec::new()));

        pending_entry.0.add_signature(*v.author_signature());
        pending_entry.1.push(*v.author());

        if validators.has_super_majority_votes(&pending_entry.1) {
            assert!(!self.qc_created.contains(&round));
            let qc = QuorumCertificate::<T>::new(
                QcInfo {
                    vote: v.vote_info,
                    ledger_commit: v.ledger_commit_info,
                },
                pending_entry.0.clone(),
            );
            self.qc_created.insert(round);
            return Some(qc);
        }

        None
    }

    pub fn start_new_round(&mut self, new_round: Round) {
        self.qc_created.retain(|k| *k >= new_round);
        self.pending_votes.retain(|k, _| *k >= new_round);
    }
}

#[cfg(test)]
mod test {
    use crate::signatures::aggregate_signature::AggregateSignatures;
    use crate::types::ledger::LedgerCommitInfo;
    use crate::types::message::VoteMessage;
    use crate::types::voting::VoteInfo;
    use crate::validation::hashing::Sha256Hash;
    use crate::validation::signing::Verified;
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_testutil::signing::*;
    use monad_testutil::validators::MockLeaderElection;
    use monad_types::{BlockId, Round};
    use monad_validator::validator::Validator;
    use monad_validator::validator_set::ValidatorSet;

    use super::VoteState;

    fn create_valset(num_nodes: u32) -> (Vec<KeyPair>, ValidatorSet<MockLeaderElection>) {
        let keys = create_keys(num_nodes);

        let mut nodes = Vec::new();
        for i in 0..num_nodes {
            nodes.push(Validator {
                pubkey: keys[i as usize].pubkey().clone(),
                stake: 1,
            });
        }

        let valset = ValidatorSet::<MockLeaderElection>::new(nodes).unwrap();
        (keys, valset)
    }

    fn create_signed_vote_message(
        keypair: &KeyPair,
        vote_round: Round,
    ) -> Verified<SecpSignature, VoteMessage> {
        let vi = VoteInfo {
            id: BlockId([0x00_u8; 32]),
            round: vote_round,
            parent_id: BlockId([0x00_u8; 32]),
            parent_round: Round(0),
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Default::default()), &vi);

        let vm = VoteMessage {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let svm = Verified::new::<Sha256Hash>(vm, &keypair);

        svm
    }

    #[test]
    fn clean_older_votes() {
        let mut votestate = VoteState::<AggregateSignatures<SecpSignature>>::default();
        let (keys, valset) = create_valset(4);

        // add one vote for rounds 0-3
        for i in 0..4 {
            let svm = create_signed_vote_message(&keys[0], Round(i.try_into().unwrap()));
            let _qc = votestate.process_vote::<_, Sha256Hash>(&svm, &valset);
        }

        assert_eq!(votestate.pending_votes.len(), 4);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for i in 0..4 {
            let svm = create_signed_vote_message(&keys[i], Round(4));
            let _qc = votestate.process_vote::<_, Sha256Hash>(&svm, &valset);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 0);
    }

    #[test]
    fn handle_future_votes() {
        let mut votestate = VoteState::<AggregateSignatures<SecpSignature>>::default();
        let (keys, valset) = create_valset(4);

        // add one vote for rounds 0-3 and 5-8
        for i in 0..4 {
            let svm = create_signed_vote_message(&keys[0], Round(i.try_into().unwrap()));
            let _qc = votestate.process_vote::<_, Sha256Hash>(&svm, &valset);
        }

        for i in 5..9 {
            let svm = create_signed_vote_message(&keys[0], Round(i.try_into().unwrap()));
            let _qc = votestate.process_vote::<_, Sha256Hash>(&svm, &valset);
        }

        assert_eq!(votestate.pending_votes.len(), 8);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for i in 0..4 {
            let svm = create_signed_vote_message(&keys[i], Round(4));
            let _qc = votestate.process_vote::<_, Sha256Hash>(&svm, &valset);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 4);
    }
}
