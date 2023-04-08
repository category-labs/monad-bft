use std::collections::HashMap;

use monad_types::{Hash, NodeId};
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
// At the end of a round, this state should be reset.
pub struct VoteState<T>
where
    T: SignatureCollection,
{
    pending_vote_sigs: HashMap<Hash, T>,
    pending_vote_keys: HashMap<Hash, Vec<NodeId>>,
    qc_created: bool,
}

impl<T> VoteState<T>
where
    T: SignatureCollection,
{
    #[must_use]
    pub fn process_vote<V: LeaderElection, H: Hasher>(
        &mut self,
        v: &Verified<VoteMessage>,
        validators: &ValidatorSet<V>,
    ) -> Option<QuorumCertificate<T>> {
        if self.qc_created {
            return None;
        }

        let vote_idx = H::hash_object(&v.0.obj);
        let sigs = self.pending_vote_sigs.entry(vote_idx).or_insert(T::new());
        sigs.add_signature(v.0.author_signature.clone());

        self.pending_vote_keys
            .entry(vote_idx)
            .or_default()
            .push(v.0.author);

        let pubkeys = &self.pending_vote_keys[&vote_idx];

        if validators.has_super_majority_votes(pubkeys) {
            assert!(self.qc_created == false);
            let qc = QuorumCertificate::<T>::new(
                QcInfo {
                    vote: v.0.obj.vote_info,
                    ledger_commit: v.0.obj.ledger_commit_info,
                },
                sigs.clone(),
            );
            self.qc_created = true;
            return Some(qc);
        }

        None
    }

    pub fn new() -> Self {
        VoteState {
            pending_vote_sigs: HashMap::new(),
            pending_vote_keys: HashMap::new(),
            qc_created: false,
        }
    }

    pub fn start_new_round(&mut self) {
        self.qc_created = false;
        self.pending_vote_sigs.clear();
        self.pending_vote_keys.clear();
    }
}
