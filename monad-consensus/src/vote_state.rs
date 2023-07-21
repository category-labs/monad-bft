use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use monad_consensus_types::{
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::{SignatureBuilder, SignatureCollection},
    validation::Hasher,
};
use monad_types::{Hash, NodeId, Round};
use monad_validator::{
    validator_property::ValidatorSetPropertyType, validator_set::ValidatorSetType,
};

use crate::messages::message::VoteMessage;

// accumulate votes and create a QC if enough votes are received
// only one QC should be created in a round using the first supermajority of votes received
// At the end of a round, older rounds can be cleaned up
#[derive(Debug)]
pub struct VoteState<SCT: SignatureCollection> {
    pending_votes: BTreeMap<Round, HashMap<Hash, (SignatureBuilder<SCT>, HashSet<NodeId>)>>,
    qc_created: BTreeSet<Round>,
}

impl<SCT: SignatureCollection> Default for VoteState<SCT> {
    fn default() -> Self {
        VoteState {
            pending_votes: BTreeMap::new(),
            qc_created: BTreeSet::new(),
        }
    }
}

impl<SCT> VoteState<SCT>
where
    SCT: SignatureCollection,
{
    #[must_use]
    pub fn process_vote<H: Hasher, VT: ValidatorSetType, VPT: ValidatorSetPropertyType>(
        &mut self,
        author: &NodeId,
        v: &VoteMessage<SCT>,
        validators: &VT,
        validators_property: &VPT,
    ) -> Option<QuorumCertificate<SCT>> {
        let round = v.vote.vote_info.round;

        if self.qc_created.contains(&round) {
            return None;
        }

        if H::hash_object(&v.vote.vote_info) != v.vote.ledger_commit_info.vote_info_hash {
            // TODO: collect author for evidence?
            return None;
        }

        let vote_idx = H::hash_object(&v.vote.ledger_commit_info);

        let round_pending_votes = self.pending_votes.entry(round).or_insert(HashMap::new());
        let pending_entry = round_pending_votes
            .entry(vote_idx)
            .or_insert((SignatureBuilder::new(), HashSet::new()));

        match validators_property.get_index(author) {
            Some(id) => {
                pending_entry.0.add_signature(id, v.sig);
                pending_entry.1.insert(*author);
            }
            None => return None,
        }

        if validators.has_super_majority_votes(&pending_entry.1) {
            assert!(!self.qc_created.contains(&round));
            let signature_collection = SCT::new(
                pending_entry.0.clone(),
                validators_property.get_voting_list(),
                vote_idx.as_ref(),
            )
            .expect("failed to create a signature collection"); // FIXME: collect slashing evidence
            let qc = QuorumCertificate::<SCT>::new(
                QcInfo {
                    vote: v.vote.vote_info,
                    ledger_commit: v.vote.ledger_commit_info,
                },
                signature_collection,
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
    use monad_consensus_types::{
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        signature::{SignatureCollection, SignatureCollectionKeyPairType},
        validation::{Hasher, Sha256Hash},
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{secp256k1::SecpSignature, GenericKeyPair, GenericSignature};
    use monad_testutil::{
        signing::{get_key, *},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};
    use monad_validator::{
        validator_property::{ValidatorSetProperty, ValidatorSetPropertyType},
        validator_set::{ValidatorSet, ValidatorSetType},
    };

    use super::VoteState;
    use crate::messages::message::VoteMessage;

    type SignatureCollectionType = MultiSig<SecpSignature>;
    type HasherType = Sha256Hash;

    fn create_signed_vote_message<H: Hasher, SCT: SignatureCollection>(
        keypair: &SignatureCollectionKeyPairType<SCT>,
        vote_round: Round,
    ) -> VoteMessage<SCT> {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: vote_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Default::default()), &vi);

        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let vote_hash = H::hash_object(&vote);
        let sig = <SCT::SignatureType as GenericSignature>::sign(vote_hash.as_ref(), keypair);

        VoteMessage::<SCT> { vote, sig }
    }

    #[test]
    fn clean_older_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (_, _, votekeys, valset, vprop) =
            create_keys_w_validators::<SignatureCollectionType>(4);

        // add one vote for rounds 0-3
        for i in 0..4 {
            let svm = create_signed_vote_message::<HasherType, SignatureCollectionType>(
                &votekeys[0],
                Round(i.try_into().unwrap()),
            );
            let author = NodeId(votekeys[0].pubkey());
            let _qc = votestate.process_vote::<Sha256Hash, _, _>(&author, &svm, &valset, &vprop);
        }

        assert_eq!(votestate.pending_votes.len(), 4);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for key in votekeys.iter().take(4) {
            let svm =
                create_signed_vote_message::<HasherType, SignatureCollectionType>(key, Round(4));
            let author = NodeId(votekeys[0].pubkey());
            let _qc = votestate.process_vote::<Sha256Hash, _, _>(&author, &svm, &valset, &vprop);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 0);
    }

    #[test]
    fn handle_future_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (_, _, votekeys, valset, vprop) =
            create_keys_w_validators::<SignatureCollectionType>(4);

        // add one vote for rounds 0-3 and 5-8
        for i in 0..4 {
            let svm = create_signed_vote_message::<HasherType, SignatureCollectionType>(
                &votekeys[0],
                Round(i.try_into().unwrap()),
            );
            let author = NodeId(votekeys[0].pubkey());
            let _qc = votestate.process_vote::<Sha256Hash, _, _>(&author, &svm, &valset, &vprop);
        }

        for i in 5..9 {
            let svm = create_signed_vote_message::<HasherType, SignatureCollectionType>(
                &votekeys[0],
                Round(i.try_into().unwrap()),
            );
            let author = NodeId(votekeys[0].pubkey());
            let _qc = votestate.process_vote::<Sha256Hash, _, _>(&author, &svm, &valset, &vprop);
        }

        assert_eq!(votestate.pending_votes.len(), 8);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for key in votekeys.iter().take(4) {
            let svm =
                create_signed_vote_message::<HasherType, SignatureCollectionType>(key, Round(4));
            let author = NodeId(key.pubkey());
            let _qc = votestate.process_vote::<Sha256Hash, _, _>(&author, &svm, &valset, &vprop);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 4);
    }

    #[test]
    fn vote_idx_doesnt_match() {
        let mut vote_state = VoteState::<SignatureCollectionType>::default();
        let keypair = get_key(6);
        let blskeypair = get_key_bls(6);
        let val = (NodeId(keypair.pubkey()), Stake(1), blskeypair.pubkey());

        let vset = ValidatorSet::new(vec![(val.0, val.1)]).unwrap();
        let vprop = ValidatorSetProperty::new(vec![(val.0, val.2)]).unwrap();

        let mut vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(Some(Hash([0xad_u8; 32])), &vi),
        };

        let vote_hash = Sha256Hash::hash_object(&vote);
        let vote_sig = keypair.sign(vote_hash.as_ref());

        let vm = VoteMessage::<SignatureCollectionType> {
            vote,
            sig: vote_sig,
        };

        let author = NodeId(keypair.pubkey());

        // add a valid vote message
        let _ = vote_state.process_vote::<Sha256Hash, _, _>(&author, &vm, &vset, &vprop);
        assert_eq!(vote_state.pending_votes.len(), 1);

        // pretend a qc was not created so we can add more votes without reseting the vote state
        vote_state.qc_created.clear();

        // add an invalid vote message (the vote_info doesn't match what created the ledger_commit_info)
        vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(5),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(4),
        };

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let invalid_vote = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(
                Some(Hash([0xae_u8; 32])),
                &vi2,
            ),
        };

        let invalid_vote_hash = Sha256Hash::hash_object(&invalid_vote);
        let invalid_vote_sig = keypair.sign(invalid_vote_hash.as_ref());
        let invalid_vm = VoteMessage::<SignatureCollectionType> {
            vote: invalid_vote,
            sig: invalid_vote_sig,
        };
        let invalid_vm_author = NodeId(keypair.pubkey());

        let _ = vote_state.process_vote::<Sha256Hash, _, _>(
            &invalid_vm_author,
            &invalid_vm,
            &vset,
            &vprop,
        );

        // confirms the invalid vote message was not added to pending votes
        assert_eq!(vote_state.pending_votes.len(), 1);
    }

    #[test]
    fn duplicate_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (_, _, votekeys, valset, vprop) =
            create_keys_w_validators::<SignatureCollectionType>(4);

        // create a vote for round 0 from one node, but add it supermajority number of times
        // this should not result in QC creation
        let svm = create_signed_vote_message::<HasherType, SignatureCollectionType>(
            &votekeys[0],
            Round(0),
        );
        let author = NodeId(votekeys[0].pubkey());

        for _ in 0..4 {
            let qc = votestate.process_vote::<Sha256Hash, _, _>(&author, &svm, &valset, &vprop);
            assert!(qc.is_none());
        }
    }
}
