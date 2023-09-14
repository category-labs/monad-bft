use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use monad_consensus_types::{
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validation::Hasher,
    voting::ValidatorMapping,
};
use monad_types::{Hash, NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;

use crate::messages::message::VoteMessage;

// accumulate votes and create a QC if enough votes are received
// only one QC should be created in a round using the first supermajority of votes received
// At the end of a round, older rounds can be cleaned up
#[derive(Debug)]
pub struct VoteState<SCT: SignatureCollection> {
    pending_votes:
        BTreeMap<Round, HashMap<Hash, (Vec<(NodeId, SCT::SignatureType)>, HashSet<NodeId>)>>,
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
    pub fn process_vote<H: Hasher, VT: ValidatorSetType>(
        &mut self,
        author: &NodeId,
        vote_msg: &VoteMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Option<QuorumCertificate<SCT>> {
        let vote = vote_msg.vote;
        let round = vote_msg.vote.vote_info.round;

        if self.qc_created.contains(&round) {
            return None;
        }

        if H::hash_object(&vote.vote_info) != vote.ledger_commit_info.vote_info_hash {
            // TODO: collect author for evidence?
            return None;
        }

        let vote_idx = H::hash_object(&vote);

        let round_pending_votes = self.pending_votes.entry(round).or_default();
        let pending_entry = round_pending_votes
            .entry(vote_idx)
            .or_insert((Vec::new(), HashSet::new()));

        pending_entry.0.push((*author, vote_msg.sig));
        pending_entry.1.insert(*author);

        if validators.has_super_majority_votes(&pending_entry.1) {
            assert!(!self.qc_created.contains(&round));
            let signature_collection = SCT::new(
                pending_entry.0.clone(),
                validator_mapping,
                vote_idx.as_ref(),
            )
            .expect("failed to create a signature collection"); // FIXME: collect slashing evidence
            let qc = QuorumCertificate::<SCT>::new::<H>(
                QcInfo {
                    vote: vote.vote_info,
                    ledger_commit: vote.ledger_commit_info,
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
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        validation::Sha256Hash,
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_testutil::{
        signing::{get_key, *},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use super::VoteState;
    use crate::{messages::message::VoteMessage, validation::signing::Verified};

    type SignatureType = SecpSignature;
    type SignatureCollectionType = MultiSig<SecpSignature>;

    fn create_signed_vote_message<SCT: SignatureCollection>(
        keypair: &KeyPair,
        certkeypair: &SignatureCollectionKeyPairType<SCT>,
        vote_round: Round,
    ) -> Verified<SecpSignature, VoteMessage<SCT>> {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: vote_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Default::default()), &vi);

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let vm = VoteMessage::new::<Sha256Hash>(v, certkeypair);

        Verified::new::<Sha256Hash>(vm, keypair)
    }

    #[test]
    fn clean_older_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (keys, cert_keys, valset, val_map) =
            create_keys_w_validators::<SignatureCollectionType>(4);

        // add one vote for rounds 0-3
        for i in 0..4 {
            let svm = create_signed_vote_message::<SignatureCollectionType>(
                &keys[0],
                &cert_keys[0],
                Round(i.try_into().unwrap()),
            );
            let _qc =
                votestate.process_vote::<Sha256Hash, _>(svm.author(), &*svm, &valset, &val_map);
        }

        assert_eq!(votestate.pending_votes.len(), 4);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for (key, certkey) in keys.iter().zip(cert_keys.iter()).take(4) {
            let svm = create_signed_vote_message::<SignatureCollectionType>(key, certkey, Round(4));
            let _qc =
                votestate.process_vote::<Sha256Hash, _>(svm.author(), &*svm, &valset, &val_map);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 0);
    }

    #[test]
    fn handle_future_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (keys, cert_keys, valset, vmap) =
            create_keys_w_validators::<SignatureCollectionType>(4);

        // add one vote for rounds 0-3 and 5-8
        for i in 0..4 {
            let svm =
                create_signed_vote_message(&keys[0], &cert_keys[0], Round(i.try_into().unwrap()));
            let _qc = votestate.process_vote::<Sha256Hash, _>(svm.author(), &*svm, &valset, &vmap);
        }

        for i in 5..9 {
            let svm =
                create_signed_vote_message(&keys[0], &cert_keys[0], Round(i.try_into().unwrap()));
            let _qc = votestate.process_vote::<Sha256Hash, _>(svm.author(), &*svm, &valset, &vmap);
        }

        assert_eq!(votestate.pending_votes.len(), 8);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for (key, certkey) in keys.iter().zip(cert_keys.iter().take(4)) {
            let svm = create_signed_vote_message::<SignatureCollectionType>(key, certkey, Round(4));
            let _qc = votestate.process_vote::<Sha256Hash, _>(svm.author(), &*svm, &valset, &vmap);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 4);
    }

    #[test]
    fn vote_idx_doesnt_match() {
        let mut vote_state = VoteState::<SignatureCollectionType>::default();
        let keypair = get_key(6);
        let cert_key = get_certificate_key::<SignatureCollectionType>(6);
        let node_id = NodeId(keypair.pubkey());

        let vset = ValidatorSet::new(vec![(node_id, Stake(1))]).unwrap();
        let vmap = ValidatorMapping::new(vec![(node_id, cert_key.pubkey())]);

        let mut vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(Some(Hash([0xad_u8; 32])), &vi),
        };

        let vm = VoteMessage::new::<Sha256Hash>(v, &cert_key);

        let svm = Verified::<SignatureType, _>::new::<Sha256Hash>(vm, &keypair);

        // add a valid vote message
        let _ = vote_state.process_vote::<Sha256Hash, _>(svm.author(), &*svm, &vset, &vmap);
        assert_eq!(vote_state.pending_votes.len(), 1);

        // pretend a qc was not created so we can add more votes without reseting the vote state
        vote_state.qc_created.clear();

        // add an invalid vote message (the vote_info doesn't match what created the ledger_commit_info)
        vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(5),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(4),
            seq_num: 0,
        };

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let invalid_vote = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(
                Some(Hash([0xae_u8; 32])),
                &vi2,
            ),
        };
        let invalid_vm = VoteMessage::new::<Sha256Hash>(invalid_vote, &cert_key);

        let invalid_svm = Verified::<SignatureType, _>::new::<Sha256Hash>(invalid_vm, &keypair);
        let _ = vote_state.process_vote::<Sha256Hash, _>(
            invalid_svm.author(),
            &*invalid_svm,
            &vset,
            &vmap,
        );

        // confirms the invalid vote message was not added to pending votes
        assert_eq!(vote_state.pending_votes.len(), 1);
    }

    #[test]
    fn duplicate_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<SignatureCollectionType>(4);

        // create a vote for round 0 from one node, but add it supermajority number of times
        // this should not result in QC creation
        let svm = create_signed_vote_message(&keys[0], &certkeys[0], Round(0));
        let (author, _sig, msg) = svm.destructure();

        for _ in 0..4 {
            let qc = votestate.process_vote::<Sha256Hash, _>(&author, &msg, &valset, &vmap);
            assert!(qc.is_none());
        }
    }
}
