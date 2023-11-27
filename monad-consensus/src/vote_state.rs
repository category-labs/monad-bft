use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use monad_consensus_types::{
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};
use monad_crypto::hasher::{Hash, Hasher};
use monad_types::{NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;

use crate::messages::message::VoteMessage;

// accumulate votes and create a QC if enough votes are received
// only one QC should be created in a round using the first supermajority of votes received
// At the end of a round, older rounds can be cleaned up
#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug)]
pub enum VoteStateCommand {
    // TODO: evidence collection command
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
    ) -> (Option<QuorumCertificate<SCT>>, Vec<VoteStateCommand>) {
        let vote = vote_msg.vote;
        let round = vote_msg.vote.vote_info.round;

        let mut ret_commands = Vec::new();

        if self.qc_created.contains(&round) {
            return (None, ret_commands);
        }

        if H::hash_object(&vote.vote_info) != vote.ledger_commit_info.vote_info_hash {
            // TODO: collect author for evidence?
            return (None, ret_commands);
        }

        let vote_idx = H::hash_object(&vote);

        let round_pending_votes = self.pending_votes.entry(round).or_default();
        let pending_entry = round_pending_votes
            .entry(vote_idx)
            .or_insert((Vec::new(), HashSet::new()));

        pending_entry.0.push((*author, vote_msg.sig));
        pending_entry.1.insert(*author);

        while validators.has_super_majority_votes(&pending_entry.1) {
            assert!(!self.qc_created.contains(&round));
            match SCT::new(
                pending_entry.0.clone(),
                validator_mapping,
                vote_idx.as_ref(),
            ) {
                Ok(sigcol) => {
                    let qc = QuorumCertificate::<SCT>::new::<H>(
                        QcInfo {
                            vote: vote.vote_info,
                            ledger_commit: vote.ledger_commit_info,
                        },
                        sigcol,
                    );
                    self.qc_created.insert(round);
                    return (Some(qc), ret_commands);
                }
                Err(err) => match err {
                    SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs) => {
                        ret_commands.extend(Self::handle_invalid_vote(pending_entry, invalid_sigs));
                    }
                    _ => {
                        unreachable!("unexpected error {}", err);
                    }
                },
            }
        }

        (None, ret_commands)
    }

    #[must_use]
    fn handle_invalid_vote(
        pending_entry: &mut (Vec<(NodeId, SCT::SignatureType)>, HashSet<NodeId>),
        invalid_votes: Vec<(NodeId, SCT::SignatureType)>,
    ) -> Vec<VoteStateCommand> {
        let invalid_vote_set = invalid_votes
            .into_iter()
            .map(|(a, _)| a)
            .collect::<HashSet<_>>();
        pending_entry
            .0
            .retain(|(node_id, _)| !invalid_vote_set.contains(node_id));
        pending_entry
            .1
            .retain(|node_id| !invalid_vote_set.contains(node_id));
        // TODO: evidence
        vec![]
    }

    pub fn start_new_round(&mut self, new_round: Round) {
        self.qc_created.retain(|k| *k >= new_round);
        self.pending_votes.retain(|k, _| *k >= new_round);
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_consensus_types::{
        certificate_signature::CertificateSignature,
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        hasher::{Hash, Hasher, HasherType},
        secp256k1::{KeyPair, SecpSignature},
    };
    use monad_testutil::{
        signing::{get_key, *},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use super::VoteState;
    use crate::{messages::message::VoteMessage, validation::signing::Verified};

    type SignatureType = SecpSignature;
    type SignatureCollectionType = MultiSig<SecpSignature>;

    // FIXME: we don't need to create Verified<VoteMessage> as the vote signature
    // is now in the VoteMessage
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
            seq_num: SeqNum(0),
        };

        let lci = LedgerCommitInfo::new::<HasherType>(Some(Default::default()), &vi);

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let vm = VoteMessage::new::<HasherType>(v, certkeypair);

        Verified::new::<HasherType>(vm, keypair)
    }

    fn create_vote_message<SCT: SignatureCollection>(
        certkeypair: &SignatureCollectionKeyPairType<SCT>,
        vote_round: Round,
        valid: bool,
    ) -> VoteMessage<SCT> {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: vote_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let lci = LedgerCommitInfo::new::<HasherType>(Some(Default::default()), &vi);

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let mut vm = VoteMessage::new::<HasherType>(v, certkeypair);
        if !valid {
            let invalid_msg = b"invalid";
            vm.sig = <SCT::SignatureType as CertificateSignature>::sign(
                invalid_msg.as_ref(),
                certkeypair,
            );
        }
        vm
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
            let (_qc, cmds) =
                votestate.process_vote::<HasherType, _>(svm.author(), &*svm, &valset, &val_map);
            assert!(cmds.is_empty());
        }

        assert_eq!(votestate.pending_votes.len(), 4);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for (key, certkey) in keys.iter().zip(cert_keys.iter()).take(4) {
            let svm = create_signed_vote_message::<SignatureCollectionType>(key, certkey, Round(4));
            let _qc =
                votestate.process_vote::<HasherType, _>(svm.author(), &*svm, &valset, &val_map);
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
            let _qc = votestate.process_vote::<HasherType, _>(svm.author(), &*svm, &valset, &vmap);
        }

        for i in 5..9 {
            let svm =
                create_signed_vote_message(&keys[0], &cert_keys[0], Round(i.try_into().unwrap()));
            let _qc = votestate.process_vote::<HasherType, _>(svm.author(), &*svm, &valset, &vmap);
        }

        assert_eq!(votestate.pending_votes.len(), 8);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for (key, certkey) in keys.iter().zip(cert_keys.iter().take(4)) {
            let svm = create_signed_vote_message::<SignatureCollectionType>(key, certkey, Round(4));
            let _qc = votestate.process_vote::<HasherType, _>(svm.author(), &*svm, &valset, &vmap);
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
            seq_num: SeqNum(0),
        };

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<HasherType>(Some(Hash([0xad_u8; 32])), &vi),
        };

        let vm = VoteMessage::new::<HasherType>(v, &cert_key);

        let svm = Verified::<SignatureType, _>::new::<HasherType>(vm, &keypair);

        // add a valid vote message
        let _ = vote_state.process_vote::<HasherType, _>(svm.author(), &*svm, &vset, &vmap);
        assert_eq!(vote_state.pending_votes.len(), 1);

        // pretend a qc was not created so we can add more votes without reseting the vote state
        vote_state.qc_created.clear();

        // add an invalid vote message (the vote_info doesn't match what created the ledger_commit_info)
        vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(5),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(4),
            seq_num: SeqNum(0),
        };

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let invalid_vote = Vote {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<HasherType>(
                Some(Hash([0xae_u8; 32])),
                &vi2,
            ),
        };
        let invalid_vm = VoteMessage::new::<HasherType>(invalid_vote, &cert_key);

        let invalid_svm = Verified::<SignatureType, _>::new::<HasherType>(invalid_vm, &keypair);
        let _ = vote_state.process_vote::<HasherType, _>(
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
            let (qc, cmds) = votestate.process_vote::<HasherType, _>(&author, &msg, &valset, &vmap);
            assert!(cmds.is_empty());
            assert!(qc.is_none());
        }
    }

    #[test]
    fn invalid_votes_no_qc() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<SignatureCollectionType>(4);
        let vote_round = Round(0);

        let v0_valid = create_vote_message(&certkeys[0], vote_round, true);
        let v1_valid = create_vote_message(&certkeys[1], vote_round, true);
        let v2_invalid = create_vote_message(&certkeys[2], vote_round, false);

        let vote_idx = HasherType::hash_object(&v0_valid.vote);

        let (qc, _) = votestate.process_vote::<HasherType, _>(
            &NodeId(keys[0].pubkey()),
            &v0_valid,
            &valset,
            &vmap,
        );
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .0
                .len()
                == 1
        );
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .1
                .len()
                == 1
        );

        let (qc, _) = votestate.process_vote::<HasherType, _>(
            &NodeId(keys[1].pubkey()),
            &v1_valid,
            &valset,
            &vmap,
        );
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .0
                .len()
                == 2
        );
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .1
                .len()
                == 2
        );

        // VoteState attempts to create a QC, but failed because one of the sigs is invalid
        // doesn't have supermajority after removing the invalid
        let (qc, _) = votestate.process_vote::<HasherType, _>(
            &NodeId(keys[2].pubkey()),
            &v2_invalid,
            &valset,
            &vmap,
        );
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .0
                .len()
                == 2
        );
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .1
                .len()
                == 2
        );
    }

    #[test]
    fn invalid_votes_qc() {
        let mut votestate = VoteState::<SignatureCollectionType>::default();

        let keys = create_keys(4);
        let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

        let mut staking_list = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(std::iter::repeat(Stake(1)))
            .collect::<Vec<_>>();

        // node2 has supermajority stake by itself
        staking_list[2].1 = Stake(10);

        let voting_identity = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(certkeys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();

        let valset = ValidatorSet::new(staking_list).expect("create validator set");
        let vmap = ValidatorMapping::new(voting_identity);

        let vote_round = Round(0);

        let v0_valid = create_vote_message(&certkeys[0], vote_round, true);
        let v1_invalid = create_vote_message(&certkeys[1], vote_round, false);
        let v2_valid = create_vote_message(&certkeys[2], vote_round, true);

        let vote_idx = HasherType::hash_object(&v0_valid.vote);

        let (qc, _) = votestate.process_vote::<HasherType, _>(
            &NodeId(keys[0].pubkey()),
            &v0_valid,
            &valset,
            &vmap,
        );
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .0
                .len()
                == 1
        );
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .1
                .len()
                == 1
        );

        // VoteState accepts the invalid signature because the stake is not enough
        // to trigger verification
        let (qc, _) = votestate.process_vote::<HasherType, _>(
            &NodeId(keys[1].pubkey()),
            &v1_invalid,
            &valset,
            &vmap,
        );
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .0
                .len()
                == 2
        );
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .1
                .len()
                == 2
        );

        // VoteState attempts to create a QC
        // the first attempt fails: v1.sig is invalid
        // the second iteration succeeds: still have enough stake after removing v1
        let (qc, _) = votestate.process_vote::<HasherType, _>(
            &NodeId(keys[2].pubkey()),
            &v2_valid,
            &valset,
            &vmap,
        );
        assert!(qc.is_some());
        assert_eq!(
            qc.unwrap()
                .signatures
                .verify(&vmap, vote_idx.as_ref())
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![NodeId(keys[0].pubkey()), NodeId(keys[2].pubkey())]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .0
                .len()
                == 2
        );
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .get(&vote_idx)
                .unwrap()
                .1
                .len()
                == 2
        );
    }
}
