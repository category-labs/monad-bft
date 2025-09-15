// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::{BTreeMap, HashMap, HashSet};

use monad_consensus_types::{quorum_certificate::QuorumCertificate, voting::Vote};
use monad_crypto::{
    certificate_signature::{CertificateSignature, PubKey},
    signing_domain,
};
use monad_types::{NodeId, Round};
use monad_validator::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    validator_mapping::ValidatorMapping,
    validator_set::ValidatorSetType,
};
use tracing::{debug, error, info, warn};

use crate::messages::message::VoteMessage;

/// Maximum number of signature verification attempts per vote to prevent infinite loops
const MAX_VERIFICATION_ATTEMPTS: u32 = 3;

/// VoteState accumulates votes and creates a QC if enough votes are received
/// Only one QC should be created in a round using the first supermajority of votes received
/// At the end of a round, older rounds can be cleaned up
#[derive(Debug, PartialEq, Eq)]
pub struct VoteState<SCT: SignatureCollection> {
    /// Received pending votes for rounds >= self.earliest_round
    pending_votes: BTreeMap<Round, RoundVoteState<SCT::NodeIdPubKey, SCT::SignatureType>>,
    /// The earliest round that we'll accept votes for
    /// We use this to not build the same QC twice, and to know which votes are stale
    earliest_round: Round,
}

#[derive(Debug, PartialEq, Eq)]
struct RoundVoteState<PT: PubKey, ST: CertificateSignature> {
    /// Pending votes, keyed by vote
    /// It's possible for a Node to have pending votes in multiple buckets if they're malicious
    pending_votes: HashMap<Vote, BTreeMap<NodeId<PT>, ST>>,
    // All vote hashes each node has voted on; multiple vote hashes for a given node implies
    // they're malicious
    node_votes: HashMap<NodeId<PT>, HashSet<ST>>,
    /// Track verification attempts per vote to prevent infinite loops
    /// Maps Vote -> number of verification attempts
    verification_attempts: HashMap<Vote, u32>,
}

impl<PT: PubKey, ST: CertificateSignature> Default for RoundVoteState<PT, ST> {
    fn default() -> Self {
        RoundVoteState {
            pending_votes: HashMap::new(),
            node_votes: HashMap::new(),
            verification_attempts: HashMap::new(),
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
    pub fn new(round: Round) -> Self {
        VoteState {
            earliest_round: round,
            pending_votes: Default::default(),
        }
    }

    #[must_use]
    pub fn process_vote<VT>(
        &mut self,
        author: &NodeId<SCT::NodeIdPubKey>,
        vote_msg: &VoteMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> (Option<QuorumCertificate<SCT>>, Vec<VoteStateCommand>)
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let vote = vote_msg.vote;
        let round = vote_msg.vote.round;

        let mut ret_commands = Vec::new();

        if round < self.earliest_round {
            error!(
                ?round,
                earliest_round = ?self.earliest_round,
                "process_vote called on round < self.earliest_round",
            );
            return (None, ret_commands);
        }

        // pending votes for a given round + vote hash
        let round_state = self.pending_votes.entry(round).or_default();
        let node_votes = round_state.node_votes.entry(*author).or_default();
        node_votes.insert(vote_msg.sig);
        if node_votes.len() > 1 {
            // TODO: collect double vote as evidence
        }

        // pending votes for a given round + vote hash
        let round_pending_votes = round_state.pending_votes.entry(vote).or_default();
        round_pending_votes.insert(*author, vote_msg.sig);

        debug!(
            ?round,
            ?vote,
            epoch = ?vote.epoch,
            current_stake = ?validators.calculate_current_stake(&round_pending_votes.keys().copied().collect::<Vec<_>>()),
            total_stake = ?validators.get_total_stake(),
            "collecting vote"
        );

        while validators
            .has_super_majority_votes(&round_pending_votes.keys().copied().collect::<Vec<_>>())
            .expect("has_super_majority_votes succeeds since addresses are unique")
        {
            // Check verification attempt limit to prevent infinite loops
            let verification_attempts = round_state.verification_attempts.entry(vote).or_insert(0);
            if *verification_attempts >= MAX_VERIFICATION_ATTEMPTS {
                warn!(
                    round = ?vote.round,
                    epoch = ?vote.epoch,
                    attempts = *verification_attempts,
                    max_attempts = MAX_VERIFICATION_ATTEMPTS,
                    "Reached maximum verification attempts, stopping to prevent infinite loop"
                );
                break;
            }

            *verification_attempts += 1;

            assert!(round >= self.earliest_round);
            let vote_enc = alloy_rlp::encode(vote);
            match SCT::new::<signing_domain::Vote>(
                round_pending_votes
                    .iter()
                    .map(|(node, signature)| (*node, *signature)),
                validator_mapping,
                vote_enc.as_ref(),
            ) {
                Ok(sigcol) => {
                    let qc = QuorumCertificate::<SCT>::new(vote, sigcol);
                    // we update self.earliest round so that we no longer will build a QC for
                    // current round
                    self.earliest_round = round + Round(1);

                    info!(
                        round = ?vote.round,
                        epoch = ?vote.epoch,
                        attempts = *verification_attempts,
                        "Created new QC"
                    );
                    return (Some(qc), ret_commands);
                }
                Err(SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs)) => {
                    // remove invalid signatures from round_pending_votes, and populate commands
                    let cmds = Self::handle_invalid_vote(round_pending_votes, invalid_sigs);

                    warn!(
                        round = ?vote.round,
                        epoch = ?vote.epoch,
                        attempts = *verification_attempts,
                        invalid_count = invalid_sigs.len(),
                        "Invalid signatures when creating new QC, removed invalid signatures"
                    );
                    ret_commands.extend(cmds);
                }
                Err(
                    SignatureCollectionError::NodeIdNotInMapping(_)
                    | SignatureCollectionError::ConflictingSignatures(_)
                    | SignatureCollectionError::InvalidSignaturesVerify
                    | SignatureCollectionError::DeserializeError(_),
                ) => {
                    unreachable!("InvalidSignaturesCreate is only expected error from creating SC");
                }
            }
        }

        (None, ret_commands)
    }

    #[must_use]
    fn handle_invalid_vote(
        pending_entry: &mut BTreeMap<NodeId<SCT::NodeIdPubKey>, SCT::SignatureType>,
        invalid_votes: Vec<(NodeId<SCT::NodeIdPubKey>, SCT::SignatureType)>,
    ) -> Vec<VoteStateCommand> {
        let invalid_vote_set = invalid_votes
            .into_iter()
            .map(|(a, _)| a)
            .collect::<HashSet<_>>();
        pending_entry.retain(|node_id, _| !invalid_vote_set.contains(node_id));
        // TODO: evidence
        vec![]
    }

    pub fn start_new_round(&mut self, new_round: Round) {
        self.earliest_round = new_round;
        self.pending_votes.retain(|k, _| *k >= new_round);
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_consensus_types::voting::Vote;
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        hasher::Hash,
        signing_domain, NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_testutil::{signing::*, validators::create_keys_w_validators};
    use monad_types::{BlockId, Epoch, NodeId, Round, Stake};
    use monad_validator::{
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        validator_mapping::ValidatorMapping,
        validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
    };

    use super::VoteState;
    use crate::messages::message::VoteMessage;

    type SigningDomainType = signing_domain::Vote;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

    fn create_vote_message<SCT: SignatureCollection>(
        certkeypair: &SignatureCollectionKeyPairType<SCT>,
        vote_round: Round,
        valid: bool,
    ) -> VoteMessage<SCT> {
        let v = Vote {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: vote_round,
        };

        let mut vm = VoteMessage::new(v, certkeypair);
        if !valid {
            let invalid_msg = b"invalid";
            vm.sig = <SCT::SignatureType as CertificateSignature>::sign::<SigningDomainType>(
                invalid_msg.as_ref(),
                certkeypair,
            );
        }
        vm
    }

    #[test]
    fn clean_older_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));
        let (keys, cert_keys, valset, val_map) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        // add one vote for rounds 0-3
        let mut votes = Vec::new();
        for i in 0..4 {
            let svm = create_vote_message::<SignatureCollectionType>(
                &cert_keys[0],
                Round(i.try_into().unwrap()),
                true,
            );
            let (_qc, cmds) = votestate.process_vote(
                &NodeId::new(cert_keys[0].pubkey()),
                &svm,
                &valset,
                &val_map,
            );
            votes.push(svm);
            assert!(cmds.is_empty());
        }

        assert_eq!(votestate.pending_votes.len(), 4);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for certkey in cert_keys.iter().take(4) {
            let svm = create_vote_message::<SignatureCollectionType>(certkey, Round(4), true);
            let _qc =
                votestate.process_vote(&NodeId::new(certkey.pubkey()), &svm, &valset, &val_map);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 0);

        // apply old votes again
        for svm in votes {
            let (_qc, cmds) = votestate.process_vote(
                &NodeId::new(cert_keys[0].pubkey()),
                &svm,
                &valset,
                &val_map,
            );
        }

        // pending_votes should still be 0 after starting a new round and processing old votes
        assert_eq!(votestate.pending_votes.len(), 0);
    }

    #[test]
    fn handle_future_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));
        let (keys, cert_keys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        // add one vote for rounds 0-3 and 5-8
        for i in 0..4 {
            let svm = create_vote_message(&cert_keys[0], Round(i.try_into().unwrap()), true);
            let _qc =
                votestate.process_vote(&NodeId::new(cert_keys[0].pubkey()), &svm, &valset, &vmap);
        }

        for i in 5..9 {
            let svm = create_vote_message(&cert_keys[0], Round(i.try_into().unwrap()), true);
            let _qc =
                votestate.process_vote(&NodeId::new(cert_keys[0].pubkey()), &svm, &valset, &vmap);
        }

        assert_eq!(votestate.pending_votes.len(), 8);

        // add supermajority number of votes for round 4, expecting older rounds to be
        // removed
        for certkey in cert_keys.iter().take(4) {
            let svm = create_vote_message::<SignatureCollectionType>(certkey, Round(4), true);
            let _qc = votestate.process_vote(&NodeId::new(certkey.pubkey()), &svm, &valset, &vmap);
        }
        votestate.start_new_round(Round(5));

        assert_eq!(votestate.pending_votes.len(), 4);
    }

    #[test]
    fn duplicate_votes() {
        let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        // create a vote for round 0 from one node, but add it supermajority number of times
        // this should not result in QC creation
        let svm = create_vote_message(&certkeys[0], Round(0), true);
        let author = NodeId::new(certkeys[0].pubkey());

        for _ in 0..4 {
            let (qc, cmds) = votestate.process_vote(&author, &svm, &valset, &vmap);
            assert!(cmds.is_empty());
            assert!(qc.is_none());
        }
    }

    #[test]
    fn invalid_votes_no_qc() {
        let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let vote_round = Round(0);

        let v0_valid = create_vote_message(&certkeys[0], vote_round, true);
        let v1_valid = create_vote_message(&certkeys[1], vote_round, true);
        let v2_invalid = create_vote_message(&certkeys[2], vote_round, false);

        let vote = v0_valid.vote;

        let (qc, _) =
            votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v0_valid, &valset, &vmap);
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .pending_votes
                .get(&vote)
                .unwrap()
                .len()
                == 1
        );

        let (qc, _) =
            votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v1_valid, &valset, &vmap);
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .pending_votes
                .get(&vote)
                .unwrap()
                .len()
                == 2
        );

        // VoteState attempts to create a QC, but failed because one of the sigs is invalid
        // doesn't have supermajority after removing the invalid
        let (qc, _) =
            votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v2_invalid, &valset, &vmap);
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .pending_votes
                .get(&vote)
                .unwrap()
                .len()
                == 2
        );
    }

    #[test]
    fn invalid_votes_qc() {
        let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));

        let keys = create_keys::<SignatureType>(4);
        let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

        let mut staking_list = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(std::iter::repeat(Stake::ONE))
            .collect::<Vec<_>>();

        // node2 has supermajority stake by itself
        staking_list[2].1 = Stake::from(10);

        let voting_identity = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(certkeys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();

        let valset = ValidatorSetFactory::default()
            .create(staking_list)
            .expect("create validator set");
        let vmap = ValidatorMapping::new(voting_identity);

        let vote_round = Round(0);

        let v0_valid = create_vote_message(&certkeys[0], vote_round, true);
        let v1_invalid = create_vote_message(&certkeys[1], vote_round, false);
        let v2_valid = create_vote_message(&certkeys[2], vote_round, true);

        let vote = v0_valid.vote;

        let (qc, _) =
            votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v0_valid, &valset, &vmap);
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .pending_votes
                .get(&vote)
                .unwrap()
                .len()
                == 1
        );

        // VoteState accepts the invalid signature because the stake is not enough
        // to trigger verification
        let (qc, _) =
            votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v1_invalid, &valset, &vmap);
        assert!(qc.is_none());
        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .pending_votes
                .get(&vote)
                .unwrap()
                .len()
                == 2
        );

        // VoteState attempts to create a QC
        // the first attempt fails: v1.sig is invalid
        // the second iteration succeeds: still have enough stake after removing v1
        let (qc, _) =
            votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v2_valid, &valset, &vmap);
        assert!(qc.is_some());
        assert_eq!(
            qc.unwrap()
                .signatures
                .verify::<SigningDomainType>(&vmap, &alloy_rlp::encode(vote))
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![NodeId::new(keys[0].pubkey()), NodeId::new(keys[2].pubkey())]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        assert!(
            votestate
                .pending_votes
                .get(&vote_round)
                .unwrap()
                .pending_votes
                .get(&vote)
                .unwrap()
                .len()
                == 2
        );
    }
}
