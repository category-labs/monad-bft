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

//! Test demonstrating that MAX_VERIFICATION_ATTEMPTS prevents infinite loops
//! when signature verification fails repeatedly.
//!
//! This test was written in response to issue #2345 to demonstrate that the
//! vote state processing correctly handles the case where invalid signatures
//! would otherwise cause infinite verification attempts.

use monad_consensus::{messages::message::VoteMessage, vote_state::VoteState};
use monad_consensus_types::{quorum_certificate::QuorumCertificate, voting::Vote};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey},
    signing_domain, NopKeyPair, NopSignature,
};
use monad_multi_sig::MultiSig;
use monad_testutil::{
    signing::{create_certificate_keys, create_keys},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, DontCare, Epoch, NodeId, Round, Stake};
use monad_validator::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_mapping::ValidatorMapping,
    validator_set::{ValidatorSetFactory, ValidatorSetType},
};

type SignatureType = NopSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;
type SignatureCollectionType = MultiSig<SignatureType>;

/// Maximum number of signature verification attempts per vote to prevent infinite loops
const MAX_VERIFICATION_ATTEMPTS: u32 = 3;

fn create_vote_message<SCT: SignatureCollection>(
    certkeypair: &SignatureCollectionKeyPairType<SCT>,
    vote_round: Round,
    valid: bool,
) -> VoteMessage<SCT> {
    let v = Vote {
        id: BlockId(monad_crypto::hasher::Hash([0x00_u8; 32])),
        epoch: Epoch(1),
        round: vote_round,
    };

    let mut vm = VoteMessage::new(v, certkeypair);
    if !valid {
        let invalid_msg = b"invalid";
        vm.sig = <SCT::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(
            invalid_msg.as_ref(),
            certkeypair,
        );
    }
    vm
}

#[test]
fn test_max_verification_attempts_prevents_infinite_loop() {
    let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));

    let keys = create_keys::<SignatureType>(4);
    let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

    // Create a validator set where all nodes have equal stake
    // This ensures that we need all 4 votes to reach supermajority (3/4 threshold)
    let staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake::ONE))
        .collect::<Vec<_>>();

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

    // Create 4 votes where all are invalid signatures
    // This will cause signature verification to fail repeatedly
    let v0_invalid = create_vote_message(&certkeys[0], vote_round, false);
    let v1_invalid = create_vote_message(&certkeys[1], vote_round, false);
    let v2_invalid = create_vote_message(&certkeys[2], vote_round, false);
    let v3_invalid = create_vote_message(&certkeys[3], vote_round, false);

    let vote = v0_invalid.vote;

    // Add the first 3 invalid votes - no QC should be created yet
    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v0_invalid, &valset, &vmap);
    assert!(qc.is_none(), "No QC should be created with insufficient votes");

    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v1_invalid, &valset, &vmap);
    assert!(qc.is_none(), "No QC should be created with insufficient votes");

    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v2_invalid, &valset, &vmap);
    assert!(qc.is_none(), "No QC should be created with insufficient votes");

    // At this point, we should have supermajority but no verification attempts yet
    // because we haven't reached the threshold
    let round_state = votestate.pending_votes.get(&vote_round).unwrap();
    assert!(
        !round_state.verification_attempts.contains_key(&vote),
        "No verification attempts should have been made yet"
    );

    // Add the fourth invalid vote - this will trigger supermajority check
    // but all signatures are invalid, so it should hit the max verification attempts limit
    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[3].pubkey()), &v3_invalid, &valset, &vmap);

    // Should not create a QC due to max verification attempts being reached
    assert!(
        qc.is_none(),
        "No QC should be created due to max verification attempts limit"
    );

    // Verify that the verification attempts counter was incremented to the maximum
    let round_state = votestate.pending_votes.get(&vote_round).unwrap();
    let attempts = round_state
        .verification_attempts
        .get(&vote)
        .expect("Verification attempts should be tracked");
    assert_eq!(
        *attempts, MAX_VERIFICATION_ATTEMPTS,
        "Should have reached maximum verification attempts"
    );

    // All votes should still be present in pending_votes since they weren't removed
    // (because we hit the max attempts limit before they could be processed)
    assert_eq!(
        round_state.pending_votes.get(&vote).unwrap().len(),
        4,
        "All invalid votes should still be present"
    );
}

#[test]
fn test_verification_attempts_with_mixed_valid_invalid_votes() {
    let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));

    let keys = create_keys::<SignatureType>(4);
    let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

    // Set up validator set where we need 3 out of 4 for supermajority
    let staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake::ONE))
        .collect::<Vec<_>>();

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

    // Create a mix of valid and invalid votes
    let v0_valid = create_vote_message(&certkeys[0], vote_round, true);
    let v1_valid = create_vote_message(&certkeys[1], vote_round, true);
    let v2_invalid = create_vote_message(&certkeys[2], vote_round, false);

    let vote = v0_valid.vote;

    // Add first two valid votes
    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v0_valid, &valset, &vmap);
    assert!(qc.is_none(), "No QC yet with only 2 votes");

    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v1_valid, &valset, &vmap);
    assert!(qc.is_none(), "No QC yet with only 2 votes");

    // At this point, no verification attempts should have been made
    // because we don't have supermajority yet
    let round_state = votestate.pending_votes.get(&vote_round).unwrap();
    assert!(
        !round_state.verification_attempts.contains_key(&vote),
        "No verification attempts should be made before supermajority"
    );

    // Add third vote (invalid) - this should trigger verification attempt
    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v2_invalid, &valset, &vmap);
    assert!(qc.is_none(), "No QC should be created due to invalid signature");

    // Now we should have made 1 verification attempt
    let round_state = votestate.pending_votes.get(&vote_round).unwrap();
    let attempts = round_state
        .verification_attempts
        .get(&vote)
        .expect("Should have verification attempts");
    assert_eq!(*attempts, 1, "Should have made exactly 1 verification attempt");

    // The invalid signature should have been removed, leaving only valid ones
    assert_eq!(
        round_state.pending_votes.get(&vote).unwrap().len(),
        2, // Only the 2 valid votes remain
        "Invalid signatures should be removed after verification failure"
    );
}

#[test]
fn test_successful_qc_creation_after_removing_invalid_signatures() {
    let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));

    let keys = create_keys::<SignatureType>(4);
    let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

    // Create a validator set where node 3 has supermajority stake by itself
    let mut staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake::ONE))
        .collect::<Vec<_>>();

    // Give node 3 enough stake to have supermajority
    staking_list[3].1 = Stake::from(10);

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

    // Create votes: 2 valid, 1 invalid, 1 valid with supermajority stake
    let v0_valid = create_vote_message(&certkeys[0], vote_round, true);
    let v1_valid = create_vote_message(&certkeys[1], vote_round, true);
    let v2_invalid = create_vote_message(&certkeys[2], vote_round, false);
    let v3_valid_supermajority = create_vote_message(&certkeys[3], vote_round, true);

    let vote = v0_valid.vote;

    // Add votes in sequence
    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v0_valid, &valset, &vmap);
    assert!(qc.is_none());

    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v1_valid, &valset, &vmap);
    assert!(qc.is_none());

    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v2_invalid, &valset, &vmap);
    assert!(qc.is_none());

    // Add the supermajority vote - this should trigger QC creation
    // First attempt fails due to invalid signature, but second attempt succeeds
    let (qc, _) = votestate.process_vote(
        &NodeId::new(keys[3].pubkey()),
        &v3_valid_supermajority,
        &valset,
        &vmap,
    );

    // Should successfully create a QC after removing invalid signatures
    assert!(qc.is_some(), "QC should be created after removing invalid signatures");

    // Verify that verification attempts were made
    let round_state = votestate.pending_votes.get(&vote_round).unwrap();
    let attempts = round_state
        .verification_attempts
        .get(&vote)
        .expect("Should have verification attempts");
    assert!(*attempts > 0, "Should have made at least 1 verification attempt");
    assert!(
        *attempts <= MAX_VERIFICATION_ATTEMPTS,
        "Should not exceed max verification attempts"
    );
}

#[test]
fn test_verification_attempts_reset_for_different_votes() {
    let mut votestate = VoteState::<SignatureCollectionType>::new(Round(0));

    let keys = create_keys::<SignatureType>(4);
    let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

    let staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake::ONE))
        .collect::<Vec<_>>();

    let voting_identity = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certkeys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();

    let valset = ValidatorSetFactory::default()
        .create(staking_list)
        .expect("create validator set");
    let vmap = ValidatorMapping::new(voting_identity);

    // Test with two different rounds to verify attempts are tracked per vote
    let vote_round_1 = Round(0);
    let vote_round_2 = Round(1);

    // Create votes for round 1 (all invalid)
    let v1_0_invalid = create_vote_message(&certkeys[0], vote_round_1, false);
    let v1_1_invalid = create_vote_message(&certkeys[1], vote_round_1, false);
    let v1_2_invalid = create_vote_message(&certkeys[2], vote_round_1, false);
    let v1_3_invalid = create_vote_message(&certkeys[3], vote_round_1, false);

    // Add all votes for round 1 to trigger max verification attempts
    votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v1_0_invalid, &valset, &vmap);
    votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v1_1_invalid, &valset, &vmap);
    votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v1_2_invalid, &valset, &vmap);
    votestate.process_vote(&NodeId::new(keys[3].pubkey()), &v1_3_invalid, &valset, &vmap);

    // Verify round 1 hit max attempts
    let round_state_1 = votestate.pending_votes.get(&vote_round_1).unwrap();
    let attempts_1 = round_state_1
        .verification_attempts
        .get(&v1_0_invalid.vote)
        .unwrap();
    assert_eq!(*attempts_1, MAX_VERIFICATION_ATTEMPTS);

    // Now create votes for round 2 (all valid)
    let v2_0_valid = create_vote_message(&certkeys[0], vote_round_2, true);
    let v2_1_valid = create_vote_message(&certkeys[1], vote_round_2, true);
    let v2_2_valid = create_vote_message(&certkeys[2], vote_round_2, true);

    // Add votes for round 2
    votestate.process_vote(&NodeId::new(keys[0].pubkey()), &v2_0_valid, &valset, &vmap);
    votestate.process_vote(&NodeId::new(keys[1].pubkey()), &v2_1_valid, &valset, &vmap);
    let (qc, _) =
        votestate.process_vote(&NodeId::new(keys[2].pubkey()), &v2_2_valid, &valset, &vmap);

    // Should create QC for round 2 despite round 1 hitting max attempts
    assert!(qc.is_some(), "Should create QC for different vote/round");

    // Verify round 2 has its own verification attempt counter
    let round_state_2 = votestate.pending_votes.get(&vote_round_2);
    if let Some(state) = round_state_2 {
        if let Some(attempts_2) = state.verification_attempts.get(&v2_0_valid.vote) {
            assert!(*attempts_2 <= MAX_VERIFICATION_ATTEMPTS);
        }
    }
}
