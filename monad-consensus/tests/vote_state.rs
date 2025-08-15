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

use monad_consensus::{messages::message::VoteMessage, vote_state::VoteState};
use monad_consensus_types::{quorum_certificate::QuorumCertificate, voting::Vote};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    NopKeyPair, NopSignature,
};
use monad_multi_sig::MultiSig;
use monad_testutil::validators::create_keys_w_validators;
use monad_types::{DontCare, Epoch, NodeId, Round};
use monad_validator::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_mapping::ValidatorMapping,
    validator_set::{ValidatorSet, ValidatorSetFactory},
};
use test_case::test_case;

type SignatureType = NopSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;
type SignatureCollectionType = MultiSig<SignatureType>;

fn create_vote_message(
    key: &NopKeyPair,
    certkey: &SignatureCollectionKeyPairType<SignatureCollectionType>,
    vote_epoch: Epoch,
    vote_round: Round,
) -> (NodeId<PubKeyType>, VoteMessage<SignatureCollectionType>) {
    let v = Vote {
        epoch: vote_epoch,
        round: vote_round,
        ..DontCare::dont_care()
    };

    let vm = VoteMessage::<SignatureCollectionType>::new(v, certkey);

    (NodeId::new(key.pubkey()), vm)
}

fn setup_ctx(
    num_nodes: u32,
    num_rounds: u64,
) -> (
    Vec<NopKeyPair>,
    ValidatorSet<PubKeyType>,
    ValidatorMapping<PubKeyType, SignatureCollectionKeyPairType<SignatureCollectionType>>,
    Vec<(NodeId<PubKeyType>, VoteMessage<SignatureCollectionType>)>,
) {
    let (keys, cert_keys, valset, vmap) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(num_nodes, ValidatorSetFactory::default());

    let mut votes = Vec::new();
    for j in 0..num_rounds {
        for i in 0..num_nodes {
            let svm_with_author = create_vote_message(
                &keys[i as usize],
                &cert_keys[i as usize],
                Epoch(1),
                Round(j),
            );

            votes.push(svm_with_author);
        }
    }

    (keys, valset, vmap, votes)
}

fn verify_qcs(
    qcs: Vec<&Option<QuorumCertificate<SignatureCollectionType>>>,
    expected_qcs: u32,
    expected_sigs: u32,
) {
    assert_eq!(qcs.len(), expected_qcs as usize);

    for i in 0..expected_qcs {
        assert_eq!(
            qcs[i as usize]
                .as_ref()
                .unwrap()
                .signatures
                .num_signatures(),
            expected_sigs as usize
        );
    }
}

#[test_case(4 ; "min nodes")]
#[test_case(15 ; "multiple nodes")]
fn test_votes(num_nodes: u32) {
    let (_, valset, vmap, votes) = setup_ctx(num_nodes, 1);

    let mut voteset = VoteState::<SignatureCollectionType>::new(Round(0));
    let mut qcs = Vec::new();
    for i in 0..num_nodes {
        let (author, v) = &votes[i as usize];
        let (qc, cmds) = voteset.process_vote(author, v, &valset, &vmap);
        assert!(cmds.is_empty());
        qcs.push(qc);
    }
    let valid_qc: Vec<&Option<QuorumCertificate<SignatureCollectionType>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    // number of expected signatures is 2/3 + 1 for num_nodes because all weights were equal
    let num_expected_sigs = 2 * num_nodes / 3 + 1;

    // no reset of voteset is done, so expect only one qc regardless of validator count
    verify_qcs(valid_qc, 1, num_expected_sigs);
}

#[test_case(4, 2 ; "min nodes 1 reset")]
#[test_case(8, 2 ; "multiple nodes 1 reset")]
#[test_case(20, 5 ; "multiple nodes multiple resets")]
fn test_reset(num_nodes: u32, num_rounds: u32) {
    let (_, valset, vmap, votes) = setup_ctx(num_nodes, num_rounds.into());

    let mut voteset = VoteState::<SignatureCollectionType>::new(Round(0));
    let mut qcs = Vec::new();

    for k in 0..num_rounds {
        for i in 0..num_nodes {
            let (author, v) = &votes[(k * num_nodes + i) as usize];
            let (qc, cmds) = voteset.process_vote(author, v, &valset, &vmap);
            assert!(cmds.is_empty());
            qcs.push(qc);
        }

        voteset.start_new_round(Round(k.into()) + Round(1));
    }

    let valid_qc: Vec<&Option<QuorumCertificate<SignatureCollectionType>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    let num_expected_sigs = 2 * num_nodes / 3 + 1;

    verify_qcs(valid_qc, num_rounds, num_expected_sigs);
}

#[test_case(4 ; "min nodes")]
#[test_case(15 ; "multiple nodes")]
fn test_minority(num_nodes: u32) {
    let (_, valset, vmap, votes) = setup_ctx(num_nodes, 1);

    let mut voteset = VoteState::<SignatureCollectionType>::new(Round(0));
    let mut qcs = Vec::new();

    let majority = 2 * num_nodes / 3 + 1;

    for i in 0..majority - 1 {
        let (author, v) = &votes[i as usize];
        let (qc, cmds) = voteset.process_vote(author, v, &valset, &vmap);
        assert!(cmds.is_empty());
        qcs.push(qc);
    }

    let valid_qc: Vec<&Option<QuorumCertificate<SignatureCollectionType>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    assert_eq!(valid_qc.len(), 0);
}
