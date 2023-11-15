use monad_consensus::{
    messages::message::VoteMessage,
    validation::signing::{Unverified, Verified},
    vote_state::VoteState,
};
use monad_consensus_types::{
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, HasherType},
    secp256k1::{KeyPair, SecpSignature},
};
use monad_testutil::{signing::*, validators::create_keys_w_validators};
use monad_types::{BlockId, Round, SeqNum};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};
use test_case::test_case;

type SignatureCollectionType = MultiSig<SecpSignature>;

fn create_signed_vote_message(
    key: &KeyPair,
    certkey: &SignatureCollectionKeyPairType<SignatureCollectionType>,
    vote_round: Round,
) -> Unverified<SecpSignature, VoteMessage<SignatureCollectionType>> {
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

    let vm = VoteMessage::<SignatureCollectionType>::new::<HasherType>(v, certkey);

    TestSigner::sign_object::<HasherType, _>(vm, key)
}

fn setup_ctx(
    num_nodes: u32,
) -> (
    Vec<KeyPair>,
    ValidatorSet,
    ValidatorMapping<SignatureCollectionKeyPairType<SignatureCollectionType>>,
    Vec<Verified<SecpSignature, VoteMessage<SignatureCollectionType>>>,
) {
    let (keys, cert_keys, valset, vmap) =
        create_keys_w_validators::<SignatureCollectionType>(num_nodes);

    let mut votes = Vec::new();
    for i in 0..num_nodes {
        let svm = create_signed_vote_message(&keys[i as usize], &cert_keys[i as usize], Round(0));
        let vm = svm
            .verify::<HasherType>(valset.get_members(), &keys[i as usize].pubkey())
            .unwrap();

        votes.push(vm);
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
    let (_, valset, vmap, votes) = setup_ctx(num_nodes);

    let mut voteset = VoteState::<SignatureCollectionType>::default();
    let mut qcs = Vec::new();
    for i in 0..num_nodes {
        let v = &votes[i as usize];
        let (qc, cmds) = voteset.process_vote::<HasherType, _>(v.author(), v, &valset, &vmap);
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
    let (_, valset, vmap, votes) = setup_ctx(num_nodes);

    let mut voteset = VoteState::<SignatureCollectionType>::default();
    let mut qcs = Vec::new();

    for k in 0..num_rounds {
        for i in 0..num_nodes {
            let v = &votes[i as usize];
            let (qc, cmds) = voteset.process_vote::<HasherType, _>(v.author(), v, &valset, &vmap);
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
    let (_, valset, vmap, votes) = setup_ctx(num_nodes);

    let mut voteset = VoteState::<SignatureCollectionType>::default();
    let mut qcs = Vec::new();

    let majority = 2 * num_nodes / 3 + 1;

    for i in 0..majority - 1 {
        let v = &votes[i as usize];
        let (qc, cmds) = voteset.process_vote::<HasherType, _>(v.author(), v, &valset, &vmap);
        assert!(cmds.is_empty());
        qcs.push(qc);
    }

    let valid_qc: Vec<&Option<QuorumCertificate<SignatureCollectionType>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    assert_eq!(valid_qc.len(), 0);
}
