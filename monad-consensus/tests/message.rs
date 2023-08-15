use monad_consensus::messages::message::{ProposalMessage, TimeoutMessage, VoteMessage};
use monad_consensus_types::{
    block::Block,
    certificate_signature::CertificateKeyPair,
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, Payload, TransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo},
    validation::{Hasher, Sha256Hash},
    voting::{Vote, VoteInfo},
};
use monad_crypto::secp256k1::{KeyPair, SecpSignature};
use monad_testutil::signing::*;
use monad_types::*;
use sha2::Digest;
use test_case::test_case;

type SignatureCollectionType = MultiSig<SecpSignature>;

#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new::<Sha256Hash>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                },
                ledger_commit: Default::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tm: TimeoutMessage<SecpSignature, MockSignatures> = TimeoutMessage {
        tminfo: ti,
        last_round_tc: None,
    };

    let mut hasher = sha2::Sha256::new();
    hasher.update(tm.tminfo.round);
    hasher.update(tm.tminfo.high_qc.info.vote.round);
    let h1: Hash = Hash(hasher.finalize_reset().into());

    let h2 = Sha256Hash::hash_object(&tm);

    assert_eq!(h1, h2);
}

#[test]
fn proposal_msg_hash() {
    use monad_testutil::signing::hash;

    let txns = TransactionList(vec![1, 2, 3, 4]);

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
    let author = NodeId(keypair.pubkey());
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new::<Sha256Hash>(
        QcInfo {
            vote: VoteInfo {
                id: BlockId(Hash([0x00_u8; 32])),
                round: Round(0),
                parent_id: BlockId(Hash([0x00_u8; 32])),
                parent_round: Round(0),
            },
            ledger_commit: LedgerCommitInfo::default(),
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures>::new::<Sha256Hash>(
        author,
        round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
        },
        &qc,
    );

    let proposal: ProposalMessage<SecpSignature, MockSignatures> = ProposalMessage {
        block: block.clone(),
        last_round_tc: None,
    };

    let h1 = Sha256Hash::hash_object(&proposal);
    let h2 = hash(&block);

    assert_eq!(h1, h2);
}

#[test]
fn max_high_qc() {
    let high_qc_rounds = [
        HighQcRound { qc_round: Round(1) },
        HighQcRound { qc_round: Round(3) },
        HighQcRound { qc_round: Round(1) },
    ]
    .iter()
    .map(|x| {
        let msg = Sha256Hash::hash_object(x);
        let keypair = get_key(0);
        HighQcRoundSigTuple {
            high_qc_round: *x,
            author_signature: keypair.sign(msg.as_ref()),
        }
    })
    .collect();

    let tc = TimeoutCertificate {
        round: Round(2),
        high_qc_rounds,
    };

    assert_eq!(tc.max_round(), Round(3));
}

#[test]
fn test_vote_message() {
    let lci = LedgerCommitInfo {
        commit_state_hash: Some(Default::default()),
        vote_info_hash: Default::default(),
    };

    let v = Vote {
        vote_info: VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        },
        ledger_commit_info: lci,
    };

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey.clone()).unwrap();
    let certkeypair = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(&mut privkey).unwrap();

    let vm = VoteMessage::<SignatureCollectionType>::new::<Sha256Hash>(v, &certkeypair);

    let expected_vote_info_hash = vm.vote.ledger_commit_info.vote_info_hash;

    let msg = Sha256Hash::hash_object(&vm);
    let svm = TestSigner::sign_object::<Sha256Hash, _>(vm, &keypair);

    assert_eq!(
        svm.author_signature().recover_pubkey(msg.as_ref()).unwrap(),
        keypair.pubkey()
    );

    // TODO fix this test.. would be best if we could do this as a unit test
    // assert_eq!(
    //     svm.obj.ledger_commit_info.vote_info_hash,
    //     expected_vote_info_hash
    // );
}

#[test_case(None ; "None commit_state")]
#[test_case(Some(Default::default()) ; "Some commit_state")]
fn vote_msg_hash(cs: Option<Hash>) {
    let vi = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(0),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
    };
    let vi_hash = Sha256Hash::hash_object(&vi);

    let lci = LedgerCommitInfo {
        commit_state_hash: cs,
        vote_info_hash: vi_hash,
    };

    let v = Vote {
        vote_info: vi,
        ledger_commit_info: lci,
    };

    let certkey = get_certificate_key::<SignatureCollectionType>(7);
    let vm = VoteMessage::<SignatureCollectionType>::new::<Sha256Hash>(v, &certkey);

    let mut hasher = sha2::Sha256::new();
    hasher.update(vi_hash);
    if let Some(cs) = cs {
        hasher.update(cs);
    }
    unsafe {
        let sig_bytes = std::mem::transmute::<
            <SignatureCollectionType as SignatureCollection>::SignatureType,
            [u8; std::mem::size_of::<
                <SignatureCollectionType as SignatureCollection>::SignatureType,
            >()],
        >(vm.sig);
        hasher.update(sig_bytes);
    }

    let h1: Hash = Hash(hasher.finalize_reset().into());

    let h2 = Sha256Hash::hash_object(&vm);

    assert_eq!(h1, h2);
}
