use monad_consensus::messages::message::{ProposalMessage, TimeoutMessage, VoteMessage};
use monad_consensus_types::{
    block::Block,
    certificate_signature::CertificateKeyPair,
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, Payload, RandaoReveal, TransactionHashList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, Hasher, HasherType},
    secp256k1::{KeyPair, SecpSignature},
};
use monad_eth_types::EthAddress;
use monad_testutil::signing::*;
use monad_types::*;
use test_case::test_case;
use zerocopy::AsBytes;

type SignatureCollectionType = MultiSig<SecpSignature>;

#[test]
fn timeout_digest() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: Default::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.round);
    hasher.update(ti.high_qc.info.vote.round);
    let h1 = hasher.hash();

    let h2 = ti.timeout_digest::<HasherType>();

    assert_eq!(h1, h2);
}

#[test]
fn timeout_info_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: Default::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.round.0.as_bytes());
    hasher.update(ti.high_qc.info.vote.id.0.as_bytes());
    hasher.update(ti.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&ti);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: Default::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let mut hasher = HasherType::new();
    hasher.update(tmo.tminfo.round.0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.info.vote.id.0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: Default::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let cert_key = get_certificate_key::<MockSignatures>(7);

    let tmo_msg = TimeoutMessage::new::<HasherType>(tmo, &cert_key);

    let mut hasher = HasherType::new();
    hasher.update(tmo_msg.timeout.tminfo.round.0.as_bytes());
    hasher.update(tmo_msg.timeout.tminfo.high_qc.info.vote.id.0.as_bytes());
    hasher.update(tmo_msg.timeout.tminfo.high_qc.get_hash());
    unsafe {
        let sig_bytes = std::mem::transmute::<
            <SignatureCollectionType as SignatureCollection>::SignatureType,
            [u8; std::mem::size_of::<
                <SignatureCollectionType as SignatureCollection>::SignatureType,
            >()],
        >(tmo_msg.sig);
        hasher.update(sig_bytes);
    }

    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo_msg);

    assert_eq!(h1, h2);
}

#[test]
fn proposal_msg_hash() {
    use monad_testutil::signing::hash;

    let txns = TransactionHashList::new(vec![1, 2, 3, 4]);

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
    let author = NodeId(keypair.pubkey());
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new::<HasherType>(
        QcInfo {
            vote: VoteInfo {
                id: BlockId(Hash([0x00_u8; 32])),
                round: Round(0),
                parent_id: BlockId(Hash([0x00_u8; 32])),
                parent_round: Round(0),
                seq_num: 0,
            },
            ledger_commit: LedgerCommitInfo::default(),
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures>::new::<HasherType>(
        author,
        round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        },
        &qc,
    );

    let proposal: ProposalMessage<MockSignatures> = ProposalMessage {
        block: block.clone(),
        last_round_tc: None,
    };

    let h1 = HasherType::hash_object(&proposal);
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
        let msg = HasherType::hash_object(x);
        let keypair = get_key(0);
        HighQcRoundSigColTuple {
            high_qc_round: *x,
            sigs: keypair.sign(msg.as_ref()),
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
            seq_num: 0,
        },
        ledger_commit_info: lci,
    };

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey.clone()).unwrap();
    let certkeypair = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(&mut privkey).unwrap();

    let vm = VoteMessage::<SignatureCollectionType>::new::<HasherType>(v, &certkeypair);

    let expected_vote_info_hash = vm.vote.ledger_commit_info.vote_info_hash;

    let msg = HasherType::hash_object(&vm);
    let svm = TestSigner::sign_object::<HasherType, _>(vm, &keypair);

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
        seq_num: 0,
    };
    let vi_hash = HasherType::hash_object(&vi);

    let lci = LedgerCommitInfo {
        commit_state_hash: cs,
        vote_info_hash: vi_hash,
    };

    let v = Vote {
        vote_info: vi,
        ledger_commit_info: lci,
    };

    let certkey = get_certificate_key::<SignatureCollectionType>(7);
    let vm = VoteMessage::<SignatureCollectionType>::new::<HasherType>(v, &certkey);

    let mut hasher = HasherType::new();
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

    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&vm);

    assert_eq!(h1, h2);
}
