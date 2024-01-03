use monad_consensus::messages::message::{ProposalMessage, TimeoutMessage, VoteMessage};
use monad_consensus_types::{
    block::{Block, UnverifiedBlock},
    ledger::CommitResult,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, Hashable, Hasher, HasherType},
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
        high_qc: QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.round);
    hasher.update(ti.high_qc.get_round());
    let h1 = hasher.hash();

    let h2 = ti.timeout_digest();

    assert_eq!(h1, h2);
}

#[test]
fn timeout_info_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.round.0.as_bytes());
    hasher.update(ti.high_qc.get_block_id().0.as_bytes());
    hasher.update(ti.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&ti);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
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
    hasher.update(tmo.tminfo.high_qc.get_block_id().0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x00_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x00_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let cert_key = get_certificate_key::<MockSignatures>(7);

    let tmo_msg = TimeoutMessage::new(tmo, &cert_key);

    let mut hasher = HasherType::new();
    hasher.update(tmo_msg.timeout.tminfo.round.0.as_bytes());
    hasher.update(
        tmo_msg
            .timeout
            .tminfo
            .high_qc
            .info
            .vote
            .vote_info
            .id
            .0
            .as_bytes(),
    );
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

    let txns = FullTransactionList::new(vec![1, 2, 3, 4].into());

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
    let author = NodeId(keypair.pubkey());
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new(
        QcInfo {
            vote: Vote {
                vote_info: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                    seq_num: SeqNum(0),
                },
                ledger_commit_info: CommitResult::NoCommit,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures>::new(
        author,
        round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        },
        &qc,
    );

    let proposal: ProposalMessage<MockSignatures> = ProposalMessage {
        block: UnverifiedBlock(block.clone()),
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

#[test_case(CommitResult::NoCommit ; "None commit_state")]
#[test_case(CommitResult::Commit ; "Some commit_state")]
fn vote_msg_hash(cs: CommitResult) {
    let vi = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(0),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
    };

    let v = Vote {
        vote_info: vi,
        ledger_commit_info: cs,
    };

    let certkey = get_certificate_key::<SignatureCollectionType>(7);
    let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkey);

    let mut hasher = HasherType::new();
    v.hash(&mut hasher);
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
