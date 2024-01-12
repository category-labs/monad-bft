use std::fs::create_dir_all;

use bytes::Bytes;
use criterion::{criterion_group, Criterion};
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::UnverifiedBlock,
    ledger::CommitResult,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, FullTransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hash, Hasher, HasherType},
    secp256k1::SecpSignature,
};
use monad_executor_glue::{ConsensusEvent, MonadEvent};
use monad_testutil::{
    block::setup_block,
    signing::{get_certificate_key, get_key},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, NodeId, Round, SeqNum, Serializable, TimeoutVariant};
use monad_wal::{
    wal::{WALogger, WALoggerConfig},
    PersistenceLogger,
};
use tempfile::{tempdir, TempDir};

const N_VALIDATORS: usize = 400;

type SignatureType = SecpSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type BenchEvent = MonadEvent<SignatureType, SignatureCollectionType>;

struct MonadEventBencher {
    event: BenchEvent,
    logger: WALogger<BenchEvent>,
    _tmpdir: TempDir,
}

impl MonadEventBencher {
    fn new(event: BenchEvent) -> Self {
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let file_path = tmpdir.path().join("wal");
        let config = WALoggerConfig {
            file_path,
            sync: false,
        };
        println!(
            "size of event: {}",
            Serializable::<Bytes>::serialize(&event).len()
        );
        Self {
            event,
            logger: WALogger::<BenchEvent>::new(config).unwrap().0,
            _tmpdir: tmpdir,
        }
    }

    fn append(&mut self) {
        self.logger.push(&self.event).unwrap()
    }
}

fn bench_proposal(c: &mut Criterion) {
    let txns = FullTransactionList::new(vec![0x23_u8; 32 * 10000].into());
    let (keypairs, _certkeypairs, _validators, validator_mapping) =
        create_keys_w_validators::<SignatureType, SignatureCollectionType>(1);
    let author_keypair = &keypairs[0];

    let blk = setup_block::<SignatureType, SignatureCollectionType>(
        NodeId::new(author_keypair.pubkey()),
        Round(10),
        Round(9),
        txns,
        ExecutionArtifacts::zero(),
        &keypairs,
        &validator_mapping,
    );

    let proposal = ConsensusMessage::Proposal(ProposalMessage {
        block: UnverifiedBlock(blk),
        last_round_tc: None,
    });
    let proposal_hash = HasherType::hash_object(&proposal);
    let unverified_message = Unverified::new(
        Unvalidated::new(proposal),
        author_keypair.sign(proposal_hash.as_ref()),
    );

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: author_keypair.pubkey(),
        unverified_message,
    });

    let mut bencher = MonadEventBencher::new(event);

    c.bench_function("bench_proposal", |b| b.iter(|| bencher.append()));
}

fn bench_vote(c: &mut Criterion) {
    let keypair = get_key::<SignatureType>(1);
    let certkey = get_certificate_key::<SignatureCollectionType>(2);
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
    };

    let v = Vote {
        vote_info: vi,
        ledger_commit_info: CommitResult::NoCommit,
    };

    let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkey);

    let vote = ConsensusMessage::Vote(vm);

    let vote_hash = HasherType::hash_object(&vote);
    let unverified_message = Unverified::new(Unvalidated::new(vote), <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(vote_hash.as_ref(), &keypair));

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: keypair.pubkey(),
        unverified_message,
    });

    let mut bencher = MonadEventBencher::new(event);

    c.bench_function("bench_vote", |b| {
        b.iter(|| {
            for _ in 0..N_VALIDATORS {
                bencher.append();
            }
        })
    });
}

fn bench_timeout(c: &mut Criterion) {
    let (keypairs, cert_keys, _validators, validator_mapping) =
        create_keys_w_validators::<SignatureType, SignatureCollectionType>(N_VALIDATORS as u32);
    let author_keypair = &keypairs[0];
    let author_certkey = &cert_keys[0];

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
    };

    let qcinfo = QcInfo {
        vote: Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        },
    };

    let qcinfo_hash = HasherType::hash_object(&qcinfo.vote);

    let mut sigs = Vec::new();
    for (key, cert_key) in keypairs.iter().zip(cert_keys.iter()) {
        let node_id = NodeId::new(key.pubkey());
        let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(qcinfo_hash.as_ref(), cert_key);
        sigs.push((node_id, sig));
    }
    let aggsig =
        SignatureCollectionType::new(sigs, &validator_mapping, qcinfo_hash.as_ref()).unwrap();

    let qc = QuorumCertificate::new(qcinfo, aggsig);

    let tmo_info = TimeoutInfo {
        round: Round(3),
        high_qc: qc,
    };

    let high_qc_round = HighQcRound { qc_round: Round(1) };
    let tc_round = Round(2);
    let mut hasher = HasherType::new();
    hasher.update(tc_round);
    hasher.update(high_qc_round.qc_round);
    let high_qc_round_hash = hasher.hash();

    let mut sigs = Vec::new();
    for (key, certkey) in keypairs.iter().zip(cert_keys.iter()) {
        let node_id = NodeId::new(key.pubkey());
        let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(high_qc_round_hash.as_ref(), certkey);
        sigs.push((node_id, sig));
    }
    let sigcol =
        SignatureCollectionType::new(sigs, &validator_mapping, high_qc_round_hash.as_ref())
            .unwrap();

    let high_qc_rounds = vec![HighQcRoundSigColTuple {
        high_qc_round,
        sigs: sigcol,
    }];

    let tc = TimeoutCertificate {
        round: tc_round,
        high_qc_rounds,
    };

    let timeout = Timeout {
        tminfo: tmo_info,
        last_round_tc: Some(tc),
    };

    let tmo = ConsensusMessage::Timeout(TimeoutMessage::new(timeout, author_certkey));

    let tmo_hash = HasherType::hash_object(&tmo);
    let unverified_message = Unverified::new(
        Unvalidated::new(tmo),
        author_keypair.sign(tmo_hash.as_ref()),
    );

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: author_keypair.pubkey(),
        unverified_message,
    });

    let mut bencher = MonadEventBencher::new(event);

    c.bench_function("bench_timeout", |b| {
        b.iter(|| {
            for _ in 0..N_VALIDATORS {
                bencher.append();
            }
        })
    });
}

fn bench_local_timeout(c: &mut Criterion) {
    let event: MonadEvent<SecpSignature, MultiSig<SecpSignature>> =
        MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(TimeoutVariant::Pacemaker));

    let mut bencher = MonadEventBencher::new(event);

    c.bench_function("bench_local_timeout", |b| {
        b.iter(|| {
            bencher.append();
        })
    });
}

criterion_group!(
    bench,
    bench_proposal,
    bench_vote,
    bench_timeout,
    bench_local_timeout,
);

#[cfg(target_os = "linux")]
criterion::criterion_main!(bench);

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("Linux only benchmark");
}
