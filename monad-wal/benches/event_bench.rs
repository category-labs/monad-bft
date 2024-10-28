use std::fs::create_dir_all;

use bytes::Bytes;
use criterion::{criterion_group, Criterion};
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    ledger::CommitResult,
    payload::{ExecutionProtocol, FullTransactionList, TransactionPayload},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hash, Hasher, HasherType},
};
use monad_executor_glue::{ConsensusEvent, MonadEvent};
use monad_multi_sig::MultiSig;
use monad_secp::SecpSignature;
use monad_testutil::{
    block::setup_block,
    signing::{get_certificate_key, get_key},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, DontCare, Epoch, NodeId, Round, SeqNum, Serializable};
use monad_validator::validator_set::ValidatorSetFactory;
use monad_wal::{
    wal::{WALogger, WALoggerConfig},
    PersistenceLoggerBuilder,
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
        let config = WALoggerConfig::new(
            file_path, false, // sync
        );
        println!(
            "size of event: {}",
            Serializable::<Bytes>::serialize(&event).len()
        );
        Self {
            event,
            logger: config.build().unwrap(),
            _tmpdir: tmpdir,
        }
    }

    fn append(&mut self) {
        self.logger.push(&self.event).unwrap()
    }
}

fn bench_proposal(c: &mut Criterion) {
    let txns = TransactionPayload::List(FullTransactionList::new(vec![0x23_u8; 32 * 10000].into()));
    let validator_factory = ValidatorSetFactory::default();
    let (keypairs, _certkeypairs, _validators, validator_mapping) =
        create_keys_w_validators::<SignatureType, SignatureCollectionType, _>(1, validator_factory);
    let author_keypair = &keypairs[0];

    let (block, payload) = setup_block::<SignatureType, SignatureCollectionType>(
        NodeId::new(author_keypair.pubkey()),
        Round(10),
        Round(9),
        BlockId(Hash([43_u8; 32])),
        txns,
        ExecutionProtocol::dont_care(),
        &keypairs,
        &validator_mapping,
    );

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block,
        payload,
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: proposal,
    };
    let msg_hash = HasherType::hash_object(&conmsg);
    let unverified_message = Unverified::new(
        Unvalidated::new(conmsg),
        author_keypair.sign(msg_hash.as_ref()),
    );

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: NodeId::new(author_keypair.pubkey()),
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
        epoch: Epoch(1),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
        timestamp: 0,
    };

    let v = Vote {
        vote_info: vi,
        ledger_commit_info: CommitResult::NoCommit,
    };

    let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkey);

    let vote = ProtocolMessage::Vote(vm);
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: vote,
    };

    let msg_hash = HasherType::hash_object(&conmsg);
    let unverified_message = Unverified::new(Unvalidated::new(conmsg), <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg_hash.as_ref(), &keypair));

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: NodeId::new(keypair.pubkey()),
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
    let validator_factory = ValidatorSetFactory::default();
    let (keypairs, cert_keys, _validators, validator_mapping) =
        create_keys_w_validators::<SignatureType, SignatureCollectionType, _>(
            N_VALIDATORS as u32,
            validator_factory,
        );
    let author_keypair = &keypairs[0];
    let author_certkey = &cert_keys[0];

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        epoch: Epoch(1),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
        timestamp: 0,
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
        epoch: Epoch(1),
        round: Round(3),
        high_qc: qc,
    };

    let high_qc_round = HighQcRound { qc_round: Round(1) };
    let tc_epoch = Epoch(1);
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
        epoch: tc_epoch,
        round: tc_round,
        high_qc_rounds,
    };

    let timeout = Timeout {
        tminfo: tmo_info,
        last_round_tc: Some(tc),
    };

    let tmo = ProtocolMessage::Timeout(TimeoutMessage::new(timeout, author_certkey));
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: tmo,
    };
    let msg_hash = HasherType::hash_object(&conmsg);
    let unverified_message = Unverified::new(
        Unvalidated::new(conmsg),
        author_keypair.sign(msg_hash.as_ref()),
    );

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: NodeId::new(author_keypair.pubkey()),
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
        MonadEvent::ConsensusEvent(ConsensusEvent::Timeout);

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
