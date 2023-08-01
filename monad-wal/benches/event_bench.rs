use std::fs::create_dir_all;

#[cfg(target_os = "linux")]
use criterion::criterion_main;
use criterion::{criterion_group, Criterion};
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    pacemaker::PacemakerTimerExpire,
    validation::signing::Unverified,
};
use monad_consensus_types::{
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, TransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo},
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::secp256k1::{KeyPair, SecpSignature};
use monad_state::{ConsensusEvent, MonadEvent};
use monad_testutil::{
    block::setup_block,
    signing::{create_keys, get_key},
};
use monad_types::{BlockId, Hash, NodeId, Round, Serializable};
use monad_wal::{
    wal::{WALogger, WALoggerConfig},
    PersistenceLogger,
};
use tempfile::{tempdir, TempDir};

const N_VALIDATORS: usize = 400;

type BenchEvent = MonadEvent<SecpSignature, MultiSig<SecpSignature>>;
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
        let config = WALoggerConfig { file_path };
        println!("size of event: {}", event.serialize().len());
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
    let txns = TransactionList(vec![0x23_u8; 32 * 10000]);
    let keypairs = create_keys(1);
    let author_keypair = &keypairs[0];

    let blk = setup_block(
        NodeId(author_keypair.pubkey()),
        10,
        9,
        txns,
        ExecutionArtifacts::zero(),
        &keypairs,
    );

    let proposal = ConsensusMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: None,
    });
    let proposal_hash = Sha256Hash::hash_object(&proposal);
    let unverified_message = Unverified::new(proposal, author_keypair.sign(proposal_hash.as_ref()));

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: author_keypair.pubkey(),
        unverified_message,
    });

    let mut bencher = MonadEventBencher::new(event);

    c.bench_function("bench_proposal", |b| b.iter(|| bencher.append()));
}

fn bench_vote(c: &mut Criterion) {
    let keypair: KeyPair = get_key(1);
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
    };
    let lci = LedgerCommitInfo {
        commit_state_hash: None,
        vote_info_hash: Hash([42_u8; 32]),
    };
    let vote = ConsensusMessage::Vote(VoteMessage {
        vote_info: vi,
        ledger_commit_info: lci,
    });

    let vote_hash = Sha256Hash::hash_object(&vote);
    let unverified_message = Unverified::new(vote, keypair.sign(vote_hash.as_ref()));

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
    let keypairs = create_keys(N_VALIDATORS as u32);
    let author_keypair = &keypairs[0];

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
    };
    let lci = LedgerCommitInfo::new::<Sha256Hash>(None, &vi);

    let qcinfo = QcInfo {
        vote: vi,
        ledger_commit: lci,
    };

    let qcinfo_hash = Sha256Hash::hash_object(&qcinfo.ledger_commit);

    let mut aggsig = MultiSig::new();
    for keypair in keypairs.iter() {
        aggsig.add_signature(keypair.sign(qcinfo_hash.as_ref()));
    }

    let qc = QuorumCertificate::new(qcinfo, aggsig);

    let tmo_info = TimeoutInfo {
        round: Round(3),
        high_qc: qc,
    };

    let high_qc_round = HighQcRound { qc_round: Round(1) };
    let tc_round = Round(2);
    let mut hasher = Sha256Hash::new();
    hasher.update(tc_round);
    hasher.update(high_qc_round.qc_round);
    let high_qc_round_hash = hasher.hash();

    let mut high_qc_rounds = Vec::new();
    for keypair in keypairs.iter() {
        high_qc_rounds.push(HighQcRoundSigTuple {
            high_qc_round,
            author_signature: keypair.sign(high_qc_round_hash.as_ref()),
        });
    }

    let tc = TimeoutCertificate {
        round: tc_round,
        high_qc_rounds,
    };

    let tmo = ConsensusMessage::Timeout(TimeoutMessage {
        tminfo: tmo_info,
        last_round_tc: Some(tc),
    });

    let tmo_hash = Sha256Hash::hash_object(&tmo);
    let unverified_message = Unverified::new(tmo, author_keypair.sign(tmo_hash.as_ref()));

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
        MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(PacemakerTimerExpire {}));

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
criterion_main!(bench);

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("Linux only benchmark");
}
