use std::fs::create_dir_all;

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
    block::TransactionList,
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::{SignatureBuilder, SignatureCollection},
    timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo},
    validation::{Hasher, Sha256Hash},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    secp256k1::{KeyPair, SecpSignature},
    GenericKeyPair, GenericSignature,
};
use monad_state::{ConsensusEvent, MonadEvent};
use monad_testutil::{block::setup_block, signing::get_key, validators::create_keys_w_validators};
use monad_types::{BlockId, Hash, NodeId, Round, Serializable};
use monad_validator::validator_property::ValidatorSetPropertyType;
use monad_wal::{
    wal::{WALogger, WALoggerConfig},
    PersistenceLogger,
};
use tempfile::{tempdir, TempDir};

const N_VALIDATORS: usize = 400;

type SignatureCollectionType = MultiSig<SecpSignature>;
type BenchEvent = MonadEvent<SecpSignature, SignatureCollectionType>;

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
    let (keypairs, _blskeypairs, votekeys, validators, validators_prop) =
        create_keys_w_validators::<SignatureCollectionType>(1);
    let author_keypair = &keypairs[0];

    let voters = keypairs
        .iter()
        .map(|key| NodeId(key.pubkey()))
        .zip(votekeys.iter());

    let blk = setup_block(
        NodeId(author_keypair.pubkey()),
        10,
        9,
        txns,
        voters.into_iter(),
        &validators_prop,
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

    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: lci,
    };

    let vote_msg = VoteMessage::<SignatureCollectionType> {
        vote,
        sig:<<SignatureCollectionType as SignatureCollection>::SignatureType as GenericSignature>::sign(Sha256Hash::hash_object(&vote).as_ref(), &keypair),
    };

    let consensus_vote = ConsensusMessage::Vote(vote_msg);

    let vote_hash = Sha256Hash::hash_object(&consensus_vote);
    let unverified_message = Unverified::new(consensus_vote, keypair.sign(vote_hash.as_ref()));

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
    let (keypairs, _blskeypairs, votekeys, _validators, validators_prop) =
        create_keys_w_validators::<SignatureCollectionType>(N_VALIDATORS as u32);
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

    let mut builder = SignatureBuilder::new();

    for keypair in votekeys.iter() {
        let idx = validators_prop
            .get_index(&NodeId(keypair.pubkey()))
            .unwrap();

        builder.add_signature(idx, keypair.sign(qcinfo_hash.as_ref()));
    }
    let aggsig = MultiSig::new(
        builder,
        validators_prop.get_voting_list(),
        qcinfo_hash.as_ref(),
    )
    .unwrap();

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
criterion::criterion_main!(bench);

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("Linux only benchmark");
}
