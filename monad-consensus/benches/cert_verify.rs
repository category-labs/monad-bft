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

//! Benchmarks for QC/TC verification and QC formation at the abstraction
//! levels exercised by the consensus timeout/voting paths.
//!
//! Verify-side worst case: a node receives a burst of timeout messages
//! following a round that timed out, at the maximum validator-set size.
//! Each message carries a TC for the previous round; in the adversarial
//! case every TC is byte-distinct, evading the LRU cache, and the
//! per-message cost scales with the number of tip-round groups inside
//! the TC.
//!
//! Create-side worst case: leader optimistically trusts each vote
//! signature on receipt and only checks aggregate validity at QC
//! formation. When the BLS aggregate verify fails, an aggregation tree
//! recursively bisects to identify bad signers. The theoretical worst
//! case (f≥N/2 spread across leaf order, full 2N-1 traversal) is
//! unreachable in practice: QC formation aborts once bad sigs exceed
//! the BFT threshold. Bench sweeps the realistic regime up to ~N/3.
//!
//! The bench runs at both N=200 (pre-MIP-9 cap) and
//! N=`MAX_VALIDATOR_SET_SIZE` (post-MIP-9, current cap) so the safe-K
//! window can be compared. Cache-warm cases are omitted: warm hits are
//! µs-scale and not considered a budget concern.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use monad_bls::BlsSignatureCollection;
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::TimeoutMessage,
    },
    validation::{
        certificate_cache::CertificateCache,
        signing::{verify_qc, verify_tc, Unvalidated, Unverified, Verified},
    },
};
use monad_consensus_types::{
    block::MockExecutionProtocol,
    quorum_certificate::QuorumCertificate,
    timeout::{HighExtend, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::Vote,
    RoundCertificate,
};
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    signing_domain,
};
use monad_secp::SecpSignature;
use monad_testutil::validators::create_keys_w_validators;
use monad_types::{BlockId, Epoch, Hash, NodeId, Round, SeqNum, GENESIS_ROUND};
use monad_validator::{
    epoch_manager::EpochManager,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_mapping::ValidatorMapping,
    validator_set::{ValidatorSet, ValidatorSetFactory, ValidatorSetType, MAX_VALIDATOR_SET_SIZE},
    validators_epoch_mapping::ValidatorsEpochMapping,
    weighted_round_robin::WeightedRoundRobin,
};

type SignatureType = SecpSignature;
type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
type ExecutionProtocolType = MockExecutionProtocol;
type PubkeyType = CertificateSignaturePubKey<SignatureType>;
type ValidatorSetFactoryType = ValidatorSetFactory<PubkeyType>;
type Election = WeightedRoundRobin<PubkeyType>;

/// Validator-set sizes to compare. 200 = pre-MIP-9 cap, 300 = post-MIP-9 (current).
const N_VALUES: [u32; 2] = [200, MAX_VALIDATOR_SET_SIZE as u32];

/// Tip-round group counts to sweep. Tight 1..=5 covers the realistic
/// fragmentation regime where the 800 ms budget breaks; 10 included for slope
/// confirmation on the per-call cases.
const K_VALUES: [usize; 6] = [1, 2, 3, 4, 5, 10];

const TC_ROUND: Round = Round(1000);
const QC_ROUND: Round = Round(999);
const MSG_ROUND: Round = Round(1001);
const EPOCH: Epoch = Epoch(1);

struct Fixture {
    n: u32,
    keys: Vec<<SignatureType as CertificateSignature>::KeyPairType>,
    cert_keys: Vec<SignatureCollectionKeyPairType<SignatureCollectionType>>,
    validator_set: ValidatorSet<PubkeyType>,
    validator_mapping:
        ValidatorMapping<PubkeyType, SignatureCollectionKeyPairType<SignatureCollectionType>>,
    epoch_manager: EpochManager,
    val_epoch_map: ValidatorsEpochMapping<ValidatorSetFactoryType, SignatureCollectionType>,
    election: Election,
    leader: NodeId<PubkeyType>,
}

fn build_fixture(n: u32) -> Fixture {
    let (keys, cert_keys, validator_set, validator_mapping) =
        create_keys_w_validators::<SignatureType, SignatureCollectionType, _>(
            n,
            ValidatorSetFactory::default(),
        );

    let leader = NodeId::new(keys[0].pubkey());

    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(EPOCH, Round(0))]);

    // ValidatorsEpochMapping::insert needs an owned ValidatorMapping, and
    // ValidatorMapping is not Clone — so build a second copy from the same
    // keys for the epoch map.
    let mapping_for_epoch_map = ValidatorMapping::new(
        keys.iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(cert_keys.iter().map(|ck| ck.pubkey())),
    );
    let stake_list: Vec<_> = validator_set
        .get_members()
        .iter()
        .map(|(a, b)| (*a, *b))
        .collect();
    let mut val_epoch_map = ValidatorsEpochMapping::<
        ValidatorSetFactoryType,
        SignatureCollectionType,
    >::new(ValidatorSetFactory::default());
    val_epoch_map.insert(EPOCH, stake_list, mapping_for_epoch_map);

    Fixture {
        n,
        keys,
        cert_keys,
        validator_set,
        validator_mapping,
        epoch_manager,
        val_epoch_map,
        election: Election::default(),
        leader,
    }
}

fn build_qc_at(fix: &Fixture, round: Round) -> QuorumCertificate<SignatureCollectionType> {
    let vote = Vote {
        id: BlockId(Hash([round.0 as u8; 32])),
        round,
        epoch: EPOCH,
    };
    let msg = alloy_rlp::encode(vote);
    let sigs: Vec<_> = fix
        .keys
        .iter()
        .zip(fix.cert_keys.iter())
        .map(|(k, ck)| {
            (
                NodeId::new(k.pubkey()),
                <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<
                    signing_domain::Vote,
                >(&msg, ck),
            )
        })
        .collect();
    let sigcol =
        SignatureCollectionType::new::<signing_domain::Vote>(sigs, &fix.validator_mapping, &msg)
            .unwrap();
    QuorumCertificate::new(vote, sigcol)
}

/// Build a TC at TC_ROUND with `k_groups` distinct tip-round groups.
/// `tc.high_extend = HighExtend::Qc(high_extend_qc)` with `high_extend_qc.round = QC_ROUND`,
/// which matches the highest group's `high_qc_round` (also QC_ROUND).
/// `omit_validator` excludes one validator from signing — used to make a
/// series of TCs byte-distinct for the round-simulation cases.
fn build_tc(
    fix: &Fixture,
    high_extend_qc: &QuorumCertificate<SignatureCollectionType>,
    k_groups: usize,
    omit_validator: Option<usize>,
) -> TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
    assert!(k_groups >= 1);
    let high_extend =
        HighExtend::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::Qc(
            high_extend_qc.clone(),
        );

    let mut timeouts = Vec::with_capacity(fix.keys.len());
    for (i, (k, ck)) in fix.keys.iter().zip(fix.cert_keys.iter()).enumerate() {
        if Some(i) == omit_validator {
            continue;
        }
        // Validator i goes into group g = i % k_groups. Group g's high_qc_round
        // is QC_ROUND - g; group 0 holds the maximum (QC_ROUND), matching the
        // single shared high_extend QC.
        let g = (i % k_groups) as u64;
        let tinfo = TimeoutInfo {
            epoch: EPOCH,
            round: TC_ROUND,
            high_qc_round: Round(QC_ROUND.0 - g),
            high_tip_round: GENESIS_ROUND,
        };
        let timeout = Timeout::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::new(
            ck,
            tinfo,
            high_extend.clone(),
            false,
            None,
        );
        timeouts.push((NodeId::new(k.pubkey()), timeout));
    }

    TimeoutCertificate::new(EPOCH, TC_ROUND, &timeouts, &fix.validator_mapping)
        .expect("TimeoutCertificate::new")
}

/// Wrap a TC inside a TimeoutMessage, then inside a ConsensusMessage, then
/// sign with the outer secp keypair to produce a wire-format Unverified.
fn build_unverified_msg(
    fix: &Fixture,
    high_extend_qc: &QuorumCertificate<SignatureCollectionType>,
    tc: TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
    signer_idx: usize,
) -> Unverified<
    SignatureType,
    Unvalidated<ConsensusMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>>,
> {
    let outer_keypair = &fix.keys[signer_idx];
    let inner_cert_keypair = &fix.cert_keys[signer_idx];

    let tinfo = TimeoutInfo {
        epoch: EPOCH,
        round: MSG_ROUND,
        high_qc_round: QC_ROUND,
        high_tip_round: GENESIS_ROUND,
    };
    let timeout_msg =
        TimeoutMessage::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::new(
            inner_cert_keypair,
            tinfo,
            HighExtend::Qc(high_extend_qc.clone()),
            true,
            Some(RoundCertificate::Tc(tc)),
        );

    let consensus_msg =
        ConsensusMessage::<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
            version: 0,
            message: ProtocolMessage::Timeout(timeout_msg),
        };
    let verified =
        Verified::<SignatureType, _>::new(Unvalidated::new(consensus_msg), outer_keypair);
    verified.into()
}

fn bench_verify_qc_cold(c: &mut Criterion, fix: &Fixture) {
    let qc = build_qc_at(fix, QC_ROUND);
    let validators =
        |_e: Epoch, _r: Round| Ok((&fix.validator_set, &fix.validator_mapping, fix.leader));

    c.bench_function(&format!("verify_qc/N={}/cold", fix.n), |b| {
        b.iter_batched(
            CertificateCache::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::default,
            |mut cache| verify_qc(&mut cache, &validators, &qc).unwrap(),
            BatchSize::SmallInput,
        );
    });
}

fn bench_verify_tc_cold(c: &mut Criterion, fix: &Fixture) {
    let qc = build_qc_at(fix, QC_ROUND);
    let validators =
        |_e: Epoch, _r: Round| Ok((&fix.validator_set, &fix.validator_mapping, fix.leader));

    let mut group = c.benchmark_group(format!("verify_tc/N={}/cold", fix.n));
    group.sample_size(10);
    for &k in &K_VALUES {
        let tc = build_tc(fix, &qc, k, None);
        group.bench_with_input(BenchmarkId::new("groups", k), &tc, |b, tc| {
            b.iter_batched(
                CertificateCache::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::default,
                |mut cache| verify_tc(&mut cache, &validators, tc).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_round_full(c: &mut Criterion, fix: &Fixture) {
    let qc = build_qc_at(fix, QC_ROUND);

    let mut group = c.benchmark_group(format!("round_full/N={}_msgs", fix.n));
    group.sample_size(10);

    for &k in &K_VALUES {
        let messages: Vec<_> = (0..(fix.n as usize))
            .map(|i| {
                let tc = build_tc(fix, &qc, k, Some(i));
                build_unverified_msg(fix, &qc, tc, i)
            })
            .collect();
        group.bench_with_input(BenchmarkId::new("groups", k), &messages, |b, msgs| {
            b.iter_batched(
                || {
                    (
                        CertificateCache::<
                            SignatureType,
                            SignatureCollectionType,
                            ExecutionProtocolType,
                        >::default(),
                        msgs.clone(),
                    )
                },
                |(mut cache, msgs)| {
                    // Receiver starts at TC_ROUND (= 1000): the first
                    // timeout for MSG_ROUND (= 1001) triggers the TC
                    // verify on last_round_certificate. After handling
                    // it, consensus would advance current_round to
                    // MSG_ROUND, so subsequent timeouts in the burst
                    // skip the TC re-verify (current_round >=
                    // tminfo.round) and only re-verify high_extend's QC.
                    // This matches the real round-advance behavior.
                    let mut current_round = TC_ROUND;
                    for (i, unverified) in msgs.into_iter().enumerate() {
                        let sender = fix.keys[i].pubkey();
                        let verified = unverified
                            .verify(&fix.epoch_manager, &fix.val_epoch_map, &sender)
                            .unwrap();
                        verified
                            .validate(
                                &mut cache,
                                &fix.epoch_manager,
                                &fix.val_epoch_map,
                                &fix.election,
                                0,
                                current_round,
                            )
                            .unwrap();
                        current_round = MSG_ROUND;
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Bad-signature fraction sweep for QC creation. A leader optimistically
/// trusts each incoming vote signature; failure is detected only at
/// aggregate-verify time. With f bad sigs out of N, the aggregation
/// tree recursively bisects to identify them. f=0 hits the happy path
/// (single root verify); the sweep tops out at ~N/3, the BFT threshold
/// past which QC formation can no longer succeed.
fn bench_qc_create(c: &mut Criterion, fix: &Fixture) {
    let good_vote = Vote {
        id: BlockId(Hash([0xab; 32])),
        round: QC_ROUND,
        epoch: EPOCH,
    };
    let bad_vote = Vote {
        id: BlockId(Hash([0xcd; 32])),
        round: QC_ROUND,
        epoch: EPOCH,
    };
    let good_msg = alloy_rlp::encode(good_vote);
    let bad_msg = alloy_rlp::encode(bad_vote);

    let n = fix.n as usize;
    let bad_counts: [usize; 4] = [0, 1, 2, n / 3];

    let mut group = c.benchmark_group(format!("qc_create/N={}", fix.n));
    group.sample_size(10);

    for &num_bad in &bad_counts {
        let sigs: Vec<_> = fix
            .keys
            .iter()
            .zip(fix.cert_keys.iter())
            .enumerate()
            .map(|(i, (k, ck))| {
                let msg: &[u8] = if i < num_bad { &bad_msg } else { &good_msg };
                let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<
                    signing_domain::Vote,
                >(msg, ck);
                (NodeId::new(k.pubkey()), sig)
            })
            .collect();

        group.bench_with_input(BenchmarkId::new("num_bad", num_bad), &sigs, |b, sigs| {
            b.iter_batched(
                || sigs.clone(),
                |sigs| {
                    // Result intentionally discarded: num_bad>0 returns
                    // Err with the recovered bad-signer list; we measure
                    // the path cost, not the outcome.
                    let _ = SignatureCollectionType::new::<signing_domain::Vote>(
                        sigs,
                        &fix.validator_mapping,
                        &good_msg,
                    );
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    for &n in &N_VALUES {
        let fix = build_fixture(n);
        bench_verify_qc_cold(c, &fix);
        bench_verify_tc_cold(c, &fix);
        bench_round_full(c, &fix);
        bench_qc_create(c, &fix);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
