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

use criterion::{criterion_group, criterion_main, Criterion};
use monad_bls::{BlsSignature, BlsSignatureCollection};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hasher, HasherType},
    signing_domain, NopSignature,
};
use monad_testutil::validators::create_keys_w_validators;
use monad_types::NodeId;
use monad_validator::validator_set::ValidatorSetFactory;

const N: u32 = 1000;

type SigningDomainType = signing_domain::Vote;
type SignatureType = NopSignature;
type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

fn criterion_benchmark(c: &mut Criterion) {
    let (keys, certkeys, _, validator_mapping) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(N, ValidatorSetFactory::default());
    let mut hasher = HasherType::new();
    hasher.update(b"hello world");
    let data = hasher.hash();

    // sign
    c.bench_function("bls_sign", |b| {
        b.iter(|| certkeys[0].sign::<SigningDomainType>(data.as_ref()))
    });

    let mut sigs = Vec::new();
    for (node_id, certkey) in keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certkeys.iter())
    {
        sigs.push((node_id, certkey.sign::<SigningDomainType>(data.as_ref())));
    }

    // aggregate N signatures
    c.bench_function("bls_aggregate_1000", |b| {
        b.iter_batched(
            || sigs.clone(),
            |sigs| {
                SignatureCollectionType::new::<SigningDomainType>(
                    sigs,
                    &validator_mapping,
                    data.as_ref(),
                )
                .unwrap()
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // validate valid signature
    c.bench_function("bls_validate_valid_sig", |b| {
        b.iter_batched(
            || certkeys[0].sign::<SigningDomainType>(data.as_ref()),
            |sig| <BlsSignature as CertificateSignature>::validate(&sig),
            criterion::BatchSize::SmallInput,
        );
    });

    // validate invalid signature
    c.bench_function("bls_validate_invalid_sig", |b| {
        b.iter_batched(
            || {
                let not_in_subgroup_bytes: [u8; 96] = [
                    0xac, 0xb0, 0x12, 0x4c, 0x75, 0x74, 0xf2, 0x81, 0xa2, 0x93, 0xf4, 0x18, 0x5c,
                    0xad, 0x3c, 0xb2, 0x26, 0x81, 0xd5, 0x20, 0x91, 0x7c, 0xe4, 0x66, 0x65, 0x24,
                    0x3e, 0xac, 0xb0, 0x51, 0x00, 0x0d, 0x8b, 0xac, 0xf7, 0x5e, 0x14, 0x51, 0x87,
                    0x0c, 0xa6, 0xb3, 0xb9, 0xe6, 0xc9, 0xd4, 0x1a, 0x7b, 0x02, 0xea, 0xd2, 0x68,
                    0x5a, 0x84, 0x18, 0x8a, 0x4f, 0xaf, 0xd3, 0x82, 0x5d, 0xaf, 0x6a, 0x98, 0x96,
                    0x25, 0xd7, 0x19, 0xcc, 0xd2, 0xd8, 0x3a, 0x40, 0x10, 0x1f, 0x4a, 0x45, 0x3f,
                    0xca, 0x62, 0x87, 0x8c, 0x89, 0x0e, 0xca, 0x62, 0x23, 0x63, 0xf9, 0xdd, 0xb8,
                    0xf3, 0x67, 0xa9, 0x1e, 0x84,
                ];
                BlsSignature::uncompress(&not_in_subgroup_bytes).unwrap()
            },
            |sig| <BlsSignature as CertificateSignature>::validate(&sig),
            criterion::BatchSize::SmallInput,
        );
    });

    // verify
    c.bench_function("bls_verify", |b| {
        b.iter_batched(
            || {
                SignatureCollectionType::new::<SigningDomainType>(
                    sigs.clone(),
                    &validator_mapping,
                    data.as_ref(),
                )
                .unwrap()
            },
            |sig_col| {
                sig_col
                    .verify::<SigningDomainType>(&validator_mapping, data.as_ref())
                    .unwrap()
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
