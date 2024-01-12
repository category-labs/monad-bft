use criterion::{criterion_group, criterion_main, Criterion};
use monad_consensus_types::{
    bls::BlsSignatureCollection, signature_collection::SignatureCollection,
};
use monad_crypto::{
    certificate_signature::CertificateSignaturePubKey,
    hasher::{Hasher, HasherType},
    NopSignature,
};
use monad_testutil::validators::create_keys_w_validators;
use monad_types::NodeId;

const N: u32 = 1000;

type SignatureType = NopSignature;
type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

fn criterion_benchmark(c: &mut Criterion) {
    let (keys, certkeys, _, validator_mapping) =
        create_keys_w_validators::<SignatureType, SignatureCollectionType>(N);
    let mut hasher = HasherType::new();
    hasher.update(b"hello world");
    let data = hasher.hash();

    // sign
    c.bench_function("bls_sign", |b| b.iter(|| certkeys[0].sign(data.as_ref())));

    let mut sigs = Vec::new();
    for (node_id, certkey) in keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certkeys.iter())
    {
        sigs.push((node_id, certkey.sign(data.as_ref())));
    }

    // aggregate N signatures
    c.bench_function("bls_aggregate_1000", |b| {
        b.iter_batched(
            || sigs.clone(),
            |sigs| SignatureCollectionType::new(sigs, &validator_mapping, data.as_ref()).unwrap(),
            criterion::BatchSize::SmallInput,
        )
    });

    // verify
    c.bench_function("bls_verify", |b| {
        b.iter_batched(
            || {
                SignatureCollectionType::new(sigs.clone(), &validator_mapping, data.as_ref())
                    .unwrap()
            },
            |sig_col| sig_col.verify(&validator_mapping, data.as_ref()).unwrap(),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
