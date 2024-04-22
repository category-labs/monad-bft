use criterion::{criterion_group, criterion_main, Criterion};
use monad_crypto::hasher::{Hasher, HasherType};
use monad_secp::{KeyPair, SecpSignature};
use monad_testutil::signing::get_key;
use rand::{thread_rng, RngCore};

type SignatureType = SecpSignature;

fn criterion_benchmark(c: &mut Criterion) {
    let key = get_key::<SignatureType>(37);

    let mut hasher = HasherType::new();
    hasher.update(b"hello world");
    let data = hasher.hash();

    // keygen
    c.bench_function("secp_keygen", |b| {
        b.iter_batched(
            || {
                let mut bytes = [0_u8; 32];
                let mut rng = thread_rng();
                rng.fill_bytes(&mut bytes);
                bytes
            },
            |mut bytes| KeyPair::from_bytes(&mut bytes),
            criterion::BatchSize::SmallInput,
        )
    });

    // sign
    c.bench_function("secp_sign", |b| b.iter(|| key.sign(data.as_ref())));

    // recover pubkey
    c.bench_function("secp_recover", |b| {
        b.iter_batched(
            || key.sign(data.as_ref()),
            |sig| sig.recover_pubkey(data.as_ref()),
            criterion::BatchSize::SmallInput,
        )
    });

    // verify
    c.bench_function("secp_verify", |b| {
        b.iter_batched(
            || {
                let sig = key.sign(data.as_ref());
                let pubkey = sig.recover_pubkey(data.as_ref()).unwrap();
                assert_eq!(pubkey, key.pubkey());
                (sig, pubkey)
            },
            |(sig, pubkey)| pubkey.verify(data.as_ref(), &sig),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
