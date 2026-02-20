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

use alloy_consensus::{
    transaction::SignerRecoverable, SignableTransaction, Signed, TxEnvelope, TxLegacy,
};
use alloy_primitives::{Address, Bytes, Signature, U256};
use alloy_signer::{k256::ecdsa::SigningKey, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use criterion::{criterion_group, criterion_main, Criterion};
use monad_crypto::{
    hasher::{Hasher, HasherType},
    signing_domain,
};
use monad_secp::{KeyPair, RecoverableAddress, SecpSignature};
use monad_testutil::signing::get_key;
use rand::{thread_rng, RngCore};

type SigningDomainType = signing_domain::ConsensusMessage;
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
    c.bench_function("secp_sign", |b| {
        b.iter(|| key.sign::<SigningDomainType>(data.as_ref()))
    });

    // recover pubkey
    c.bench_function("secp_recover", |b| {
        b.iter_batched(
            || key.sign::<SigningDomainType>(data.as_ref()),
            |sig| sig.recover_pubkey::<SigningDomainType>(data.as_ref()),
            criterion::BatchSize::SmallInput,
        )
    });

    // verify
    c.bench_function("secp_verify", |b| {
        b.iter_batched(
            || {
                let sig = key.sign::<SigningDomainType>(data.as_ref());
                let pubkey = sig
                    .recover_pubkey::<SigningDomainType>(data.as_ref())
                    .unwrap();
                assert_eq!(pubkey, key.pubkey());
                (sig, pubkey)
            },
            |(sig, pubkey)| pubkey.verify::<SigningDomainType>(data.as_ref(), &sig),
            criterion::BatchSize::SmallInput,
        )
    });

    benchmark_tx_signature_verification(c);
}

fn create_legacy_tx() -> TxLegacy {
    let mut rng = thread_rng();
    let mut to_bytes = [0u8; 20];
    rng.fill_bytes(&mut to_bytes);

    let mut data = vec![0u8; 200];
    rng.fill_bytes(&mut data);

    TxLegacy {
        chain_id: Some(1),
        nonce: 42,
        gas_price: 20_000_000_000,
        gas_limit: 100_000,
        to: Address::from(to_bytes).into(),
        value: U256::from(1_000_000_000_000_000_000u64),
        input: Bytes::from(data),
    }
}

fn create_invalid_signature() -> Signature {
    let invalid_r = U256::from_be_bytes([
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff,
    ]);
    let invalid_s = U256::from_be_bytes([
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff,
    ]);
    Signature::new(invalid_r, invalid_s, false)
}

fn benchmark_tx_signature_verification(c: &mut Criterion) {
    let mut rng = thread_rng();

    c.bench_function("tx_secp256k1_verify_success", |b| {
        let pk = SigningKey::random(&mut rng);
        let signer = PrivateKeySigner::from_signing_key(pk);
        let tx = create_legacy_tx();
        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx: TxEnvelope = Signed::new_unchecked(tx, signature, signature_hash).into();

        b.iter(|| signed_tx.secp256k1_recover().unwrap())
    });

    c.bench_function("tx_secp256k1_verify_fail", |b| {
        let tx = create_legacy_tx();
        let signature_hash = tx.signature_hash();
        let invalid_sig = create_invalid_signature();
        let signed_tx: TxEnvelope = Signed::new_unchecked(tx, invalid_sig, signature_hash).into();

        b.iter(|| signed_tx.secp256k1_recover().is_err())
    });

    c.bench_function("tx_alloy_recover_success", |b| {
        let pk = SigningKey::random(&mut rng);
        let signer = PrivateKeySigner::from_signing_key(pk);
        let tx = create_legacy_tx();
        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx: TxEnvelope = Signed::new_unchecked(tx, signature, signature_hash).into();

        b.iter(|| signed_tx.recover_signer().unwrap())
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
