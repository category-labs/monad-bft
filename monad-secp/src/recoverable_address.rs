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

use alloy_consensus::TxEnvelope;
use alloy_primitives::{keccak256, Address};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message, Secp256k1,
};

use crate::Error;

pub trait RecoverableAddress {
    fn secp256k1_recover(&self) -> Result<Address, Error>;
}

impl RecoverableAddress for TxEnvelope {
    fn secp256k1_recover(&self) -> Result<Address, Error> {
        let signature_hash = self.signature_hash();
        let secp_message = Message::from_digest(*signature_hash.as_ref());

        let secp = Secp256k1::new();

        let signature = self.signature().as_bytes();
        let recid = self.signature().recid().to_byte();

        let recoverable_sig = RecoverableSignature::from_compact(
            &signature[0..64],
            RecoveryId::try_from(recid as i32)?,
        )?;

        let recovered_pubkey = secp.recover_ecdsa(secp_message, &recoverable_sig)?;
        let recovered_pubkey_bytes = recovered_pubkey.serialize_uncompressed();
        let recovered_hash = keccak256(&recovered_pubkey_bytes[1..]);
        Ok(Address::from_slice(&recovered_hash[12..]))
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        SignableTransaction, Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702,
        TxLegacy,
    };
    use alloy_primitives::{Bytes, Signature, U256};
    use alloy_signer::{k256::ecdsa::SigningKey, SignerSync};
    use alloy_signer_local::PrivateKeySigner;
    use rand::{thread_rng, Rng};
    use rstest::rstest;

    use super::*;

    fn generate_random_private_key() -> SigningKey {
        let mut rng = thread_rng();
        SigningKey::random(&mut rng)
    }

    fn generate_random_address() -> Address {
        let mut rng = thread_rng();
        let mut bytes = [0u8; 20];
        rng.fill(&mut bytes);
        Address::from(bytes)
    }

    fn create_legacy_tx() -> TxLegacy {
        TxLegacy {
            chain_id: Some(1),
            nonce: 42,
            gas_price: 20_000_000_000,
            gas_limit: 21_000,
            to: generate_random_address().into(),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::from(vec![0x12, 0x34, 0x56, 0x78]),
        }
    }

    fn create_eip2930_tx() -> TxEip2930 {
        TxEip2930 {
            chain_id: 1,
            nonce: 42,
            gas_price: 20_000_000_000,
            gas_limit: 21_000,
            to: generate_random_address().into(),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::from(vec![0x12, 0x34, 0x56, 0x78]),
            access_list: Default::default(),
        }
    }

    fn create_eip1559_tx() -> TxEip1559 {
        TxEip1559 {
            chain_id: 1,
            nonce: 42,
            max_priority_fee_per_gas: 1_500_000_000,
            max_fee_per_gas: 30_000_000_000,
            gas_limit: 100_000,
            to: generate_random_address().into(),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::from(vec![0x12, 0x34, 0x56, 0x78]),
            access_list: Default::default(),
        }
    }

    fn create_eip4844_tx() -> TxEip4844 {
        TxEip4844 {
            chain_id: 1,
            nonce: 42,
            max_priority_fee_per_gas: 1_500_000_000,
            max_fee_per_gas: 30_000_000_000,
            gas_limit: 100_000,
            to: generate_random_address(),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::from(vec![0x12, 0x34, 0x56, 0x78]),
            access_list: Default::default(),
            blob_versioned_hashes: vec![],
            max_fee_per_blob_gas: 1_000_000_000,
        }
    }

    fn create_eip7702_tx() -> TxEip7702 {
        TxEip7702 {
            chain_id: 1,
            nonce: 42,
            max_priority_fee_per_gas: 1_500_000_000,
            max_fee_per_gas: 30_000_000_000,
            gas_limit: 100_000,
            to: generate_random_address(),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::from(vec![0x12, 0x34, 0x56, 0x78]),
            access_list: Default::default(),
            authorization_list: vec![],
        }
    }

    enum TestTransaction {
        Legacy(TxLegacy),
        Eip2930(TxEip2930),
        Eip1559(TxEip1559),
        Eip4844(TxEip4844),
        Eip7702(TxEip7702),
    }

    impl TestTransaction {
        fn sign(self, signer: &PrivateKeySigner) -> TxEnvelope {
            fn sign_tx<T: SignableTransaction<Signature>>(
                tx: T,
                signer: &PrivateKeySigner,
            ) -> (T, Signature, alloy_primitives::FixedBytes<32>) {
                let signature_hash = tx.signature_hash();
                let signature = signer.sign_hash_sync(&signature_hash).unwrap();
                (tx, signature, signature_hash)
            }

            match self {
                TestTransaction::Legacy(tx) => {
                    let (tx, sig, hash) = sign_tx(tx, signer);
                    Signed::new_unchecked(tx, sig, hash).into()
                }
                TestTransaction::Eip2930(tx) => {
                    let (tx, sig, hash) = sign_tx(tx, signer);
                    Signed::new_unchecked(tx, sig, hash).into()
                }
                TestTransaction::Eip1559(tx) => {
                    let (tx, sig, hash) = sign_tx(tx, signer);
                    Signed::new_unchecked(tx, sig, hash).into()
                }
                TestTransaction::Eip4844(tx) => {
                    let (tx, sig, hash) = sign_tx(tx, signer);
                    let variant: TxEip4844Variant = TxEip4844Variant::TxEip4844(tx);
                    Signed::new_unchecked(variant, sig, hash).into()
                }
                TestTransaction::Eip7702(tx) => {
                    let (tx, sig, hash) = sign_tx(tx, signer);
                    Signed::new_unchecked(tx, sig, hash).into()
                }
            }
        }
    }

    #[rstest]
    #[case::legacy(TestTransaction::Legacy(create_legacy_tx()))]
    #[case::eip2930(TestTransaction::Eip2930(create_eip2930_tx()))]
    #[case::eip1559(TestTransaction::Eip1559(create_eip1559_tx()))]
    #[case::eip4844(TestTransaction::Eip4844(create_eip4844_tx()))]
    #[case::eip7702(TestTransaction::Eip7702(create_eip7702_tx()))]
    fn test_tx_envelope_recovery(#[case] test_tx: TestTransaction) {
        let pk = generate_random_private_key();
        let signer = PrivateKeySigner::from_signing_key(pk);
        let expected_address = signer.address();

        let signed_tx = test_tx.sign(&signer);

        let recovered_address = signed_tx.secp256k1_recover().unwrap();
        assert_eq!(recovered_address, expected_address);
    }

    #[test]
    fn test_invalid_signature_recovery() {
        let tx = create_legacy_tx();
        let signature_hash = tx.signature_hash();

        let invalid_r = U256::from_be_bytes([
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
        ]);
        let invalid_s = U256::from_be_bytes([
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
        ]);

        let invalid_sig = Signature::new(invalid_r, invalid_s, false);
        let signed_tx: TxEnvelope = Signed::new_unchecked(tx, invalid_sig, signature_hash).into();
        assert!(signed_tx.secp256k1_recover().is_err());
    }
}
