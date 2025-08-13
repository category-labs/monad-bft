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

#[cfg(test)]
mod test {
    use alloy_consensus::{SignableTransaction, TxEip7702};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::{Address, FixedBytes, PrimitiveSignature, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use itertools::Itertools;
    use monad_eth_block_policy::static_validate_transaction;
    use monad_eth_txpool_types::TransactionError;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    const CHAIN_ID: u64 = 1337;
    const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;

    fn make_authorization(nonce: u64, address: Address) -> SignedAuthorization {
        let auth = Authorization {
            chain_id: CHAIN_ID,
            address,
            nonce,
        };
        let signature = sign_tx(&auth.signature_hash());
        auth.into_signed(signature)
    }

    #[test]
    fn test_static_validate_empty_authorization_list() {
        let tx = TxEip7702 {
            chain_id: CHAIN_ID,
            nonce: 0,
            gas_limit: PROPOSAL_GAS_LIMIT,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 0,
            authorization_list: vec![],
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let txn = tx.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::InvalidSetCodeTx)));
    }

    #[test]
    fn test_static_validate_no_authorization_list() {
        let tx = TxEip7702 {
            chain_id: CHAIN_ID,
            nonce: 0,
            gas_limit: PROPOSAL_GAS_LIMIT,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 0,
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let txn = tx.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::InvalidSetCodeTx)));
    }

    #[test]
    fn test_static_validate_too_large_authorization_list() {
        let nonces = 0..20;

        let tx = TxEip7702 {
            chain_id: CHAIN_ID,
            nonce: 0,
            gas_limit: PROPOSAL_GAS_LIMIT,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 0,
            authorization_list: nonces
                .map(|nonce| make_authorization(nonce, Address(FixedBytes([0x11; 20]))))
                .collect_vec(),
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let txn = tx.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::InvalidSetCodeTx)));
    }
}
