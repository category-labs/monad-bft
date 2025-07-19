#[cfg(test)]
mod test {
    use std::str::FromStr;

    use alloy_consensus::{SignableTransaction, TxEip7702};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::{Address, Bytes, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::{k256::elliptic_curve::generic_array::sequence, SignerSync};
    use alloy_signer_local::PrivateKeySigner;
    use itertools::Itertools;
    use monad_crypto::NopSignature;
    use monad_eth_block_policy::{static_validate_transaction, EthBlockPolicy};
    use monad_eth_txpool_types::TransactionError;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Hash, SeqNum, GENESIS_SEQ_NUM};
    use tracing::{debug, trace, warn};

    use super::*;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    const EXEC_DELAY: u64 = 3;
    const RESERVE_BALANCE: u128 = 100_000_000_000_000_000_000;
    const CHAIN_ID: u64 = 1337;
    const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;

    fn make_authorization(nonce: u64, address: Address) -> SignedAuthorization {
        let auth = Authorization {
            chain_id: CHAIN_ID,
            address,
            nonce,
        };
        let signature = sign_tx(&auth.signature_hash());
        return auth.into_signed(signature);
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
