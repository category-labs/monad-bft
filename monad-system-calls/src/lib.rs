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
    SignableTransaction, Transaction, TxEnvelope, TxLegacy, transaction::Recovered,
};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;

const SYSTEM_SENDER_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));
const SYSTEM_SENDER_ETH_ADDRESS: Address =
    Address::new(hex!("0x6f49a8F621353f12378d0046E7d7e4b9B249DC9e"));

pub enum SystemCall {}

impl SystemCall {
    pub fn into_txn(self) -> Recovered<TxEnvelope> {
        // Placeholder transaction
        let transaction = TxLegacy {
            chain_id: Some(1337),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(Address::new([0_u8; 20])),
            value: Default::default(),
            input: Bytes::new(),
        };

        let signature_hash = transaction.signature_hash();
        let local_signer = LocalSigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();
        let signature = local_signer.sign_hash_sync(&signature_hash).unwrap();

        Recovered::new_unchecked(
            TxEnvelope::Legacy(transaction.into_signed(signature)),
            SYSTEM_SENDER_ETH_ADDRESS,
        )
    }
}

pub fn generate_system_transactions(sys_calls: Vec<SystemCall>) -> Vec<Recovered<TxEnvelope>> {
    sys_calls
        .into_iter()
        .map(|sys_call| sys_call.into_txn())
        .collect()
}

pub fn is_system_transaction(txn: &Recovered<TxEnvelope>) -> bool {
    txn.signer() == SYSTEM_SENDER_ETH_ADDRESS
}

#[derive(Debug)]
pub enum SystemTransactionError {
    InvalidTxType,
    NonZeroNonce,
    NonZeroGasPrice,
    NonZeroGasLimit,
    InvalidTxKind,
    NonZeroValue,
}

pub fn is_valid_system_transaction(
    txn: &Recovered<TxEnvelope>,
) -> Result<(), SystemTransactionError> {
    assert!(is_system_transaction(txn));

    if !txn.tx().is_legacy() {
        return Err(SystemTransactionError::InvalidTxType);
    }

    if txn.tx().nonce() != 0 {
        return Err(SystemTransactionError::NonZeroNonce);
    }

    if txn.tx().gas_price() != Some(0) {
        return Err(SystemTransactionError::NonZeroGasPrice);
    }

    if txn.tx().gas_limit() != 0 {
        return Err(SystemTransactionError::NonZeroGasLimit);
    }

    if !matches!(txn.tx().kind(), TxKind::Call(_)) {
        return Err(SystemTransactionError::InvalidTxKind);
    }

    if txn.tx().value() != U256::ZERO {
        return Err(SystemTransactionError::NonZeroValue);
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use alloy_consensus::{
        SignableTransaction, TxEip1559, TxEnvelope, TxLegacy, transaction::Recovered,
    };
    use alloy_eips::eip2930::AccessList;
    use alloy_primitives::{Address, Bytes, TxKind, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::LocalSigner;

    use crate::{
        SYSTEM_SENDER_ETH_ADDRESS, SYSTEM_SENDER_PRIV_KEY, SystemTransactionError,
        is_valid_system_transaction,
    };

    fn get_valid_system_transaction() -> TxLegacy {
        TxLegacy {
            chain_id: Some(1337),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(Address::new([0_u8; 20])),
            value: Default::default(),
            input: Bytes::new(),
        }
    }

    fn sign_with_system_sender(transaction: TxLegacy) -> Recovered<TxEnvelope> {
        let signature_hash = transaction.signature_hash();
        let local_signer = LocalSigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();
        let signature = local_signer.sign_hash_sync(&signature_hash).unwrap();

        Recovered::new_unchecked(
            TxEnvelope::Legacy(transaction.into_signed(signature)),
            SYSTEM_SENDER_ETH_ADDRESS,
        )
    }

    #[test]
    fn test_invalid_tx_type() {
        let transaction = TxEip1559 {
            chain_id: 1337,
            nonce: 0,
            max_fee_per_gas: 0,
            max_priority_fee_per_gas: 0,
            gas_limit: 0,
            to: TxKind::Call(Address::new([0_u8; 20])),
            value: Default::default(),
            access_list: AccessList(Vec::new()),
            input: Bytes::new(),
        };

        let signature_hash = transaction.signature_hash();
        let local_signer = LocalSigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();
        let signature = local_signer.sign_hash_sync(&signature_hash).unwrap();

        let recovered = Recovered::new_unchecked(
            TxEnvelope::Eip1559(transaction.into_signed(signature)),
            SYSTEM_SENDER_ETH_ADDRESS,
        );

        assert!(matches!(
            is_valid_system_transaction(&recovered),
            Err(SystemTransactionError::InvalidTxType)
        ));
    }

    #[test]
    fn test_invalid_nonce() {
        let mut tx = get_valid_system_transaction();
        tx.nonce = 1;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            is_valid_system_transaction(&invalid_tx),
            Err(SystemTransactionError::NonZeroNonce)
        ));
    }

    #[test]
    fn test_invalid_gas_price() {
        let mut tx = get_valid_system_transaction();
        tx.gas_price = 1;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            is_valid_system_transaction(&invalid_tx),
            Err(SystemTransactionError::NonZeroGasPrice)
        ));
    }

    #[test]
    fn test_invalid_gas_limit() {
        let mut tx = get_valid_system_transaction();
        tx.gas_limit = 1;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            is_valid_system_transaction(&invalid_tx),
            Err(SystemTransactionError::NonZeroGasLimit)
        ));
    }

    #[test]
    fn test_invalid_tx_kind() {
        let mut tx = get_valid_system_transaction();
        tx.to = TxKind::Create;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            is_valid_system_transaction(&invalid_tx),
            Err(SystemTransactionError::InvalidTxKind)
        ));
    }

    #[test]
    fn test_invalid_value() {
        let mut tx = get_valid_system_transaction();
        tx.value = U256::ONE;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            is_valid_system_transaction(&invalid_tx),
            Err(SystemTransactionError::NonZeroValue)
        ));
    }
}
