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

//! This module is used to validate that a block contains the expected
//! valid systems transactions with the information from the block header.
//! `validate_and_extract_system_transactions()` is used to extract the
//! expected valid system transactions and verify that the user transactions
//! are not from the system sender and don't invoke any system calls.
//! `is_system_sender` and `is_restricted_system_call` should be used to reject
//! invalid transactions before they are included in a proposal (TxPool)

use std::collections::VecDeque;

use alloy_consensus::{Transaction, TxEnvelope, transaction::Recovered};
use alloy_primitives::{Address, TxKind};
use monad_chain_config::{ChainConfig, revision::ChainRevision};
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthExecutionProtocol, ExtractEthAddress};
use monad_validator::signature_collection::SignatureCollection;
use tracing::debug;

use crate::{SYSTEM_SENDER_ETH_ADDRESS, SystemCall, SystemTransaction, generate_system_calls};

#[derive(Debug)]
pub enum SystemTransactionError {
    UnexpectedSenderAddress,
    InvalidTxType,
    InvalidChainId,
    InvalidTxSignature,
    NonZeroGasPrice,
    NonZeroGasLimit,
    InvalidTxKind,
    UnexpectedDestAddress,
    UnexpectedInput,
    UnexpectedValue,
}

#[derive(Debug)]
pub enum SystemTransactionValidationError {
    MissingSystemTransaction,
    UnexpectedSystemTransaction,
    NonSequentialNonces,
    SystemTransactionError(SystemTransactionError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SystemTransactionValidator;

impl SystemTransactionValidator {
    // Used to validate sender of user transactions in RPC and TxPool
    pub fn is_system_sender(address: Address) -> bool {
        address == SYSTEM_SENDER_ETH_ADDRESS
    }

    // Used to validate inputs of user transactions in RPC and TxPool
    pub fn is_restricted_system_call(txn: &Recovered<TxEnvelope>) -> bool {
        SystemCall::is_restricted_system_call(txn)
    }

    fn static_validate_system_transaction<CCT, CRT>(
        txn: &Recovered<TxEnvelope>,
        chain_config: &CCT,
    ) -> Result<(), SystemTransactionError>
    where
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        if !Self::is_system_sender(txn.signer()) {
            return Err(SystemTransactionError::UnexpectedSenderAddress);
        }

        if !txn.tx().is_legacy() {
            return Err(SystemTransactionError::InvalidTxType);
        }

        // EIP-155
        if txn.tx().chain_id() != Some(chain_config.chain_id()) {
            return Err(SystemTransactionError::InvalidChainId);
        }

        // EIP-2
        if txn.signature().normalize_s().is_some() {
            return Err(SystemTransactionError::InvalidTxSignature);
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

        Ok(())
    }

    fn validate_system_transaction_input(
        expected_sys_call: SystemCall,
        sys_txn: Recovered<TxEnvelope>,
    ) -> Result<SystemTransaction, SystemTransactionError> {
        expected_sys_call.validate_system_transaction_input(sys_txn)
    }

    fn validate_system_transaction<CCT, CRT>(
        expected_sys_call: SystemCall,
        sys_txn: Recovered<TxEnvelope>,
        chain_config: &CCT,
    ) -> Result<SystemTransaction, SystemTransactionError>
    where
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        Self::static_validate_system_transaction(&sys_txn, chain_config)?;

        Self::validate_system_transaction_input(expected_sys_call, sys_txn)
    }

    // Used to extract expected systems transactions in block validator
    pub fn validate_and_extract_system_transactions<ST, SCT, CCT, CRT>(
        block_header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        mut txns: VecDeque<Recovered<TxEnvelope>>,
        chain_config: &CCT,
    ) -> Result<
        (Vec<SystemTransaction>, Vec<Recovered<TxEnvelope>>),
        SystemTransactionValidationError,
    >
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        CertificateSignaturePubKey<ST>: ExtractEthAddress,
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        let timestamp_s: u64 = (block_header.timestamp_ns / 1_000_000_000)
            .try_into()
            .unwrap_or(u64::MAX);

        if !chain_config
            .get_execution_chain_revision(timestamp_s)
            .execution_chain_params()
            .validate_system_txs
        {
            return Ok((Vec::new(), txns.into()));
        }

        let mut validated_sys_txns = Vec::new();

        let address = block_header.author.pubkey().get_eth_address();
        let expected_sys_calls = generate_system_calls(
            block_header.seq_num,
            block_header.epoch,
            block_header.qc.get_epoch(),
            address,
            chain_config,
        );
        let mut curr_sys_sender_nonce = None;
        for expected_sys_call in expected_sys_calls {
            let Some(sys_txn) = txns.pop_front() else {
                return Err(SystemTransactionValidationError::MissingSystemTransaction);
            };

            match Self::validate_system_transaction(expected_sys_call, sys_txn, chain_config) {
                Ok(validated_sys_txn) => {
                    // system sender nonce must be sequential
                    if let Some(old_nonce) = curr_sys_sender_nonce {
                        if validated_sys_txn.nonce() != old_nonce + 1 {
                            debug!(?validated_sys_txn, "invalid system transaction nonce");
                            return Err(SystemTransactionValidationError::NonSequentialNonces);
                        }
                    }
                    curr_sys_sender_nonce = Some(validated_sys_txn.nonce());

                    validated_sys_txns.push(validated_sys_txn);
                }
                Err(err) => {
                    return Err(SystemTransactionValidationError::SystemTransactionError(
                        err,
                    ));
                }
            }
        }

        for user_txn in &txns {
            if Self::is_system_sender(user_txn.signer()) {
                return Err(SystemTransactionValidationError::UnexpectedSystemTransaction);
            }

            if Self::is_restricted_system_call(user_txn) {
                return Err(SystemTransactionValidationError::UnexpectedSystemTransaction);
            }
        }

        Ok((validated_sys_txns, txns.into()))
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, transaction::Recovered};
    use alloy_eips::eip2930::AccessList;
    use alloy_primitives::{Address, B256, Bytes, TxKind};
    use alloy_signer::SignerSync;
    use alloy_signer_local::LocalSigner;
    use monad_chain_config::{ChainConfig, MockChainConfig};
    use monad_consensus_types::{
        block::ConsensusBlockHeader,
        payload::{ConsensusBlockBodyId, RoundSignature},
        quorum_certificate::QuorumCertificate,
    };
    use monad_crypto::{NopKeyPair, NopSignature, certificate_signature::CertificateKeyPair};
    use monad_eth_testutil::make_legacy_tx;
    use monad_eth_types::{EthExecutionProtocol, ProposedEthHeader};
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Epoch, GENESIS_SEQ_NUM, Hash, NodeId, Round, SeqNum};

    use crate::{
        SYSTEM_SENDER_ETH_ADDRESS, SYSTEM_SENDER_PRIV_KEY,
        test_utils::{get_valid_system_transaction, sign_with_system_sender},
        validator::{
            SystemTransactionError, SystemTransactionValidationError, SystemTransactionValidator,
        },
    };

    const BASE_FEE: u64 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    #[test]
    fn test_invalid_sender() {
        let tx = get_valid_system_transaction();
        let signature_hash = tx.signature_hash();
        let local_signer = LocalSigner::from_bytes(&B256::repeat_byte(1)).unwrap();
        let signature = local_signer.sign_hash_sync(&signature_hash).unwrap();
        let invalid_tx = Recovered::new_unchecked(
            TxEnvelope::Legacy(tx.into_signed(signature)),
            local_signer.address(),
        );

        assert!(matches!(
            SystemTransactionValidator::static_validate_system_transaction(
                &invalid_tx,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::UnexpectedSenderAddress)
        ));
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
            SystemTransactionValidator::static_validate_system_transaction(
                &recovered,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::InvalidTxType)
        ));
    }

    #[test]
    fn test_invalid_gas_price() {
        let mut tx = get_valid_system_transaction();
        tx.gas_price = 1;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            SystemTransactionValidator::static_validate_system_transaction(
                &invalid_tx,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::NonZeroGasPrice)
        ));
    }

    #[test]
    fn test_invalid_gas_limit() {
        let mut tx = get_valid_system_transaction();
        tx.gas_limit = 1;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            SystemTransactionValidator::static_validate_system_transaction(
                &invalid_tx,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::NonZeroGasLimit)
        ));
    }

    #[test]
    fn test_invalid_tx_kind() {
        let mut tx = get_valid_system_transaction();
        tx.to = TxKind::Create;
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            SystemTransactionValidator::static_validate_system_transaction(
                &invalid_tx,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::InvalidTxKind)
        ));
    }

    #[test]
    fn test_invalid_chain_id() {
        let mut tx = get_valid_system_transaction();
        tx.chain_id = None;
        let invalid_tx = sign_with_system_sender(tx.clone());
        assert!(matches!(
            SystemTransactionValidator::static_validate_system_transaction(
                &invalid_tx,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::InvalidChainId)
        ));

        tx.chain_id = Some(MockChainConfig::DEFAULT.chain_id() + 1);
        let invalid_tx = sign_with_system_sender(tx);
        assert!(matches!(
            SystemTransactionValidator::static_validate_system_transaction(
                &invalid_tx,
                &MockChainConfig::DEFAULT
            ),
            Err(SystemTransactionError::InvalidChainId)
        ));
    }

    #[test]
    fn test_unexpected_system_txn() {
        let unsigned_tx1 = make_legacy_tx(B256::repeat_byte(0xAu8), 0, 0, 0, 10);
        let signer = unsigned_tx1.recover_signer().unwrap();
        let tx1 = Recovered::new_unchecked(unsigned_tx1, signer);

        let tx2 = sign_with_system_sender(get_valid_system_transaction());

        let txs = vec![tx1, tx2].into();

        // no expected system calls generated with this block header
        let nop_keypair = NopKeyPair::from_bytes(&mut [0_u8; 32]).unwrap();
        let block_header: ConsensusBlockHeader<
            NopSignature,
            MockSignatures<NopSignature>,
            EthExecutionProtocol,
        > = ConsensusBlockHeader::new(
            NodeId::new(nop_keypair.pubkey()),
            Epoch(1),
            Round(1),
            Vec::new(), // delayed_execution_results
            ProposedEthHeader::default(),
            ConsensusBlockBodyId(Hash([0_u8; 32])),
            QuorumCertificate::genesis_qc(),
            GENESIS_SEQ_NUM + SeqNum(1),
            1,
            RoundSignature::new(Round(1), &nop_keypair),
            Some(BASE_FEE),
            Some(BASE_FEE_TREND),
            Some(BASE_FEE_MOMENT),
        );

        let result = SystemTransactionValidator::validate_and_extract_system_transactions(
            &block_header,
            txs,
            &MockChainConfig::DEFAULT,
        );
        assert!(matches!(
            result,
            Err(SystemTransactionValidationError::UnexpectedSystemTransaction)
        ));
    }
}
