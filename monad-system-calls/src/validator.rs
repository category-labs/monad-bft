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
use alloy_primitives::{Address, Bytes, TxKind, U256};
use monad_chain_config::{ChainConfig, revision::ChainRevision};
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthExecutionProtocol, ExtractEthAddress, ValidatedTx};
use monad_types::Epoch;
use monad_validator::signature_collection::SignatureCollection;
use tracing::{debug, info, warn};

use crate::{SYSTEM_SENDER_ETH_ADDRESS, SystemCall, generate_system_calls};

#[derive(Debug)]
pub enum SystemTransactionError {
    UnexpectedSenderAddress,
    InvalidTxType,
    InvalidChainId,
    InvalidTxSignature,
    NonZeroGasPrice,
    NonZeroGasLimit,
    InvalidTxKind,
    UnexpectedDestAddress {
        expected: Address,
        actual: Option<Address>,
    },
    UnexpectedInput {
        expected_input: Bytes,
        actual_input: Bytes,
    },
    UnexpectedValue {
        expected_value: U256,
        actual_value: U256,
    },
}

#[derive(Debug)]
pub enum SystemTransactionValidationError {
    MissingSystemTransaction,
    UnexpectedSystemTransaction,
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

    /// Set as a public function for fuzzer integration but does not need to be called externally otherwise
    pub fn static_validate_system_transaction<CCT, CRT>(
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

        if !txn.inner().is_legacy() {
            return Err(SystemTransactionError::InvalidTxType);
        }

        // EIP-155
        if txn.inner().chain_id() != Some(chain_config.chain_id()) {
            return Err(SystemTransactionError::InvalidChainId);
        }

        // EIP-2
        if txn.signature().normalize_s().is_some() {
            return Err(SystemTransactionError::InvalidTxSignature);
        }

        if txn.inner().gas_price() != Some(0) {
            return Err(SystemTransactionError::NonZeroGasPrice);
        }

        if txn.inner().gas_limit() != 0 {
            return Err(SystemTransactionError::NonZeroGasLimit);
        }

        if !matches!(txn.inner().kind(), TxKind::Call(_)) {
            return Err(SystemTransactionError::InvalidTxKind);
        }

        Ok(())
    }

    // Used to extract statically validated system transactions in block validator
    pub fn extract_system_transactions<ST, SCT, CCT, CRT>(
        block_header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        mut recovered_txns: VecDeque<Recovered<TxEnvelope>>,
        chain_config: &CCT,
    ) -> Result<(Vec<ValidatedTx>, Vec<Recovered<TxEnvelope>>), SystemTransactionValidationError>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
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
            return Ok((Vec::new(), recovered_txns.into()));
        }

        let mut sys_txns = Vec::new();
        while let Some(txn) = recovered_txns.front() {
            if !Self::is_system_sender(txn.signer()) {
                break;
            }

            // encountered a transaction from the system sender
            // static validate system transaction
            if let Err(sys_txn_err) =
                SystemTransactionValidator::static_validate_system_transaction(txn, chain_config)
            {
                debug!(
                    ?txn,
                    ?sys_txn_err,
                    "system transaction failed static validation"
                );
                return Err(SystemTransactionValidationError::SystemTransactionError(
                    sys_txn_err,
                ));
            };

            sys_txns.push(ValidatedTx {
                tx: recovered_txns.pop_front().unwrap(),
                authorizations_7702: Vec::new(),
            });
        }

        for user_txn in &recovered_txns {
            if SystemTransactionValidator::is_system_sender(user_txn.signer()) {
                debug!(?user_txn, "unexpected system sender in user transactions");
                return Err(SystemTransactionValidationError::UnexpectedSystemTransaction);
            }

            if SystemTransactionValidator::is_restricted_system_call(user_txn) {
                debug!(?user_txn, "unexpected system call in user transaction");
                return Err(SystemTransactionValidationError::UnexpectedSystemTransaction);
            }
        }

        Ok((sys_txns, recovered_txns.into()))
    }

    // Used to validate system transaction input in block policy
    pub fn validate_system_transactions_input<ST, SCT, CCT, CRT>(
        block_header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        parent_block_epoch: Epoch,
        sys_txns: &Vec<ValidatedTx>,
        chain_config: &CCT,
    ) -> Result<(), SystemTransactionValidationError>
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
            assert!(
                sys_txns.is_empty(),
                "system transactions shouldn't be extracted from the block"
            );
            return Ok(());
        }

        let expected_sys_calls = generate_system_calls(
            block_header.seq_num,
            block_header.epoch,
            parent_block_epoch,
            block_header.author.pubkey().get_eth_address(),
            chain_config,
        );

        info!(
            ?expected_sys_calls,
            block_seq_num =? block_header.seq_num,
            block_epoch =? block_header.epoch,
            ?parent_block_epoch,
            "generated expected system calls"
        );

        if expected_sys_calls.len() != sys_txns.len() {
            warn!(
                ?sys_txns,
                ?expected_sys_calls,
                "unexpected system transactions length"
            );
            return Err(SystemTransactionValidationError::UnexpectedSystemTransaction);
        }

        for (expected_sys_call, sys_txn) in expected_sys_calls.into_iter().zip(sys_txns) {
            if let Err(sys_txn_error) = expected_sys_call.validate_system_transaction_input(sys_txn)
            {
                debug!(
                    ?expected_sys_call,
                    ?sys_txn_error,
                    "system transaction error"
                );
                return Err(SystemTransactionValidationError::SystemTransactionError(
                    sys_txn_error,
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::{
        SignableTransaction, TxEip1559, TxEnvelope,
        transaction::{Recovered, SignerRecoverable},
    };
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
    use monad_eth_types::{EthExecutionProtocol, ProposedEthHeader, ValidatedTx};
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
        let tx1 = ValidatedTx {
            tx: Recovered::new_unchecked(unsigned_tx1, signer),
            authorizations_7702: Vec::new(),
        };
        let tx2 = ValidatedTx {
            tx: sign_with_system_sender(get_valid_system_transaction()),
            authorizations_7702: Vec::new(),
        };

        let txs = vec![tx1, tx2];

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

        let result = SystemTransactionValidator::validate_system_transactions_input(
            &block_header,
            Epoch(1),
            &txs,
            &MockChainConfig::DEFAULT,
        );
        assert!(matches!(
            result,
            Err(SystemTransactionValidationError::UnexpectedSystemTransaction)
        ));
    }
}
