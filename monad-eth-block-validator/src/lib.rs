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

use std::{
    collections::{BTreeMap, VecDeque},
    marker::PhantomData,
};

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS,
    proofs::calculate_transaction_root,
    transaction::{Recovered, Transaction},
    TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use monad_consensus_types::{
    block::{BlockPolicy, ConsensusBlockHeader, ConsensusFullBlock},
    block_validator::{BlockValidationError, BlockValidator},
    payload::ConsensusBlockBody,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_txn_max_value, validation::static_validate_transaction, EthBlockPolicy,
    EthValidatedBlock,
};
use monad_eth_types::{
    EthBlockBody, EthExecutionProtocol, Nonce, ProposedEthHeader, BASE_FEE_PER_GAS,
};
use monad_secp::RecoverableAddress;
use monad_state_backend::StateBackend;
use monad_system_calls::{validator::SystemTransactionValidator, SystemTransaction};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{debug, trace_span, warn};

type NonceMap = BTreeMap<Address, Nonce>;
type TxnFeeMap = BTreeMap<Address, U256>;
type SystemTransactions = Vec<SystemTransaction>;
type ValidatedTxns = Vec<Recovered<TxEnvelope>>;

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct EthValidator<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    /// chain id
    pub chain_id: u64,

    _phantom: PhantomData<(ST, SCT, SBT)>,
}

impl<ST, SCT, SBT> EthValidator<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    pub fn new(chain_id: u64) -> Self {
        Self {
            chain_id,
            _phantom: PhantomData,
        }
    }

    fn validate_block_body(
        &self,
        header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: &ConsensusBlockBody<EthExecutionProtocol>,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        max_code_size: usize,
    ) -> Result<(SystemTransactions, ValidatedTxns, NonceMap, TxnFeeMap), BlockValidationError>
    {
        let EthBlockBody {
            transactions,
            ommers,
            withdrawals,
        } = &body.execution_body;

        if !ommers.is_empty() {
            return Err(BlockValidationError::PayloadError);
        }

        if !withdrawals.is_empty() {
            return Err(BlockValidationError::PayloadError);
        }

        // early return if number of transactions exceed limit
        // no need to individually validate transactions
        if transactions.len() > tx_limit {
            return Err(BlockValidationError::TxnError);
        }

        // recovering the signers verifies that these are valid signatures
        let recovered_txns: VecDeque<Recovered<TxEnvelope>> = transactions
            .into_par_iter()
            .map(|tx| {
                let _span = trace_span!("validator: recover signer").entered();
                let signer = tx.secp256k1_recover()?;
                Ok(Recovered::new_unchecked(tx.clone(), signer))
            })
            .collect::<Result<_, monad_secp::Error>>()
            .map_err(|_err| BlockValidationError::TxnError)?;

        let (system_txns, eth_txns) =
            match SystemTransactionValidator::validate_and_extract_system_transactions(
                header,
                recovered_txns,
            ) {
                Ok((system_txns, eth_txns)) => (system_txns, eth_txns),
                Err(err) => {
                    debug!(?err, "system transaction validator error");
                    return Err(BlockValidationError::SystemTxnError);
                }
            };

        // recover the account nonces for system txns
        let mut nonces = BTreeMap::new();

        // duplicate check. this is also done in SystemTransactionValidator
        for sys_txn in &system_txns {
            let maybe_old_nonce = nonces.insert(sys_txn.signer(), sys_txn.nonce());
            // A block is invalid if we see a smaller or equal nonce
            // after the first or if there is a nonce gap
            if let Some(old_nonce) = maybe_old_nonce {
                if sys_txn.nonce() != old_nonce + 1 {
                    return Err(BlockValidationError::SystemTxnError);
                }
            }
        }

        // recover the account nonces and txn fee for user txns
        let mut txn_fees: BTreeMap<Address, U256> = BTreeMap::new();

        for eth_txn in &eth_txns {
            if static_validate_transaction(
                eth_txn,
                self.chain_id,
                proposal_gas_limit,
                max_code_size,
            )
            .is_err()
            {
                return Err(BlockValidationError::TxnError);
            }

            // TODO(kai): currently block base fee is hardcoded
            // update this when base fee is included in consensus proposal
            if eth_txn.max_fee_per_gas() < BASE_FEE_PER_GAS.into() {
                return Err(BlockValidationError::TxnError);
            }

            let maybe_old_nonce = nonces.insert(eth_txn.signer(), eth_txn.nonce());
            // txn iteration is following the same order as they are in the
            // block. A block is invalid if we see a smaller or equal nonce
            // after the first or if there is a nonce gap
            if let Some(old_nonce) = maybe_old_nonce {
                if eth_txn.nonce() != old_nonce + 1 {
                    return Err(BlockValidationError::TxnError);
                }
            }

            let txn_fee_entry = txn_fees.entry(eth_txn.signer()).or_insert(U256::ZERO);

            *txn_fee_entry = txn_fee_entry.saturating_add(compute_txn_max_value(eth_txn));
        }

        let total_gas: u64 = eth_txns.iter().map(|tx| tx.gas_limit()).sum();
        let system_txns_size: usize = system_txns.iter().map(|tx| tx.length()).sum();
        let user_txns_size: usize = eth_txns.iter().map(|tx| tx.length()).sum();
        let proposal_size = system_txns_size + user_txns_size;
        debug!(
            total_gas,
            proposal_size,
            txs = transactions.len(),
            "proposal stats"
        );

        if total_gas > proposal_gas_limit {
            return Err(BlockValidationError::TxnError);
        }

        if proposal_size as u64 > proposal_byte_limit {
            return Err(BlockValidationError::TxnError);
        }

        Ok((system_txns, eth_txns, nonces, txn_fees))
    }

    fn validate_block_header(
        &self,
        header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: &ConsensusBlockBody<EthExecutionProtocol>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
        proposal_gas_limit: u64,
    ) -> Result<(), BlockValidationError> {
        if header.block_body_id != body.get_id() {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }

        if let Some(author_pubkey) = author_pubkey {
            if let Err(e) = header
                .round_signature
                .verify(header.block_round, author_pubkey)
            {
                warn!("Invalid randao_reveal signature, reason: {:?}", e);
                return Err(BlockValidationError::RandaoError);
            };
        }

        let ProposedEthHeader {
            ommers_hash,
            beneficiary: _,
            transactions_root,
            withdrawals_root,
            difficulty,
            number,
            gas_limit,
            timestamp,
            mix_hash,
            nonce,
            base_fee_per_gas,
            extra_data,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_block_root,
        } = &header.execution_inputs;

        if ommers_hash != EMPTY_OMMER_ROOT_HASH {
            return Err(BlockValidationError::HeaderError);
        }
        if transactions_root != calculate_transaction_root(&body.execution_body.transactions) {
            return Err(BlockValidationError::HeaderError);
        }
        if withdrawals_root != EMPTY_WITHDRAWALS {
            return Err(BlockValidationError::HeaderError);
        }
        if difficulty != &0 {
            return Err(BlockValidationError::HeaderError);
        }
        if number != &header.seq_num.0 {
            return Err(BlockValidationError::HeaderError);
        }
        if gas_limit != &proposal_gas_limit {
            return Err(BlockValidationError::HeaderError);
        }
        if u128::from(*timestamp) != header.timestamp_ns / 1_000_000_000 {
            return Err(BlockValidationError::HeaderError);
        }
        if *mix_hash != header.round_signature.get_hash().0 {
            return Err(BlockValidationError::HeaderError);
        }
        if nonce != &[0_u8; 8] {
            return Err(BlockValidationError::HeaderError);
        }
        if extra_data != &[0_u8; 32] {
            return Err(BlockValidationError::HeaderError);
        }
        if base_fee_per_gas != &BASE_FEE_PER_GAS {
            return Err(BlockValidationError::HeaderError);
        }
        if blob_gas_used != &0 {
            return Err(BlockValidationError::HeaderError);
        }
        if excess_blob_gas != &0 {
            return Err(BlockValidationError::HeaderError);
        }
        if parent_beacon_block_root != &[0_u8; 32] {
            return Err(BlockValidationError::HeaderError);
        }

        Ok(())
    }
}

// FIXME: add specific error returns for the different failures
impl<ST, SCT, SBT> BlockValidator<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
    for EthValidator<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    #[tracing::instrument(
        level = "debug", 
        skip_all,
        fields(seq_num = header.seq_num.as_u64())
    )]
    fn validate(
        &self,
        header: ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: ConsensusBlockBody<EthExecutionProtocol>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit:  u64,
        max_code_size: usize,
    ) -> Result<
        <EthBlockPolicy<ST, SCT> as BlockPolicy<
            ST,
            SCT,
            EthExecutionProtocol,
            SBT,
        >>::ValidatedBlock,
        BlockValidationError,
    >{
        self.validate_block_header(&header, &body, author_pubkey, proposal_gas_limit)?;

        if let Ok((system_txns, validated_txns, nonces, txn_fees)) = self.validate_block_body(
            &header,
            &body,
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            max_code_size,
        ) {
            let block = ConsensusFullBlock::new(header, body)?;
            Ok(EthValidatedBlock {
                block,
                system_txns,
                validated_txns,
                nonces,
                txn_fees,
            })
        } else {
            Err(BlockValidationError::PayloadError)
        }
    }
}

#[cfg(test)]
mod test {
    use alloy_consensus::Signed;
    use alloy_primitives::{PrimitiveSignature, B256};
    use monad_consensus_types::{
        payload::{ConsensusBlockBodyId, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
    };
    use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
    use monad_eth_testutil::make_legacy_tx;
    use monad_state_backend::InMemoryState;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};

    use super::*;

    const BASE_FEE: u128 = BASE_FEE_PER_GAS as u128;

    const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
    const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;

    fn get_header(
        payload_id: ConsensusBlockBodyId,
    ) -> ConsensusBlockHeader<NopSignature, MockSignatures<NopSignature>, EthExecutionProtocol>
    {
        let nop_keypair = NopKeyPair::from_bytes(&mut [0_u8; 32]).unwrap();
        ConsensusBlockHeader::new(
            NodeId::new(nop_keypair.pubkey()),
            Epoch(1),
            Round(1),
            Vec::new(), // delayed_execution_results
            ProposedEthHeader::default(),
            payload_id,
            QuorumCertificate::genesis_qc(),
            GENESIS_SEQ_NUM + SeqNum(1),
            1,
            RoundSignature::new(Round(1), &nop_keypair),
        )
    }

    #[test]
    fn test_invalid_block_with_nonce_gap() {
        let block_validator: EthValidator<
            NopSignature,
            MockSignatures<NopSignature>,
            InMemoryState<NopSignature, MockSignatures<NopSignature>>,
        > = EthValidator::new(1337);

        // txn1 with nonce 1 while txn2 with nonce 3 (there is a nonce gap)
        let txn1 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);
        let txn2 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 3, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result = block_validator.validate_block_body(
            &header,
            &payload,
            10,
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            0x6000,
        );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_gas_limit() {
        let block_validator: EthValidator<
            NopSignature,
            MockSignatures<NopSignature>,
            InMemoryState<NopSignature, MockSignatures<NopSignature>>,
        > = EthValidator::new(1337);

        // total gas used is 400_000_000 which is higher than block gas limit
        let txn1 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 200_000_000, 1, 10);
        let txn2 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 200_000_000, 2, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result = block_validator.validate_block_body(
            &header,
            &payload,
            10,
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            0x6000,
        );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_tx_limit() {
        let block_validator: EthValidator<
            NopSignature,
            MockSignatures<NopSignature>,
            InMemoryState<NopSignature, MockSignatures<NopSignature>>,
        > = EthValidator::new(1337);

        // tx limit per block is 1
        let txn1 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);
        let txn2 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 2, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result = block_validator.validate_block_body(
            &header,
            &payload,
            1,
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            0x6000,
        );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_size_limit() {
        let block_validator: EthValidator<
            NopSignature,
            MockSignatures<NopSignature>,
            InMemoryState<NopSignature, MockSignatures<NopSignature>>,
        > = EthValidator::new(1337);

        // proposal limit is 4MB
        let txn1 = make_legacy_tx(
            B256::repeat_byte(0xAu8),
            BASE_FEE,
            300_000_000,
            1,
            PROPOSAL_SIZE_LIMIT as usize,
        );

        // create a block with the above transactions
        let txs = vec![txn1];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result = block_validator.validate_block_body(
            &header,
            &payload,
            10,
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            0x6000,
        );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_eip2_signature() {
        let block_validator: EthValidator<
            NopSignature,
            MockSignatures<NopSignature>,
            InMemoryState<NopSignature, MockSignatures<NopSignature>>,
        > = EthValidator::new(1337);

        let valid_txn = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);

        // create a block with the above transaction
        let txs = vec![valid_txn.clone()];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return Ok
        let result = block_validator.validate_block_body(
            &header,
            &payload,
            10,
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            0x6000,
        );
        assert!(result.is_ok());

        // ECDSA signature is malleable
        // given a signature, we can form a second signature by computing additive inverse of s and flips v
        let original_signature = valid_txn.signature();
        let secp256k1_n = U256::from_str_radix(
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141",
            16,
        )
        .unwrap();
        let new_s = secp256k1_n.saturating_sub(original_signature.s());

        // form the new signature and transaction
        let invalid_signature = PrimitiveSignature::from_scalars_and_parity(
            original_signature.r().into(),
            new_s.into(),
            !original_signature.v(),
        );
        let inner_tx = valid_txn.as_legacy().unwrap().tx();
        let invalid_txn: TxEnvelope =
            Signed::new_unchecked(inner_tx.clone(), invalid_signature, *valid_txn.tx_hash()).into();

        // both transactions recover to the same signer
        assert_eq!(
            valid_txn.recover_signer().unwrap(),
            invalid_txn.recover_signer().unwrap()
        );

        // create a block with the above transaction
        let txs = vec![invalid_txn];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return Err
        let result = block_validator.validate_block_body(
            &header,
            &payload,
            10,
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            0x6000,
        );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    // TODO write tests for rest of eth-block-validator stuff
}
