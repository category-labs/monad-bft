use std::{collections::BTreeMap, marker::PhantomData};

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, proofs::calculate_transaction_root, Transaction,
    EMPTY_OMMER_ROOT_HASH,
};
use alloy_primitives::{Address, U256};
use alloy_rlp::Encodable;
use monad_consensus_types::{
    block::{BlockPolicy, ConsensusBlockHeader, ConsensusFullBlock},
    block_validator::{BlockValidationError, BlockValidator},
    payload::{
        ConsensusBlockBody, EthBlockBody, EthExecutionProtocol,
        ProposedEthHeader, BASE_FEE_PER_GAS, PROPOSAL_GAS_LIMIT, PROPOSAL_SIZE_LIMIT,
    },
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable};
use monad_eth_block_policy::{
    checked_sum, compute_txn_max_value, static_validate_transaction, EthBlockPolicy,
    EthValidatedBlock,
};
use monad_eth_types::{EthAddress, Nonce, TxEnvelopeWithSigner};
use monad_state_backend::StateBackend;
use tracing::warn;

type NonceMap = BTreeMap<EthAddress, Nonce>;
type TxnFeeMap = BTreeMap<EthAddress, U256>;
type ValidatedTxns = Vec<TxEnvelopeWithSigner>;

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct EthValidator<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    /// max number of txns to fetch
    pub tx_limit: usize,
    /// limit on cumulative gas from transactions in a block
    pub block_gas_limit: u64,
    /// chain id
    pub chain_id: u64,

    _phantom: PhantomData<(ST, SCT, SBT)>,
}

impl<ST, SCT, SBT> EthValidator<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn new(tx_limit: usize, block_gas_limit: u64, chain_id: u64) -> Self {
        Self {
            tx_limit,
            block_gas_limit,
            chain_id,
            _phantom: PhantomData,
        }
    }

    fn validate_block_body(
        &self,
        body: &ConsensusBlockBody<EthExecutionProtocol>,
    ) -> Result<(ValidatedTxns, NonceMap, TxnFeeMap), BlockValidationError> {
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
        if transactions.len() > self.tx_limit {
            return Err(BlockValidationError::TxnError);
        }

        // recovering the signers verifies that these are valid signatures
        let signers: Vec<Address> = transactions
            .iter()
            .map(|tx| tx.recover_signer().map_err(|_| BlockValidationError::TxnError))
            .collect::<Result<Vec<_>, _>>()?;

        // recover the account nonces and txn fee usage in this block
        let mut nonces = BTreeMap::new();
        let mut txn_fees: BTreeMap<EthAddress, U256> = BTreeMap::new();

        let mut validated_txns: ValidatedTxns = Vec::with_capacity(transactions.len());

        for (eth_txn, signer) in transactions.into_iter().zip(signers) {
            if static_validate_transaction(&eth_txn, self.chain_id).is_err() {
                return Err(BlockValidationError::TxnError);
            }

            // TODO(kai): currently block base fee is hardcoded
            // update this when base fee is included in consensus proposal
            if eth_txn.max_fee_per_gas() < BASE_FEE_PER_GAS.into() {
                return Err(BlockValidationError::TxnError);
            }

            let maybe_old_nonce = nonces.insert(EthAddress(signer), eth_txn.nonce());
            // txn iteration is following the same order as they are in the
            // block. A block is invalid if we see a smaller or equal nonce
            // after the first or if there is a nonce gap
            if let Some(old_nonce) = maybe_old_nonce {
                if eth_txn.nonce() != old_nonce + 1 {
                    return Err(BlockValidationError::TxnError);
                }
            }

            let txn_fee_entry = txn_fees.entry(EthAddress(signer)).or_insert(U256::ZERO);

            *txn_fee_entry = checked_sum(*txn_fee_entry, compute_txn_max_value(&eth_txn));
            validated_txns.push(TxEnvelopeWithSigner {
                transaction: eth_txn.clone(),
                signer,
            });
        }

        let total_gas = validated_txns
            .iter()
            .fold(0, |acc, tx| acc + tx.gas_limit());
        if total_gas > PROPOSAL_GAS_LIMIT {
            return Err(BlockValidationError::TxnError);
        }

        let proposal_size = validated_txns.iter().fold(0, |acc, tx| acc + tx.length());
        if proposal_size as u64 > PROPOSAL_SIZE_LIMIT {
            return Err(BlockValidationError::TxnError);
        }

        Ok((validated_txns, nonces, txn_fees))
    }

    fn validate_block_header(
        &self,
        header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: &ConsensusBlockBody<EthExecutionProtocol>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
    ) -> Result<(), BlockValidationError> {
        if header.block_body_id != body.get_id() {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }

        if let Some(author_pubkey) = author_pubkey {
            if let Err(e) = header.round_signature.verify(header.round, author_pubkey) {
                warn!("Invalid randao_reveal signature, reason: {:?}", e);
                return Err(BlockValidationError::RandaoError);
            };
        }

        let ProposedEthHeader {
            ommers_hash,
            beneficiary,
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
        if gas_limit != &PROPOSAL_GAS_LIMIT {
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
    SBT: StateBackend,
{
    fn validate(
        &self,
        header: ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: ConsensusBlockBody<EthExecutionProtocol>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
    ) -> Result<
        <EthBlockPolicy<ST, SCT> as BlockPolicy<
            ST,
            SCT,
            EthExecutionProtocol,
            SBT,
        >>::ValidatedBlock,
        BlockValidationError,
    >{
        self.validate_block_header(&header, &body, author_pubkey)?;

        if let Ok((validated_txns, nonces, txn_fees)) = self.validate_block_body(&body) {
            let block = ConsensusFullBlock::new(header, body)?;
            Ok(EthValidatedBlock {
                block,
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
    use alloy_primitives::B256;
    use monad_consensus_types::payload::FullTransactionList;
    use monad_eth_testutil::make_tx;
    use monad_eth_tx::EthFullTransactionList;

    use super::*;

    #[test]
    fn test_invalid_block_with_nonce_gap() {
        let block_validator = EthValidator::new(10, 100_000, 1337);

        // txn1 with nonce 1 while txn2 with nonce 3 (there is a nonce gap)
        let txn1 = make_tx(B256::repeat_byte(0xAu8), 1, 30_000, 1, 10);
        let txn2 = make_tx(B256::repeat_byte(0xAu8), 1, 30_000, 3, 10);

        // create a block with the above transactions
        let mut txs = Vec::new();
        txs.push(txn1.try_into_ecrecovered().unwrap_or_default());
        txs.push(txn2.try_into_ecrecovered().unwrap_or_default());
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();
        let full_txn_list = FullTransactionList::new(full_tx_list);
        let payload = Payload {
            txns: full_txn_list,
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_gas_limit() {
        let block_validator = EthValidator::new(10, 100_000, 1337);

        // total gas used is 120_000 which is higher than block gas limit
        let txn1 = make_tx(B256::repeat_byte(0xAu8), 1, 60_000, 1, 10);
        let txn2 = make_tx(B256::repeat_byte(0xAu8), 1, 60_000, 2, 10);

        // create a block with the above transactions
        let mut txs = Vec::new();
        txs.push(txn1.try_into_ecrecovered().unwrap_or_default());
        txs.push(txn2.try_into_ecrecovered().unwrap_or_default());
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();
        let full_txn_list = FullTransactionList::new(full_tx_list);
        let payload = Payload {
            txns: full_txn_list,
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_tx_limit() {
        let block_validator = EthValidator::new(1, 100_000, 1337);

        // tx limit per block is 1
        let txn1 = make_tx(B256::repeat_byte(0xAu8), 1, 30_000, 1, 10);
        let txn2 = make_tx(B256::repeat_byte(0xAu8), 1, 30_000, 2, 10);

        // create a block with the above transactions
        let mut txs = Vec::new();
        txs.push(txn1.try_into_ecrecovered().unwrap_or_default());
        txs.push(txn2.try_into_ecrecovered().unwrap_or_default());
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();
        let full_txn_list = FullTransactionList::new(full_tx_list);
        let payload = Payload {
            txns: full_txn_list,
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }
}
