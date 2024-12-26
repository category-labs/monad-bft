use std::collections::BTreeMap;

use alloy_rlp::Decodable;
use monad_consensus_types::{
    block::{Block, BlockKind, BlockPolicy, BlockType},
    block_validator::{BlockValidationError, BlockValidator},
    payload::{Payload, TransactionPayload},
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};
use monad_eth_block_policy::{
    checked_sum, compute_txn_max_value, static_validate_transaction, EthBlockPolicy,
    EthValidatedBlock,
};
use monad_eth_types::{EthAddress, EthSignedTransaction, EthTransaction, Nonce, BASE_FEE_PER_GAS};
use monad_state_backend::StateBackend;
use reth_primitives::U256;
use tracing::warn;

type NonceMap = BTreeMap<EthAddress, Nonce>;
type TxnFeeMap = BTreeMap<EthAddress, U256>;
type ValidatedTxns = Vec<EthTransaction>;

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthValidator {
    /// max number of txns to fetch
    pub tx_limit: usize,
    /// limit on cumulative gas from transactions in a block
    pub block_gas_limit: u64,
    /// chain id
    pub chain_id: u64,
}

impl EthValidator {
    pub fn new(tx_limit: usize, block_gas_limit: u64, chain_id: u64) -> Self {
        Self {
            tx_limit,
            block_gas_limit,
            chain_id,
        }
    }

    fn validate_payload(
        &self,
        payload: &Payload,
    ) -> Result<(ValidatedTxns, NonceMap, TxnFeeMap), BlockValidationError> {
        if matches!(payload.txns, TransactionPayload::Null) {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }

        match &payload.txns {
            TransactionPayload::List(txns_rlp) => {
                // RLP decodes the txns
                let Ok(eth_txns) =
                    Vec::<EthSignedTransaction>::decode(&mut txns_rlp.bytes().as_ref())
                else {
                    return Err(BlockValidationError::TxnError);
                };

                // recovering the signers verifies that these are valid signatures
                let signers = EthSignedTransaction::recover_signers(&eth_txns, eth_txns.len())
                    .ok_or(BlockValidationError::TxnError)?;

                // recover the account nonces and txn fee usage in this block
                let mut nonces = BTreeMap::new();
                let mut txn_fees: BTreeMap<EthAddress, U256> = BTreeMap::new();

                let mut validated_txns: Vec<EthTransaction> = Vec::with_capacity(eth_txns.len());

                for (eth_txn, signer) in eth_txns.into_iter().zip(signers) {
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
                    validated_txns.push(eth_txn.with_signer(signer));
                }

                if validated_txns.len() > self.tx_limit {
                    return Err(BlockValidationError::TxnError);
                }

                let total_gas = validated_txns
                    .iter()
                    .fold(0, |acc, tx| acc + tx.gas_limit());
                if total_gas > self.block_gas_limit {
                    return Err(BlockValidationError::TxnError);
                }

                Ok((validated_txns, nonces, txn_fees))
            }
            TransactionPayload::Null => {
                unreachable!();
            }
        }
    }

    fn validate_block_header<SCT: SignatureCollection>(
        &self,
        block: &Block<SCT>,
        payload: &Payload,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
    ) -> Result<(), BlockValidationError> {
        if block.payload_id != payload.get_id() {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }

        if block.timestamp <= block.get_qc().get_timestamp() {
            // timestamps must be monotonically increasing
            return Err(BlockValidationError::TimestampError);
        }

        if let Some(author_pubkey) = author_pubkey {
            if let Err(e) = block
                .execution
                .randao_reveal
                .verify::<SCT::SignatureType>(block.get_round(), author_pubkey)
            {
                warn!("Invalid randao_reveal signature, reason: {:?}", e);
                return Err(BlockValidationError::RandaoError);
            };
        }
        Ok(())
    }
}

// FIXME: add specific error returns for the different failures
impl<SCT, SBT> BlockValidator<SCT, EthBlockPolicy, SBT> for EthValidator
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn validate(
        &self,
        block: Block<SCT>,
        payload: Payload,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
    ) -> Result<<EthBlockPolicy as BlockPolicy<SCT, SBT>>::ValidatedBlock, BlockValidationError>
    {
        match block.block_kind {
            BlockKind::Executable => {
                self.validate_block_header(&block, &payload, author_pubkey)?;

                if let Ok((validated_txns, nonces, txn_fees)) = self.validate_payload(&payload) {
                    Ok(EthValidatedBlock {
                        block,
                        orig_payload: payload,
                        validated_txns,
                        nonces,
                        txn_fees,
                    })
                } else {
                    Err(BlockValidationError::PayloadError)
                }
            }
            BlockKind::Null => {
                self.validate_block_header(&block, &payload, author_pubkey)?;
                Ok(EthValidatedBlock {
                    block,
                    orig_payload: payload,
                    validated_txns: Default::default(),
                    nonces: Default::default(), // (address -> highest txn nonce) in the block
                    txn_fees: Default::default(), // (address -> txn fee) in the block
                })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use alloy_primitives::B256;
    use monad_consensus_types::payload::FullTransactionList;
    use monad_eth_testutil::make_tx;
    use monad_eth_types::EthFullTransactionList;

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
            txns: TransactionPayload::List(full_txn_list),
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
            txns: TransactionPayload::List(full_txn_list),
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
            txns: TransactionPayload::List(full_txn_list),
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }
}
