use std::collections::BTreeMap;

use alloy_consensus::transaction::Transaction;
use alloy_primitives::U256;
use alloy_rlp::Encodable;
use monad_consensus_types::{
    block::{Block, BlockKind, BlockPolicy, BlockType},
    block_validator::{BlockValidationError, BlockValidator},
    payload::{
        Payload, TransactionPayload, BASE_FEE_PER_GAS, PROPOSAL_GAS_LIMIT, PROPOSAL_SIZE_LIMIT,
    },
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};
use monad_eth_block_policy::{
    checked_sum, compute_txn_max_value, static_validate_transaction, EthBlockPolicy,
    EthValidatedBlock,
};
use monad_eth_tx::{EthFullTransactionList, EthTransaction};
use monad_eth_types::{EthAddress, Nonce};
use monad_state_backend::StateBackend;
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
    /// chain id
    pub chain_id: u64,
}

impl EthValidator {
    pub fn new(tx_limit: usize, chain_id: u64) -> Self {
        Self { tx_limit, chain_id }
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
                let Ok(EthFullTransactionList(eth_txns)) =
                    EthFullTransactionList::rlp_decode(txns_rlp.bytes().clone())
                else {
                    return Err(BlockValidationError::TxnError);
                };

                // early return if number of transactions exceed limit
                // no need to individually validate transactions
                if eth_txns.len() > self.tx_limit {
                    return Err(BlockValidationError::TxnError);
                }

                // recover the account nonces and txn fee usage in this block
                let mut nonces = BTreeMap::new();
                let mut txn_fees: BTreeMap<EthAddress, U256> = BTreeMap::new();

                let mut validated_txns: Vec<EthTransaction> = Vec::with_capacity(eth_txns.len());

                for eth_txn in eth_txns {
                    if static_validate_transaction(&eth_txn, self.chain_id).is_err() {
                        return Err(BlockValidationError::TxnError);
                    }

                    // TODO(kai): currently block base fee is hardcoded
                    // update this when base fee is included in consensus proposal
                    if eth_txn.max_fee_per_gas() < BASE_FEE_PER_GAS.into() {
                        return Err(BlockValidationError::TxnError);
                    }

                    let maybe_old_nonce =
                        nonces.insert(EthAddress(eth_txn.signer()), eth_txn.nonce());
                    // txn iteration is following the same order as they are in the
                    // block. A block is invalid if we see a smaller or equal nonce
                    // after the first or if there is a nonce gap
                    if let Some(old_nonce) = maybe_old_nonce {
                        if eth_txn.nonce() != old_nonce + 1 {
                            return Err(BlockValidationError::TxnError);
                        }
                    }

                    let txn_fee_entry = txn_fees
                        .entry(EthAddress(eth_txn.signer()))
                        .or_insert(U256::ZERO);

                    *txn_fee_entry = checked_sum(*txn_fee_entry, compute_txn_max_value(&eth_txn));
                    validated_txns.push(eth_txn);
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

    use super::*;

    const BASE_FEE: u128 = BASE_FEE_PER_GAS as u128;

    #[test]
    fn test_invalid_block_with_nonce_gap() {
        let block_validator = EthValidator::new(10, 1337);

        // txn1 with nonce 1 while txn2 with nonce 3 (there is a nonce gap)
        let txn1 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);
        let txn2 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 3, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let rlp_txs = alloy_rlp::encode(txs).into();
        let full_txn_list = FullTransactionList::new(rlp_txs);
        let payload = Payload {
            txns: TransactionPayload::List(full_txn_list),
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_gas_limit() {
        let block_validator = EthValidator::new(10, 1337);

        // total gas used is 400_000_000 which is higher than block gas limit
        let txn1 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, 200_000_000, 1, 10);
        let txn2 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, 200_000_000, 2, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let rlp_txs = alloy_rlp::encode(txs).into();
        let full_txn_list = FullTransactionList::new(rlp_txs);
        let payload = Payload {
            txns: TransactionPayload::List(full_txn_list),
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_tx_limit() {
        let block_validator = EthValidator::new(1, 1337);

        // tx limit per block is 1
        let txn1 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);
        let txn2 = make_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 2, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let rlp_txs = alloy_rlp::encode(txs).into();
        let full_txn_list = FullTransactionList::new(rlp_txs);
        let payload = Payload {
            txns: TransactionPayload::List(full_txn_list),
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_size_limit() {
        let block_validator = EthValidator::new(10, 1337);

        // proposal limit is 4MB
        let txn1 = make_tx(
            B256::repeat_byte(0xAu8),
            BASE_FEE,
            300_000_000,
            1,
            PROPOSAL_SIZE_LIMIT as usize,
        );

        // create a block with the above transactions
        let txs = vec![txn1];
        let rlp_txs = alloy_rlp::encode(txs).into();
        let full_txn_list = FullTransactionList::new(rlp_txs);
        let payload = Payload {
            txns: TransactionPayload::List(full_txn_list),
        };

        // block validation should return error
        let result = block_validator.validate_payload(&payload);
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }
}
