use std::collections::BTreeMap;

use alloy_rlp::Decodable;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    block_validator::{BlockValidationError, BlockValidator},
    payload::TransactionPayload,
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthSignedTransaction, EthTransaction};
use monad_eth_types::EthAddress;
use monad_state_backend::StateBackend;
use tracing::warn;

// allow for more fine grain debugging if needed
#[derive(Debug)]
pub enum TransactionError {
    InvalidChainId,
    MaxPriorityFeeTooHigh,
    InitCodeLimitExceeded,
    GasLimitTooLow,
}

/// Stateless helper function to check validity of an Ethereum transaction
pub fn static_validate_transaction(
    tx: &EthSignedTransaction,
    chain_id: u64,
) -> Result<(), TransactionError> {
    // EIP-155
    tx.chain_id()
        .and_then(|cid| (cid == chain_id).then_some(()))
        .ok_or(TransactionError::InvalidChainId)?;

    // EIP-1559
    if let Some(max_priority_fee) = tx.max_priority_fee_per_gas() {
        if max_priority_fee > tx.max_fee_per_gas() {
            return Err(TransactionError::MaxPriorityFeeTooHigh);
        }
    }

    // EIP-3860
    const DATA_SIZE_LIMIT: usize = 2 * 0x6000;
    if tx.to().is_some() && tx.input().len() > DATA_SIZE_LIMIT {
        return Err(TransactionError::InitCodeLimitExceeded);
    }

    // YP eq. 62 - intrinsic gas validation
    const INTRINSIC_GAS: u64 = 21000;
    if tx.gas_limit() < INTRINSIC_GAS {
        return Err(TransactionError::GasLimitTooLow);
    }

    Ok(())
}

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
        author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<<EthBlockPolicy as BlockPolicy<SCT, SBT>>::ValidatedBlock, BlockValidationError>
    {
        match &block.payload.txns {
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

                // recover the account nonces in this block
                let mut nonces = BTreeMap::new();

                let mut validated_txns: Vec<EthTransaction> = Vec::with_capacity(eth_txns.len());

                for (eth_txn, signer) in eth_txns.into_iter().zip(signers) {
                    if static_validate_transaction(&eth_txn, self.chain_id).is_err() {
                        return Err(BlockValidationError::TxnError);
                    }

                    // TODO(kai): currently block base fee is hardcoded to 1000 in monad-ledger
                    // update this when base fee is included in consensus proposal
                    if eth_txn.max_fee_per_gas() < 1000 {
                        return Err(BlockValidationError::TxnError);
                    }

                    let maybe_old_nonce = nonces.insert(EthAddress(signer), eth_txn.nonce());
                    // txn iteration is following the same order as they are in the
                    // block. A block is invalid if we see a smaller or equal nonce
                    // after the first
                    if let Some(old_nonce) = maybe_old_nonce {
                        if old_nonce >= eth_txn.nonce() {
                            return Err(BlockValidationError::TxnError);
                        }
                    }
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

                if let Err(e) = block
                    .payload
                    .randao_reveal
                    .verify::<SCT::SignatureType>(block.get_round(), author_pubkey)
                {
                    warn!("Invalid randao_reveal signature, reason: {:?}", e);
                    return Err(BlockValidationError::RandaoError);
                };

                Ok(EthValidatedBlock {
                    block,
                    validated_txns,
                    nonces, // (address -> highest txn nonce) in the block
                })
            }
            TransactionPayload::Empty => {
                if let Err(e) = block
                    .payload
                    .randao_reveal
                    .verify::<SCT::SignatureType>(block.get_round(), author_pubkey)
                {
                    warn!("Invalid randao_reveal signature, reason: {:?}", e);
                    return Err(BlockValidationError::RandaoError);
                };
                Ok(EthValidatedBlock {
                    block,
                    validated_txns: Default::default(),
                    nonces: Default::default(), // (address -> highest txn nonce) in the block
                })
            }
        }
    }
}
