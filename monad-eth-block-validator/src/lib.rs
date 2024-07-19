use std::collections::BTreeMap;

use alloy_rlp::Decodable;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    block_validator::BlockValidator,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};
use monad_eth_tx::{EthSignedTransaction, EthTransaction};
use monad_eth_types::EthAddress;
use tracing::warn;

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
impl<SCT: SignatureCollection, SBT: StateBackend, RBCT: ReserveBalanceCacheTrait<SBT>>
    BlockValidator<SCT, EthBlockPolicy, SBT, RBCT> for EthValidator
{
    fn validate(
        &self,
        block: Block<SCT>,
    ) -> Option<<EthBlockPolicy as BlockPolicy<SCT, SBT, RBCT>>::ValidatedBlock> {
        // RLP decodes the txns
        let Ok(eth_txns) =
            Vec::<EthSignedTransaction>::decode(&mut block.payload.txns.bytes().as_ref())
        else {
            return None;
        };

        // recovering the signers verifies that these are valid signatures
        let signers = EthSignedTransaction::recover_signers(&eth_txns, eth_txns.len())?;

        // recover the account nonces in this block
        let mut nonces = BTreeMap::new();

        let mut validated_txns: Vec<EthTransaction> = Vec::with_capacity(eth_txns.len());

        for (eth_txn, signer) in eth_txns.into_iter().zip(signers) {
            eth_txn
                .chain_id()
                .and_then(|cid| (cid == self.chain_id).then_some(()))?;

            let maybe_old_nonce = nonces.insert(EthAddress(signer), eth_txn.nonce());
            // txn iteration is following the same order as they are in the
            // block. A block is invalid if we see a smaller or equal nonce
            // after the first
            if let Some(old_nonce) = maybe_old_nonce {
                if old_nonce >= eth_txn.nonce() {
                    return None;
                }
            }
            validated_txns.push(eth_txn.with_signer(signer));
        }

        if validated_txns.len() > self.tx_limit {
            return None;
        }

        let total_gas = validated_txns
            .iter()
            .fold(0, |acc, tx| acc + tx.gas_limit());
        if total_gas > self.block_gas_limit {
            return None;
        }

        Some(EthValidatedBlock {
            block,
            validated_txns,
            nonces, // (address -> highest txn nonce) in the block
        })
    }

    fn other_validation(
        &self,
        block: &<EthBlockPolicy as BlockPolicy<SCT, SBT, RBCT>>::ValidatedBlock,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> bool {
        if let Err(e) = block
            .block
            .payload
            .randao_reveal
            .verify::<SCT::SignatureType>(block.get_round(), author_pubkey)
        {
            warn!("Invalid randao_reveal signature, reason: {:?}", e);
            return false;
        };
        true
    }
}
