use alloy_rlp::Decodable;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockType},
    block_validator::BlockValidator,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature};
use monad_eth_tx::EthSignedTransaction;
use monad_eth_txpool::{EthBlockPolicy, EthValidatedBlock};
use tracing::warn;

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthValidator {
    /// max number of txns to fetch
    pub tx_limit: usize,
    /// limit on cumulative gas from transactions in a block
    pub block_gas_limit: u64,
}

impl EthValidator {
    pub fn new(tx_limit: usize, block_gas_limit: u64) -> Self {
        Self {
            tx_limit,
            block_gas_limit,
        }
    }
}

// FIXME: add specific error returns for the different failures
//
impl<SCT: SignatureCollection> BlockValidator<SCT, EthBlockPolicy> for EthValidator {
    fn validate(
        &self,
        block: Block<SCT>,
    ) -> Option<<EthBlockPolicy as BlockPolicy<SCT>>::ValidatedBlock> {
        // RLP decodes the txns
        let Ok(eth_txns) =
            Vec::<EthSignedTransaction>::decode(&mut block.payload.txns.bytes().as_ref())
        else {
            return None;
        };

        // recovering the signers verifies that these are valid signatures
        let _ = EthSignedTransaction::recover_signers(&eth_txns, eth_txns.len())?;

        if eth_txns.len() > self.tx_limit {
            return None;
        }

        let total_gas = eth_txns.iter().fold(0, |acc, tx| acc + tx.gas_limit());
        if total_gas > self.block_gas_limit {
            return None;
        }

        Some(EthValidatedBlock {
            block,
            validated_txns: eth_txns,
        })
    }

    fn other_validation(
        &self,
        block: &<EthBlockPolicy as BlockPolicy<SCT>>::ValidatedBlock,
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
