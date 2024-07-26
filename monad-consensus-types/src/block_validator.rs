use core::fmt::Debug;

use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature};
use monad_eth_reserve_balance::{PassthruReserveBalanceCache, ReserveBalanceCacheTrait};

use crate::{
    block::{Block, BlockPolicy, PassthruBlockPolicy},
    signature_collection::SignatureCollection,
};

pub enum BlockValidationError {
    TxnValidationError,
    RandaoRevealSigError,
}

//pub trait BlockValidator<SCT: SignatureCollection, RBCT: ReserveBalanceCacheTrait, BPT: BlockPolicy<SCT, RBCT>> {
pub trait BlockValidator<SCT, RBCT, BPT>
where
    SCT: SignatureCollection,
    RBCT: ReserveBalanceCacheTrait,
    BPT: BlockPolicy<SCT, RBCT>,
{
    fn validate(&self, block: Block<SCT>) -> Option<BPT::ValidatedBlock>;
    fn other_validation(
        &self,
        block: &BPT::ValidatedBlock,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<(), BlockValidationError>;
    fn all_validation(
        &self,
        block: Block<SCT>,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<BPT::ValidatedBlock, BlockValidationError> {
        let Some(validated_block) = self.validate(block) else {
            return Err(BlockValidationError::TxnValidationError);
        };
        self.other_validation(&validated_block, author_pubkey)
            .and(Ok(validated_block))
    }
}

impl<
        SCT: SignatureCollection,
        RBCT: ReserveBalanceCacheTrait,
        BPT: BlockPolicy<SCT, RBCT>,
        T: BlockValidator<SCT, RBCT, BPT> + ?Sized,
    > BlockValidator<SCT, RBCT, BPT> for Box<T>
{
    fn validate(&self, block: Block<SCT>) -> Option<BPT::ValidatedBlock> {
        (**self).validate(block)
    }

    fn other_validation(
        &self,
        block: &BPT::ValidatedBlock,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<(), BlockValidationError> {
        (**self).other_validation(block, author_pubkey)
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl<SCT: SignatureCollection> BlockValidator<SCT, PassthruReserveBalanceCache, PassthruBlockPolicy>
    for MockValidator
{
    fn validate(
        &self,
        block: Block<SCT>,
    ) -> Option<
        <PassthruBlockPolicy as BlockPolicy<SCT, PassthruReserveBalanceCache>>::ValidatedBlock,
    > {
        Some(block)
    }

    fn other_validation(
        &self,
        _block: &<PassthruBlockPolicy as BlockPolicy<SCT, PassthruReserveBalanceCache>>::ValidatedBlock,
        _author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<(), BlockValidationError> {
        Ok(())
    }
}
