use core::fmt::Debug;

use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature};

use crate::{
    block::{Block, BlockPolicy, PassthruBlockPolicy},
    signature_collection::SignatureCollection,
    state::StateBackend,
};

pub trait BlockValidator<SCT: SignatureCollection, SBT: StateBackend, BPT: BlockPolicy<SCT, SBT>> {
    fn validate(&self, block: Block<SCT>) -> Option<BPT::ValidatedBlock>;
    fn other_validation(
        &self,
        block: &BPT::ValidatedBlock,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> bool;
}

impl<
        SCT: SignatureCollection,
        SBT: StateBackend,
        BPT: BlockPolicy<SCT, SBT>,
        T: BlockValidator<SCT, SBT, BPT> + ?Sized,
    > BlockValidator<SCT, SBT, BPT> for Box<T>
{
    fn validate(&self, block: Block<SCT>) -> Option<BPT::ValidatedBlock> {
        (**self).validate(block)
    }

    fn other_validation(
        &self,
        block: &BPT::ValidatedBlock,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> bool {
        (**self).other_validation(block, author_pubkey)
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl<SCT: SignatureCollection, SBT: StateBackend> BlockValidator<SCT, SBT, PassthruBlockPolicy>
    for MockValidator
{
    fn validate(
        &self,
        block: Block<SCT>,
    ) -> Option<<PassthruBlockPolicy as BlockPolicy<SCT, SBT>>::ValidatedBlock> {
        Some(block)
    }

    fn other_validation(
        &self,
        _block: &<PassthruBlockPolicy as BlockPolicy<SCT, SBT>>::ValidatedBlock,
        _author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> bool {
        true
    }
}
