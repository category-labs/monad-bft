use core::fmt::Debug;

use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature};
use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};

use crate::{
    block::{Block, BlockPolicy, PassthruBlockPolicy},
    signature_collection::SignatureCollection,
};

pub trait BlockValidator<SCT, BPT, SBT, RBCT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
{
    fn validate(&self, block: Block<SCT>) -> Option<BPT::ValidatedBlock>;
    fn other_validation(
        &self,
        block: &BPT::ValidatedBlock,
        author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> bool;
}

impl<
        SCT: SignatureCollection,
        BPT: BlockPolicy<SCT, SBT, RBCT>,
        SBT: StateBackend,
        RBCT: ReserveBalanceCacheTrait<SBT>,
        T: BlockValidator<SCT, BPT, SBT, RBCT> + ?Sized,
    > BlockValidator<SCT, BPT, SBT, RBCT> for Box<T>
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

impl<SCT: SignatureCollection, SBT: StateBackend, RBCT: ReserveBalanceCacheTrait<SBT>>
    BlockValidator<SCT, PassthruBlockPolicy, SBT, RBCT> for MockValidator
{
    fn validate(
        &self,
        block: Block<SCT>,
    ) -> Option<<PassthruBlockPolicy as BlockPolicy<SCT, SBT, RBCT>>::ValidatedBlock> {
        Some(block)
    }

    fn other_validation(
        &self,
        _block: &<PassthruBlockPolicy as BlockPolicy<SCT, SBT, RBCT>>::ValidatedBlock,
        _author_pubkey: &<<SCT::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> bool {
        true
    }
}
