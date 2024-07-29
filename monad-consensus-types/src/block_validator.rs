use core::fmt::Debug;

use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};

use crate::{
    block::{Block, BlockPolicy, PassthruBlockPolicy},
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};

// TODO these are eth-specific types... we could make these an associated type of BlockValidator if
// we care enough
#[derive(Debug)]
pub enum BlockValidationError {
    TxnError,
    RandaoError,
}

pub trait BlockValidator<SCT, BPT, SBT, RBCT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
{
    fn validate(
        &self,
        block: Block<SCT>,
        author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<BPT::ValidatedBlock, BlockValidationError>;
}

impl<SCT, BPT, SBT, RBCT, T> BlockValidator<SCT, BPT, SBT, RBCT> for Box<T>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    T: BlockValidator<SCT, BPT, SBT, RBCT> + ?Sized,
{
    fn validate(
        &self,
        block: Block<SCT>,
        author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<BPT::ValidatedBlock, BlockValidationError> {
        (**self).validate(block, author_pubkey)
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
        _author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<
        <PassthruBlockPolicy as BlockPolicy<SCT, SBT, RBCT>>::ValidatedBlock,
        BlockValidationError,
    > {
        Ok(block)
    }
}
