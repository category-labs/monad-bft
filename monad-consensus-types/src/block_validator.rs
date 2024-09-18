use core::fmt::Debug;

use auto_impl::auto_impl;
use monad_state_backend::{InMemoryState, StateBackend};

use crate::{
    block::{Block, BlockPolicy, FullBlock, PassthruBlockPolicy},
    payload::Payload,
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};

// TODO these are eth-specific types... we could make these an associated type of BlockValidator if
// we care enough
#[derive(Debug)]
pub enum BlockValidationError {
    TxnError,
    RandaoError,
    HeaderError,
    PayloadError,
    HeaderPayloadMismatchError,
}

#[auto_impl(Box)]
pub trait BlockValidator<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn validate(
        &self,
        block: Block<SCT>,
        payload: Payload,
        author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<BPT::ValidatedBlock, BlockValidationError>;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl<SCT> BlockValidator<SCT, PassthruBlockPolicy, InMemoryState> for MockValidator
where
    SCT: SignatureCollection,
{
    fn validate(
        &self,
        block: Block<SCT>,
        payload: Payload,
        _author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<
        <PassthruBlockPolicy as BlockPolicy<SCT, InMemoryState>>::ValidatedBlock,
        BlockValidationError,
    > {
        Ok(FullBlock { block, payload })
    }
}
