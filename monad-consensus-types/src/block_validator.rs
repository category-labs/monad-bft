use core::fmt::Debug;

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

impl<SCT, BPT, SBT, T> BlockValidator<SCT, BPT, SBT> for Box<T>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    T: BlockValidator<SCT, BPT, SBT> + ?Sized,
{
    fn validate(
        &self,
        block: Block<SCT>,
        payload: Payload,
        author_pubkey: &SignatureCollectionPubKeyType<SCT>,
    ) -> Result<BPT::ValidatedBlock, BlockValidationError> {
        (**self).validate(block, payload, author_pubkey)
    }
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
