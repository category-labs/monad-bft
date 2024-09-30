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
    // TODO it would be less jank if the BLS pubkey was included in the block payload.
    //
    // It's weird that we need to pass in the expected author's BLS pubkey just to validate the
    // randao payload.
    //
    // If the BLS pubkey was included as part of the block, then this validate function could just
    // assert that randao_reveal is internally consistent.
    //
    // Then, separately, the BLS pubkey could be validated alongside the SECP pubkey when leader
    // checks are done.
    fn validate(
        &self,
        block: Block<SCT>,
        payload: Payload,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
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
        _author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
    ) -> Result<
        <PassthruBlockPolicy as BlockPolicy<SCT, InMemoryState>>::ValidatedBlock,
        BlockValidationError,
    > {
        Ok(FullBlock { block, payload })
    }
}
