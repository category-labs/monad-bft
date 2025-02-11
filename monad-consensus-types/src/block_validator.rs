use core::fmt::Debug;

use auto_impl::auto_impl;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::{InMemoryState, StateBackend};
use monad_types::ExecutionProtocol;

use crate::{
    block::{
        BlockPolicy, ConsensusBlockHeader, ConsensusFullBlock, PassthruBlockPolicy,
        PassthruWrappedBlock,
    },
    payload::ConsensusBlockBody,
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
    TimestampError,
}

#[auto_impl(Box)]
pub trait BlockValidator<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
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
        header: ConsensusBlockHeader<ST, SCT, EPT>,
        body: ConsensusBlockBody<EPT>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
    ) -> Result<BPT::ValidatedBlock, BlockValidationError>;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl<ST, SCT, EPT> BlockValidator<ST, SCT, EPT, PassthruBlockPolicy, InMemoryState>
    for MockValidator
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn validate(
        &self,
        header: ConsensusBlockHeader<ST, SCT, EPT>,
        body: ConsensusBlockBody<EPT>,
        _author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
        _tx_limit: usize,
        _proposal_gas_limit: u64,
        _proposal_byte_limit: u64,
    ) -> Result<
        <PassthruBlockPolicy as BlockPolicy<ST, SCT, EPT, InMemoryState>>::ValidatedBlock,
        BlockValidationError,
    > {
        let full_block = ConsensusFullBlock::new(header, body)?;
        Ok(PassthruWrappedBlock(full_block))
    }
}
