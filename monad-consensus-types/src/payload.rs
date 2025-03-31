use std::{ops::Deref, sync::Arc};

use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};
use bytes::BytesMut;
use monad_compress::CompressionAlgo;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hasher, HasherType},
};
use monad_types::{ExecutionProtocol, Round};
use serde::{Deserialize, Serialize};

/// randao_reveal uses a proposer's public key to contribute randomness
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct RoundSignature<CST: CertificateSignature>(pub CST);

impl<CST: CertificateSignature> RoundSignature<CST> {
    /// TODO should this incorporate parent_block_id to increase "randomness"?
    pub fn new(round: Round, keypair: &CST::KeyPairType) -> Self {
        let encoded_round = alloy_rlp::encode(round);
        Self(CST::sign(&encoded_round, keypair))
    }

    pub fn verify(
        &self,
        round: Round,
        pubkey: &CertificateSignaturePubKey<CST>,
    ) -> Result<(), CST::Error> {
        let encoded_round = alloy_rlp::encode(round);
        self.0.verify(&encoded_round, pubkey)
    }

    pub fn get_hash(&self) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        hasher.hash()
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct ConsensusBlockBody<EPT>(Arc<ConsensusBlockBodyInner<EPT>>)
where
    EPT: ExecutionProtocol;
impl<EPT> ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn new(body: ConsensusBlockBodyInner<EPT>) -> Self {
        Self(body.into())
    }

    pub fn get_id(&self) -> ConsensusBlockBodyId {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        ConsensusBlockBodyId(hasher.hash())
    }

    pub fn compress<CA: CompressionAlgo>(
        self,
        compression_algo: &CA,
    ) -> CompressedConsensusBlockBody {
        let mut rlp_encoded_self = Vec::new();
        self.encode(&mut rlp_encoded_self);

        let mut compressed = Vec::new();
        // FIXME: don't panic
        compression_algo
            .compress(&rlp_encoded_self, &mut compressed)
            .expect("compression error");

        CompressedConsensusBlockBody(compressed)
    }
}

impl<EPT> Deref for ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    type Target = ConsensusBlockBodyInner<EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ConsensusBlockBodyInner<EPT>
where
    EPT: ExecutionProtocol,
{
    pub execution_body: EPT::Body,
}

#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct ConsensusBlockBodyId(pub Hash);

impl std::fmt::Debug for ConsensusBlockBodyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct CompressedConsensusBlockBody(pub Vec<u8>);

impl CompressedConsensusBlockBody {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn decompress<CA: CompressionAlgo, EPT: ExecutionProtocol>(
        self,
        compression_algo: &CA,
    ) -> ConsensusBlockBody<EPT> {
        let mut rlp_encoded_body = Vec::new();
        // FIXME: don't panic
        compression_algo
            .decompress(self.0.as_slice(), &mut rlp_encoded_body)
            .expect("decompression error");

        // FIXME: don't panic
        ConsensusBlockBody::<EPT>::decode(&mut rlp_encoded_body.as_slice())
            .expect("rlp decode error")
    }
}
