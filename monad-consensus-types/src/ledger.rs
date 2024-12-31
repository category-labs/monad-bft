use alloy_rlp::{encode_list, Decodable, Encodable, Header};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hashable, Hasher},
};
use monad_types::BlockId;
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

use crate::{
    block::{ConsensusFullBlock, ExecutionProtocol},
    signature_collection::SignatureCollection,
};

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, AsBytes, Serialize, Deserialize)]
pub enum CommitResult {
    NoCommit,
    Commit,
}

impl AsRef<[u8]> for CommitResult {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl CommitResult {
    pub fn is_commitable(&self) -> bool {
        match self {
            CommitResult::Commit => true,
            CommitResult::NoCommit => false,
        }
    }
}

impl Hashable for CommitResult {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

impl Encodable for CommitResult {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::NoCommit => {
                let enc: [&dyn Encodable; 1] = [&1u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Commit => {
                let enc: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for CommitResult {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::NoCommit),
            2 => Ok(Self::Commit),
            _ => Err(alloy_rlp::Error::Custom("unknown CommitResult variant")),
        }
    }
}

#[derive(Debug)]
pub enum OptimisticCommit<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Proposed(ConsensusFullBlock<ST, SCT, EPT>),
    Finalized(ConsensusFullBlock<ST, SCT, EPT>),
    Verified(BlockId),
}
