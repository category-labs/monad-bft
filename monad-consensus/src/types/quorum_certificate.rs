use zerocopy::AsBytes;

use monad_types::*;

use crate::types::ledger::*;
use crate::types::signature::{ConsensusSignature, SignatureCollection};
use crate::types::voting::*;
use crate::validation::hashing::{Hashable, Hasher};
use crate::validation::signing::{Signable, Signed, Unverified};

#[non_exhaustive]
#[derive(Clone, Default, Debug)]
pub struct QuorumCertificate<T>
where
    T: SignatureCollection,
{
    pub info: QcInfo,
    pub signatures: T,
    pub signature_hash: Hash,
}

impl<T: SignatureCollection> Hashable for &QuorumCertificate<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.signature_hash.as_bytes());
    }
}

impl<T> Signable for QuorumCertificate<T>
where
    T: SignatureCollection,
{
    type Output = Unverified<QuorumCertificate<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct QcInfo {
    pub vote: VoteInfo,
    pub ledger_commit: LedgerCommitInfo,
}

#[derive(Copy, Clone, Debug)]
pub struct Rank(pub QcInfo);

impl PartialEq for Rank {
    fn eq(&self, other: &Self) -> bool {
        self.0.vote.round == other.0.vote.round
    }
}

impl Eq for Rank {}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Rank {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.vote.round.0.cmp(&other.0.vote.round.0)
    }
}

impl<T: SignatureCollection> QuorumCertificate<T> {
    pub fn new(info: QcInfo, signatures: T) -> Self {
        let hash = signatures.get_hash();
        QuorumCertificate {
            info,
            signatures,
            signature_hash: hash,
        }
    }
}
