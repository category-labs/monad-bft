use monad_types::*;

use crate::types::ledger::*;
use crate::types::signature::SignatureCollection;
use crate::types::voting::*;
use crate::validation::hashing::Hasher;

pub const GENESIS_PRIME_QC_HASH: Hash = Hash([0xAA; 32]);

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuorumCertificate<T> {
    pub info: QcInfo,
    pub signatures: T,
    signature_hash: Hash,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

pub fn genesis_vote_info(genesis_block_id: BlockId) -> VoteInfo {
    VoteInfo {
        id: genesis_block_id,
        round: Round(0),
        parent_id: BlockId(GENESIS_PRIME_QC_HASH),
        parent_round: Round(0),
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

    // This is the QC that will be included in the genesis block
    pub fn genesis_prime_qc<H: Hasher>() -> Self {
        let vote_info = VoteInfo {
            id: BlockId(GENESIS_PRIME_QC_HASH),
            round: Round(0),
            parent_id: BlockId(GENESIS_PRIME_QC_HASH),
            parent_round: Round(0),
        };
        let lci = LedgerCommitInfo::new::<H>(None, &vote_info);

        let sigs = T::new();
        let sig_hash = sigs.get_hash();

        QuorumCertificate {
            info: QcInfo {
                vote: vote_info,
                ledger_commit: lci,
            },
            signatures: sigs,
            signature_hash: sig_hash,
        }
    }

    // This is the QC that will be used in the block of the first proposal
    // and will be the initial qc_high for all nodes
    // All initial genesis nodes will have to create signatures for the genesis lci
    pub fn genesis_qc<H: Hasher>(genesis_vote_info: VoteInfo, genesis_signatures: T) -> Self {
        let vote_info = genesis_vote_info;
        let lci = LedgerCommitInfo::new::<H>(None, &vote_info);

        let sig_hash = genesis_signatures.get_hash();

        QuorumCertificate {
            info: QcInfo {
                vote: vote_info,
                ledger_commit: lci,
            },
            signatures: genesis_signatures,
            signature_hash: sig_hash,
        }
    }

    pub fn get_hash(&self) -> Hash {
        self.signature_hash
    }
}
