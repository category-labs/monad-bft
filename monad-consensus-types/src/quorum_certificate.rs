use std::collections::HashSet;

use monad_crypto::hasher::{Hash, Hasher, HasherType};
use monad_types::*;

use crate::{
    ledger::*,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::*,
};

pub const GENESIS_PRIME_QC_HASH: Hash = Hash([0xAA; 32]);

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct QuorumCertificate<T> {
    pub info: QcInfo,
    pub signatures: T,
    signature_hash: Hash,
}

impl<T: std::fmt::Debug> std::fmt::Debug for QuorumCertificate<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QC")
            .field("info", &self.info)
            .field("sigs", &self.signatures)
            .field("signature_hash", &self.signature_hash)
            .finish_non_exhaustive()
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct QcInfo {
    pub vote: VoteInfo,
    pub ledger_commit: LedgerCommitInfo,
}

impl std::fmt::Debug for QcInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QcInfo")
            .field("v", &self.vote)
            .field("lc", &self.ledger_commit)
            .finish()
    }
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
        seq_num: SeqNum(0),
    }
}

impl<SCT: SignatureCollection> QuorumCertificate<SCT> {
    pub fn new(info: QcInfo, signatures: SCT) -> Self {
        let hash = signatures.get_hash();
        QuorumCertificate {
            info,
            signatures,
            signature_hash: hash,
        }
    }

    // This is the QC that will be included in the genesis block
    pub fn genesis_prime_qc() -> Self {
        let vote_info = VoteInfo {
            id: BlockId(GENESIS_PRIME_QC_HASH),
            round: Round(0),
            parent_id: BlockId(GENESIS_PRIME_QC_HASH),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };
        let lci = LedgerCommitInfo::new(None, &vote_info);

        let sigs = SCT::new(Vec::new(), &ValidatorMapping::new(std::iter::empty()), &[])
            .expect("genesis qc sigs");
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
    pub fn genesis_qc(genesis_vote_info: VoteInfo, genesis_signatures: SCT) -> Self {
        let vote_info = genesis_vote_info;
        let lci = LedgerCommitInfo::new(None, &vote_info);

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

    pub fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> HashSet<NodeId> {
        // TODO-3, consider caching this qc_msg hash in qc for performance in future
        let qc_msg = HasherType::hash_object(&self.info.ledger_commit);
        self.signatures
            .get_participants(validator_mapping, qc_msg.as_ref())
    }
}
