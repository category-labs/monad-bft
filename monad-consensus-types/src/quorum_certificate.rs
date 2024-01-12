use std::collections::HashSet;

use monad_crypto::hasher::{Hash, Hasher, HasherType};
use monad_types::*;

use crate::{
    ledger::*,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::*,
};

pub const GENESIS_QC_HASH: Hash = Hash([0xAA; 32]);

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct QuorumCertificate<SCT> {
    pub info: QcInfo,
    pub signatures: SCT,
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
    pub vote: Vote,
}

impl QcInfo {
    pub fn get_round(&self) -> Round {
        self.vote.vote_info.round
    }
}

impl std::fmt::Debug for QcInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QcInfo").field("v", &self.vote).finish()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Rank(pub QcInfo);

impl PartialEq for Rank {
    fn eq(&self, other: &Self) -> bool {
        self.0.get_round() == other.0.get_round()
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
        self.0.get_round().0.cmp(&other.0.get_round().0)
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

    // This will be the initial high qc for all nodes
    pub fn genesis_qc() -> Self {
        let vote_info = VoteInfo {
            id: BlockId(GENESIS_QC_HASH),
            round: Round(0),
            parent_id: BlockId(GENESIS_QC_HASH),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let sigs = SCT::new(Vec::new(), &ValidatorMapping::new(std::iter::empty()), &[])
            .expect("genesis qc sigs");
        let sig_hash = sigs.get_hash();

        QuorumCertificate {
            info: QcInfo {
                vote: Vote {
                    vote_info,
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            signatures: sigs,
            signature_hash: sig_hash,
        }
    }

    pub fn get_hash(&self) -> Hash {
        self.signature_hash
    }

    pub fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> HashSet<NodeId<SCT::NodeIdPubKey>> {
        // TODO-3, consider caching this qc_msg hash in qc for performance in future
        let qc_msg = HasherType::hash_object(&self.info.vote);
        self.signatures
            .get_participants(validator_mapping, qc_msg.as_ref())
    }

    pub fn get_round(&self) -> Round {
        self.info.get_round()
    }

    pub fn get_block_id(&self) -> BlockId {
        self.info.vote.vote_info.id
    }

    pub fn get_seq_num(&self) -> SeqNum {
        self.info.vote.vote_info.seq_num
    }
}
