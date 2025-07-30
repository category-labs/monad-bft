use std::sync::Mutex;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    signing_domain,
};
use monad_types::*;
use serde::{Deserialize, Serialize};

use crate::{
    block::ConsensusBlockHeader,
    signature_collection::{
        deserialize_signature_collection, serialize_signature_collection, SignatureCollection,
    },
    voting::*,
};

pub static HALT_TIP: Mutex<BlockId> = Mutex::new(GENESIS_BLOCK_ID);

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[serde(deny_unknown_fields)]
#[serde(bound(
    serialize = "SCT: SignatureCollection",
    deserialize = "SCT: SignatureCollection",
))]
pub struct QuorumCertificate<SCT> {
    pub info: Vote,
    #[serde(serialize_with = "serialize_signature_collection::<_, SCT>")]
    #[serde(deserialize_with = "deserialize_signature_collection::<_, SCT>")]
    pub signatures: SCT,
}

impl<T: std::fmt::Debug> std::fmt::Debug for QuorumCertificate<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QC")
            .field("info", &self.info)
            .field("sigs", &self.signatures)
            .finish_non_exhaustive()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Rank(pub Vote);

impl PartialEq for Rank {
    fn eq(&self, other: &Self) -> bool {
        self.0.round == other.0.round
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
        self.0.round.cmp(&other.0.round)
    }
}

impl<SCT: SignatureCollection> QuorumCertificate<SCT> {
    pub fn new(info: Vote, signatures: SCT) -> Self {
        Self { info, signatures }
    }

    // This will be the initial high qc for all nodes
    pub fn genesis_qc() -> Self {
        let vote = Vote {
            id: GENESIS_BLOCK_ID,
            epoch: Epoch(1),
            round: GENESIS_ROUND,
            v0_parent_id: None,
            v0_parent_round: None,
        };

        let sigs = SCT::new::<signing_domain::Vote>(
            Vec::new(),
            &ValidatorMapping::new(std::iter::empty()),
            &[],
        )
        .expect("genesis qc sigs");

        QuorumCertificate {
            info: vote,
            signatures: sigs,
        }
    }

    /// returns a committable block_id if the commit rule passes
    ///
    /// qc_parent_block MUST be the the block that self points to
    pub fn get_committable_id<ST, EPT>(
        &self,
        qc_parent_block: &ConsensusBlockHeader<ST, SCT, EPT>,
    ) -> Option<BlockId>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
    {
        assert_eq!(self.info.id, qc_parent_block.get_id());
        if self.info.round == qc_parent_block.qc.info.round + Round(1) {
            Some(qc_parent_block.get_parent_id())
        } else {
            None
        }
    }

    pub fn get_round(&self) -> Round {
        self.info.round
    }

    pub fn get_epoch(&self) -> Epoch {
        self.info.epoch
    }

    pub fn get_block_id(&self) -> BlockId {
        self.info.id
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampAdjustmentDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Copy)]
pub struct TimestampAdjustment {
    pub delta: u128,
    pub direction: TimestampAdjustmentDirection,
}
