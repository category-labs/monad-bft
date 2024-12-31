use std::collections::HashSet;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hasher, HasherType},
};
use monad_types::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    block::{ConsensusBlockHeader, ExecutionProtocol},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::*,
};

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[serde(deny_unknown_fields)]
pub struct QuorumCertificate<SCT> {
    pub info: Vote,

    #[serde(serialize_with = "serialize_signature_collection::<_, SCT>")]
    #[serde(deserialize_with = "deserialize_signature_collection::<_, SCT>")]
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
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
        self.0.round.0.cmp(&other.0.round.0)
    }
}

fn serialize_signature_collection<S, SCT>(
    signature_collection: &SCT,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    SCT: SignatureCollection,
    S: Serializer,
{
    let hex_str = "0x".to_string() + &hex::encode(signature_collection.serialize());
    serializer.serialize_str(&hex_str)
}

fn deserialize_signature_collection<'de, D, SCT>(deserializer: D) -> Result<SCT, D::Error>
where
    SCT: SignatureCollection,
    D: Deserializer<'de>,
{
    let buf = <std::string::String as Deserialize>::deserialize(deserializer)?;

    let Some(hex_str) = buf.strip_prefix("0x") else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

    SCT::deserialize(bytes.as_ref()).map_err(<D::Error as serde::de::Error>::custom)
}

impl<SCT: SignatureCollection> QuorumCertificate<SCT> {
    pub fn new(info: Vote, signatures: SCT) -> Self {
        QuorumCertificate { info, signatures }
    }

    // This will be the initial high qc for all nodes
    pub fn genesis_qc() -> Self {
        let vote_info = Vote {
            id: GENESIS_BLOCK_ID,
            epoch: Epoch(1),
            round: Round(0),
        };

        let sigs = SCT::new(Vec::new(), &ValidatorMapping::new(std::iter::empty()), &[])
            .expect("genesis qc sigs");

        QuorumCertificate {
            info: vote_info,
            signatures: sigs,
        }
    }

    /// returns a committable block_id if the commit rule passes
    ///
    /// qc_parent_block MUST be the the block that self points to
    pub fn check_committable<ST, EPT>(
        &self,
        qc_parent_block: &ConsensusBlockHeader<ST, SCT, EPT>,
    ) -> Option<BlockId>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
    {
        assert_eq!(self.info.id, qc_parent_block.get_id());
        if self.info.round == qc_parent_block.get_parent_round() + Round(1) {
            return Some(qc_parent_block.get_parent_id());
        }
        None
    }

    pub fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> HashSet<NodeId<SCT::NodeIdPubKey>> {
        // TODO-3, consider caching this qc_msg hash in qc for performance in future
        let qc_msg = HasherType::hash_object(&self.info);
        self.signatures
            .get_participants(validator_mapping, qc_msg.as_ref())
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
