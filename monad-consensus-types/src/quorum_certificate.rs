use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{signature_collection::SignatureCollection, voting::*};

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
        self.0.round.cmp(&other.0.round)
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
        Self { info, signatures }
    }

    // This will be the initial high qc for all nodes
    pub fn genesis_qc() -> Self {
        let vote = Vote {
            id: GENESIS_BLOCK_ID,
            epoch: Epoch(1),
            round: GENESIS_ROUND,
            parent_id: GENESIS_BLOCK_ID,
            parent_round: GENESIS_ROUND,
        };

        let sigs = SCT::new(Vec::new(), &ValidatorMapping::new(std::iter::empty()), &[])
            .expect("genesis qc sigs");

        QuorumCertificate {
            info: vote,
            signatures: sigs,
        }
    }

    /// Returns a committable block_id, if exists
    pub fn get_committable_id(&self) -> Option<BlockId> {
        if self.info.round == self.info.parent_round + Round(1) {
            Some(self.info.parent_id)
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
