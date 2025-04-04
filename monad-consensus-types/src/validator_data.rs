use std::{collections::BTreeMap, error::Error, ops::Bound, path::Path};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Stake};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    checkpoint::Checkpoint,
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ValidatorSetDataWithEpoch<SCT: SignatureCollection> {
    /// Validator set are active for this epoch
    pub epoch: Epoch,

    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub validators: ValidatorSetData<SCT>,
}

/// ValidatorSetData is used by updaters to send validator set updates to
/// MonadState
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct ValidatorSetData<SCT: SignatureCollection>(
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub Vec<ValidatorData<SCT>>,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct ValidatorData<SCT: SignatureCollection> {
    pub node_id: NodeId<SCT::NodeIdPubKey>,
    pub stake: Stake,
    #[serde(serialize_with = "serialize_cert_pubkey::<_, SCT>")]
    #[serde(deserialize_with = "deserialize_cert_pubkey::<_, SCT>")]
    pub cert_pubkey: SignatureCollectionPubKeyType<SCT>,
}

pub struct ValidatorsConfig<SCT: SignatureCollection> {
    pub validators: BTreeMap<Epoch, ValidatorSetData<SCT>>,
}

impl<SCT: SignatureCollection> ValidatorsConfig<SCT> {
    pub fn read_from_path(validators_path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        /// Top-level lists aren't supported in toml, so create this
        #[derive(Deserialize)]
        struct ValidatorsConfigFile<SCT: SignatureCollection> {
            #[serde(bound(
                serialize = "SCT: SignatureCollection",
                deserialize = "SCT: SignatureCollection",
            ))]
            validator_sets: Vec<ValidatorSetDataWithEpoch<SCT>>,
        }

        let validators_config: ValidatorsConfigFile<SCT> =
            toml::from_str(&std::fs::read_to_string(validators_path)?)?;
        assert!(!validators_config.validator_sets.is_empty());
        Ok(Self {
            validators: validators_config
                .validator_sets
                .into_iter()
                .map(|validator| (validator.epoch, validator.validators))
                .collect(),
        })
    }

    pub fn get_validator_set(&self, epoch: &Epoch) -> &ValidatorSetData<SCT> {
        self.validators
            .range((Bound::Included(Epoch(0)), Bound::Included(*epoch)))
            .last()
            .expect("no validator set <= block.epoch")
            .1
    }

    pub fn get_locked_validator_sets(
        &self,
        forkpoint: &Checkpoint<SCT>,
    ) -> Vec<ValidatorSetDataWithEpoch<SCT>> {
        forkpoint
            .validator_sets
            .iter()
            .map(|locked_epoch| ValidatorSetDataWithEpoch {
                epoch: locked_epoch.epoch,
                validators: self.get_validator_set(&locked_epoch.epoch).clone(),
            })
            .collect()
    }
}

fn serialize_cert_pubkey<S, SCT>(
    cert_pubkey: &SignatureCollectionPubKeyType<SCT>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    SCT: SignatureCollection,
    S: Serializer,
{
    let hex_str = "0x".to_string() + &hex::encode(cert_pubkey.bytes());
    serializer.serialize_str(&hex_str)
}

fn deserialize_cert_pubkey<'de, D, SCT>(
    deserializer: D,
) -> Result<SignatureCollectionPubKeyType<SCT>, D::Error>
where
    SCT: SignatureCollection,
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;

    let Some(hex_str) = buf.strip_prefix("0x") else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

    SignatureCollectionPubKeyType::<SCT>::from_bytes(&bytes)
        .map_err(<D::Error as serde::de::Error>::custom)
}

impl<SCT: SignatureCollection> ValidatorSetData<SCT> {
    pub fn new(
        validators: Vec<(SCT::NodeIdPubKey, Stake, SignatureCollectionPubKeyType<SCT>)>,
    ) -> Self {
        Self(
            validators
                .into_iter()
                .map(|(pubkey, stake, cert_pubkey)| ValidatorData {
                    node_id: NodeId::new(pubkey),
                    stake,
                    cert_pubkey,
                })
                .collect(),
        )
    }

    pub fn get_stakes(&self) -> Vec<(NodeId<SCT::NodeIdPubKey>, Stake)> {
        self.0
            .iter()
            .map(
                |ValidatorData {
                     node_id,
                     stake,
                     cert_pubkey: _,
                 }| (*node_id, *stake),
            )
            .collect()
    }

    pub fn get_cert_pubkeys(
        &self,
    ) -> Vec<(
        NodeId<SCT::NodeIdPubKey>,
        SignatureCollectionPubKeyType<SCT>,
    )> {
        self.0
            .iter()
            .map(
                |ValidatorData {
                     node_id,
                     stake: _,
                     cert_pubkey,
                 }| (*node_id, *cert_pubkey),
            )
            .collect()
    }
}

pub fn serialize_nodeid<S, SCT>(
    node_id: &NodeId<SCT::NodeIdPubKey>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    SCT: SignatureCollection,
{
    let hex_str = "0x".to_string() + &hex::encode(node_id.pubkey().bytes());
    serializer.serialize_str(&hex_str)
}

pub fn deserialize_nodeid<'de, D, SCT>(
    deserializer: D,
) -> Result<NodeId<SCT::NodeIdPubKey>, D::Error>
where
    D: Deserializer<'de>,
    SCT: SignatureCollection,
{
    let buf = <String as Deserialize>::deserialize(deserializer)?;

    let Some(hex_str) = buf.strip_prefix("0x") else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

    Ok(NodeId::new(
        <SCT as SignatureCollection>::NodeIdPubKey::from_bytes(&bytes)
            .map_err(<D::Error as serde::de::Error>::custom)?,
    ))
}
