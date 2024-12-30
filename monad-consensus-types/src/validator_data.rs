use monad_crypto::certificate_signature::PubKey;
use monad_proto::{
    error::ProtoError,
    proto::validator_data::{ProtoValidatorDataEntry, ProtoValidatorSetData},
};
use monad_types::{
    convert::{proto_to_pubkey, pubkey_to_proto},
    Epoch, NodeId, Round, Stake,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorSetDataWithEpoch<SCT: SignatureCollection> {
    /// Validator set are active for this epoch
    pub epoch: Epoch,
    /// By the end of epoch - 1, the next epoch is scheduled to start on round. Otherwise, it's left empty
    pub round: Option<Round>,
    // TODO: maybe flatten
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub validators: ValidatorSetData<SCT>,
}

/// ValidatorSetData is used by updaters to send validator set updates to
/// MonadState
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorSetData<SCT: SignatureCollection>(
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub Vec<ValidatorData<SCT>>,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorData<SCT: SignatureCollection> {
    pub node_id: NodeId<SCT::NodeIdPubKey>,
    pub stake: Stake,
    #[serde(serialize_with = "serialize_cert_pubkey::<_, SCT>")]
    #[serde(deserialize_with = "deserialize_cert_pubkey::<_, SCT>")]
    pub cert_pubkey: SignatureCollectionPubKeyType<SCT>,
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

impl<SCT: SignatureCollection> From<&ValidatorData<SCT>> for ProtoValidatorDataEntry {
    fn from(value: &ValidatorData<SCT>) -> Self {
        Self {
            node_id: Some((&value.node_id).into()),
            stake: Some((&value.stake).into()),
            cert_pubkey: Some(pubkey_to_proto(&value.cert_pubkey)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoValidatorDataEntry> for ValidatorData<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoValidatorDataEntry) -> Result<Self, Self::Error> {
        let node_id = value
            .node_id
            .ok_or(Self::Error::MissingRequiredField(
                "ValidatorData::node_id".to_owned(),
            ))?
            .try_into()?;
        let stake = value
            .stake
            .ok_or(Self::Error::MissingRequiredField(
                "ValidatorData::stake".to_owned(),
            ))?
            .try_into()?;
        let cert_pubkey = proto_to_pubkey(value.cert_pubkey.ok_or(
            Self::Error::MissingRequiredField("ValidatorData.cert_pubkey".to_owned()),
        )?)?;

        Ok(ValidatorData {
            node_id,
            stake,
            cert_pubkey,
        })
    }
}

impl<SCT: SignatureCollection> From<&ValidatorSetData<SCT>> for ProtoValidatorSetData {
    fn from(value: &ValidatorSetData<SCT>) -> Self {
        let vlist = value
            .0
            .iter()
            .map(Into::into)
            .collect::<Vec<ProtoValidatorDataEntry>>();
        Self { validators: vlist }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoValidatorSetData> for ValidatorSetData<SCT> {
    type Error = ProtoError;
    fn try_from(value: ProtoValidatorSetData) -> std::result::Result<Self, Self::Error> {
        let mut vlist = ValidatorSetData(Vec::new());
        for v in value.validators {
            vlist.0.push(v.try_into()?);
        }

        Ok(vlist)
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
