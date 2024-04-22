use monad_proto::{
    error::ProtoError,
    proto::validator_data::{ProtoValidatorDataEntry, ProtoValidatorSetData},
};
use monad_types::{
    convert::{proto_to_pubkey, pubkey_to_proto},
    NodeId, Stake,
};

use crate::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};

/// ValidatorSetData is used by updaters to send valdiator set updates
/// to MonadState
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorSetData<SCT: SignatureCollection>(pub Vec<ValidatorData<SCT>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorData<SCT: SignatureCollection> {
    pub node_id: NodeId<SCT::NodeIdPubKey>,
    pub stake: Stake,
    pub cert_pubkey: SignatureCollectionPubKeyType<SCT>,
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
