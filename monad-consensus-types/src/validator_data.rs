use monad_proto::{
    error::ProtoError,
    proto::validator_data::{ProtoValidatorData, ValidatorMapEntry},
};
use monad_types::{
    convert::{proto_to_pubkey, pubkey_to_proto},
    NodeId, Stake,
};

use crate::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};

/// ValidatorData is used by updaters to send valdiator set updates
/// to MonadState
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorData<SCT: SignatureCollection>(
    pub  Vec<(
        NodeId<SCT::NodeIdPubKey>,
        Stake,
        SignatureCollectionPubKeyType<SCT>,
    )>,
);

impl<SCT: SignatureCollection> ValidatorData<SCT> {
    pub fn new(
        validators: Vec<(SCT::NodeIdPubKey, Stake, SignatureCollectionPubKeyType<SCT>)>,
    ) -> Self {
        Self(
            validators
                .into_iter()
                .map(|(pubkey, stake, cert_pubkey)| (NodeId::new(pubkey), stake, cert_pubkey))
                .collect(),
        )
    }

    pub fn get_stakes(&self) -> Vec<(NodeId<SCT::NodeIdPubKey>, Stake)> {
        self.0
            .iter()
            .map(|(node, stake, _)| (*node, *stake))
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
            .map(|(node, _, cert_pubkey)| (*node, *cert_pubkey))
            .collect()
    }
}

impl<SCT: SignatureCollection> From<&ValidatorData<SCT>> for ProtoValidatorData {
    fn from(value: &ValidatorData<SCT>) -> Self {
        let vlist = value
            .0
            .iter()
            .map(|(node, stake, cert_pubkey)| ValidatorMapEntry {
                node_id: Some(node.into()),
                stake: Some(stake.into()),
                cert_pubkey: Some(pubkey_to_proto(cert_pubkey)),
            })
            .collect::<Vec<ValidatorMapEntry>>();
        Self { validators: vlist }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoValidatorData> for ValidatorData<SCT> {
    type Error = ProtoError;
    fn try_from(value: ProtoValidatorData) -> std::result::Result<Self, Self::Error> {
        let mut vlist = ValidatorData(Vec::new());
        for v in value.validators {
            let a = v
                .node_id
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.node_id".to_owned(),
                ))?
                .try_into()?;
            let b = v
                .stake
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.stake".to_owned(),
                ))?
                .try_into()?;
            let c = proto_to_pubkey(v.cert_pubkey.ok_or(Self::Error::MissingRequiredField(
                "ValildatorMapEntry.cert_pubkey".to_owned(),
            ))?)?;

            vlist.0.push((a, b, c));
        }

        Ok(vlist)
    }
}
