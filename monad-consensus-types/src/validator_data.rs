// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, error::Error, path::Path};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{Epoch, ExecutionProtocol, LimitedVec, NodeId, Stake};
use monad_validator::{
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
    validator_set::MAX_VALIDATOR_SET_SIZE,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::checkpoint::Checkpoint;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct ValidatorSetDataWithEpoch<SCT: SignatureCollection> {
    /// Validator set are active for this epoch
    pub epoch: Epoch,

    #[serde(bound(serialize = "", deserialize = ""))]
    pub validators: ValidatorSetData<SCT>,
}

/// ValidatorSetData is used by updaters to send validator set updates to
/// MonadState
#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct ValidatorSetData<SCT: SignatureCollection>(
    #[serde(bound(serialize = "", deserialize = ""))]
    pub LimitedVec<ValidatorData<SCT>, MAX_VALIDATOR_SET_SIZE>,
);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
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

/// Top-level lists aren't supported in toml, so create this
#[derive(Serialize, Deserialize)]
pub struct ValidatorsConfigFile<SCT: SignatureCollection> {
    #[serde(bound(serialize = "", deserialize = ""))]
    pub validator_sets: Vec<ValidatorSetDataWithEpoch<SCT>>,
}

impl<SCT> From<&ValidatorsConfig<SCT>> for ValidatorsConfigFile<SCT>
where
    SCT: SignatureCollection,
{
    fn from(config: &ValidatorsConfig<SCT>) -> Self {
        Self {
            validator_sets: config
                .validators
                .iter()
                .map(|(&epoch, vset)| ValidatorSetDataWithEpoch {
                    epoch,
                    validators: vset.clone(),
                })
                .collect(),
        }
    }
}

impl<SCT: SignatureCollection> ValidatorsConfig<SCT> {
    pub fn read_from_path(validators_path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let contents = std::fs::read_to_string(validators_path)?;

        Ok(Self::read_from_str(&contents)?)
    }

    pub fn read_from_str(str: &str) -> Result<Self, toml::de::Error> {
        let validators_config: ValidatorsConfigFile<SCT> = toml::from_str(str)?;
        assert!(!validators_config.validator_sets.is_empty());
        Ok(Self {
            validators: validators_config
                .validator_sets
                .into_iter()
                .map(|validator_set| {
                    assert!(
                        validator_set
                            .validators
                            .get_stakes()
                            .iter()
                            .all(|(_, stake)| *stake > Stake::ZERO),
                        "validators should have non-zero stake"
                    );

                    (validator_set.epoch, validator_set.validators)
                })
                .collect(),
        })
    }

    pub fn get_validator_set(&self, epoch: &Epoch) -> Option<&ValidatorSetData<SCT>> {
        self.validators.get(epoch)
    }

    pub fn get_locked_validator_sets<ST, EPT>(
        &self,
        forkpoint: &Checkpoint<ST, SCT, EPT>,
    ) -> Result<Vec<ValidatorSetDataWithEpoch<SCT>>, Epoch>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
    {
        let mut validator_sets = Vec::new();
        for locked_epoch in &forkpoint.validator_sets {
            let validators = match self.get_validator_set(&locked_epoch.epoch) {
                Some(v) => v.clone(),
                None => return Err(locked_epoch.epoch),
            };
            validator_sets.push(ValidatorSetDataWithEpoch {
                epoch: locked_epoch.epoch,
                validators,
            });
        }

        Ok(validator_sets)
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
                .collect::<Vec<_>>()
                .try_into()
                .expect("validator set size exceeds MAX_VALIDATOR_SET_SIZE"),
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

    pub fn get_pubkeys(&self) -> Vec<NodeId<SCT::NodeIdPubKey>> {
        self.0
            .iter()
            .map(
                |ValidatorData {
                     node_id,
                     stake: _,
                     cert_pubkey: _,
                 }| *node_id,
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

#[cfg(test)]
mod test {
    use monad_crypto::{certificate_signature::CertificateKeyPair, NopSignature};
    use monad_testutil::signing::{create_certificate_keys, create_keys, MockSignatures};
    use monad_types::{BlockId, Epoch, Hash, NodeId, Round, Stake};
    use monad_validator::validator_set::MAX_VALIDATOR_SET_SIZE;
    use serde::Serialize;

    use crate::{
        block::MockExecutionProtocol,
        checkpoint::{Checkpoint, LockedEpoch},
        quorum_certificate::QuorumCertificate,
        validator_data::{ValidatorData, ValidatorSetData, ValidatorsConfig},
        RoundCertificate,
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;

    #[test]
    fn test_get_locked_validator_sets() {
        let mut validators = std::collections::BTreeMap::new();
        validators.insert(
            Epoch(1),
            ValidatorSetData::<SignatureCollectionType>(Default::default()),
        );
        validators.insert(
            Epoch(2),
            ValidatorSetData::<SignatureCollectionType>(Default::default()),
        );
        let validators_config = ValidatorsConfig { validators };

        let forkpoint_success = Checkpoint {
            root: BlockId(Hash::default()),
            high_certificate: RoundCertificate::Qc(QuorumCertificate::genesis_qc()),
            validator_sets: vec![
                LockedEpoch {
                    epoch: Epoch(1),
                    round: Round(10),
                },
                LockedEpoch {
                    epoch: Epoch(2),
                    round: Round(20),
                },
            ]
            .try_into()
            .unwrap(),
        };
        assert!(validators_config
            .get_locked_validator_sets::<SignatureType, ExecutionProtocolType>(&forkpoint_success)
            .is_ok());

        let forkpoint_failure = Checkpoint {
            root: BlockId(Hash::default()),
            high_certificate: RoundCertificate::Qc(QuorumCertificate::genesis_qc()),
            validator_sets: vec![
                LockedEpoch {
                    epoch: Epoch(2),
                    round: Round(10),
                },
                LockedEpoch {
                    epoch: Epoch(3),
                    round: Round(20),
                },
            ]
            .try_into()
            .unwrap(),
        };
        assert!(validators_config
            .get_locked_validator_sets::<SignatureType, ExecutionProtocolType>(&forkpoint_failure)
            .is_err_and(|missing_epoch| missing_epoch == Epoch(3)));
    }

    fn make_validator_data(num: u32) -> Vec<ValidatorData<SignatureCollectionType>> {
        let keys = create_keys::<SignatureType>(num);
        let cert_keys = create_certificate_keys::<SignatureCollectionType>(num);
        keys.iter()
            .zip(cert_keys.iter())
            .map(|(k, ck)| ValidatorData {
                node_id: NodeId::new(k.pubkey()),
                stake: Stake::ONE,
                cert_pubkey: ck.pubkey(),
            })
            .collect()
    }

    /// Mirror of `ValidatorsConfigFile` whose innermost validator list is an
    /// unbounded `Vec` instead of `LimitedVec<_, MAX_VALIDATOR_SET_SIZE>`.
    /// Serializes to the same TOML wire shape, so the produced string can be
    /// fed to `ValidatorsConfig::read_from_str` to drive the rejection path
    /// for oversize input. Test-only.
    #[derive(Serialize)]
    struct OversizeConfigFile {
        validator_sets: Vec<OversizeSetWithEpoch>,
    }

    #[derive(Serialize)]
    struct OversizeSetWithEpoch {
        epoch: Epoch,
        validators: Vec<ValidatorData<SignatureCollectionType>>,
    }

    #[test]
    fn read_from_str_rejects_oversize_validator_set() {
        let oversize = (MAX_VALIDATOR_SET_SIZE + 1) as u32;
        let validators = make_validator_data(oversize);
        let config_file = OversizeConfigFile {
            validator_sets: vec![OversizeSetWithEpoch {
                epoch: Epoch(1),
                validators,
            }],
        };
        let toml_str = toml::to_string(&config_file).expect("serialize oversize config");

        let err = match ValidatorsConfig::<SignatureCollectionType>::read_from_str(&toml_str) {
            Ok(_) => panic!("read_from_str must reject oversize validator set"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("LimitedVec length") && msg.contains("exceeds maximum"),
            "unexpected error message: {msg}",
        );
    }

    #[test]
    fn read_from_str_accepts_max_validator_set() {
        let exact = MAX_VALIDATOR_SET_SIZE as u32;
        let validators = make_validator_data(exact);
        let config_file = OversizeConfigFile {
            validator_sets: vec![OversizeSetWithEpoch {
                epoch: Epoch(1),
                validators,
            }],
        };
        let toml_str = toml::to_string(&config_file).expect("serialize exact-size config");

        ValidatorsConfig::<SignatureCollectionType>::read_from_str(&toml_str)
            .expect("read_from_str must accept exact-size validator set");
    }

    #[test]
    #[should_panic(expected = "validator set size exceeds MAX_VALIDATOR_SET_SIZE")]
    fn new_panics_on_oversize_input() {
        let oversize = (MAX_VALIDATOR_SET_SIZE + 1) as u32;
        let keys = create_keys::<SignatureType>(oversize);
        let cert_keys = create_certificate_keys::<SignatureCollectionType>(oversize);
        let triples: Vec<_> = keys
            .iter()
            .zip(cert_keys.iter())
            .map(|(k, ck)| (k.pubkey(), Stake::ONE, ck.pubkey()))
            .collect();
        let _ = ValidatorSetData::<SignatureCollectionType>::new(triples);
    }

    #[test]
    fn new_accepts_max_input() {
        let exact = MAX_VALIDATOR_SET_SIZE as u32;
        let keys = create_keys::<SignatureType>(exact);
        let cert_keys = create_certificate_keys::<SignatureCollectionType>(exact);
        let triples: Vec<_> = keys
            .iter()
            .zip(cert_keys.iter())
            .map(|(k, ck)| (k.pubkey(), Stake::ONE, ck.pubkey()))
            .collect();
        let set = ValidatorSetData::<SignatureCollectionType>::new(triples);
        assert_eq!(set.0.len(), MAX_VALIDATOR_SET_SIZE);
    }
}
