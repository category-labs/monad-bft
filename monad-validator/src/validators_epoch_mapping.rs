use std::collections::HashMap;

use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_types::Epoch;

use crate::validator_set::ValidatorSetType;

/// A mapping from Epoch -> (Validator Stakes, Validator Certificate Pubkeys).
pub struct ValidatorsEpochMapping<VT, SCT>
where
    VT: ValidatorSetType,
    SCT: SignatureCollection,
{
    validator_map: HashMap<
        Epoch,
        (
            VT,
            ValidatorMapping<VT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
        ),
    >,
}

impl<VT, SCT> ValidatorsEpochMapping<VT, SCT>
where
    VT: ValidatorSetType,
    SCT: SignatureCollection,
{
    pub fn get_val_set(&self, epoch: &Epoch) -> Option<&VT> {
        self.validator_map.get(epoch).map(|vs| &vs.0)
    }

    pub fn get_cert_pubkeys(
        &self,
        epoch: &Epoch,
    ) -> Option<&ValidatorMapping<VT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>> {
        self.validator_map.get(epoch).map(|vs| &vs.1)
    }

    pub fn insert(
        &mut self,
        epoch: Epoch,
        val_stakes: VT,
        val_cert_pubkeys: ValidatorMapping<VT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
    ) {
        let res = self
            .validator_map
            .insert(epoch, (val_stakes, val_cert_pubkeys));

        assert!(res.is_none());
    }
}

impl<VT, SCT> Default for ValidatorsEpochMapping<VT, SCT>
where
    VT: ValidatorSetType,
    SCT: SignatureCollection,
{
    fn default() -> Self {
        Self {
            validator_map: HashMap::new(),
        }
    }
}
