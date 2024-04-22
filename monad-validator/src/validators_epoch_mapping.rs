use std::collections::HashMap;

use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_types::{Epoch, NodeId, Stake};

use crate::validator_set::{ValidatorSetType, ValidatorSetTypeFactory};

/// A mapping from Epoch -> (Validator Stakes, Validator Certificate Pubkeys).
pub struct ValidatorsEpochMapping<VTF, SCT>
where
    VTF: ValidatorSetTypeFactory,
    SCT: SignatureCollection,
{
    validator_set_factory: VTF,
    validator_map: HashMap<
        Epoch,
        (
            VTF::ValidatorSetType,
            ValidatorMapping<
                <VTF::ValidatorSetType as ValidatorSetType>::NodeIdPubKey,
                SignatureCollectionKeyPairType<SCT>,
            >,
        ),
    >,
}

impl<VTF, SCT> ValidatorsEpochMapping<VTF, SCT>
where
    VTF: ValidatorSetTypeFactory,
    SCT: SignatureCollection,
{
    pub fn new(validator_set_factory: VTF) -> Self {
        Self {
            validator_set_factory,
            validator_map: HashMap::new(),
        }
    }

    pub fn get_val_set(&self, epoch: &Epoch) -> Option<&VTF::ValidatorSetType> {
        self.validator_map.get(epoch).map(|(val_set, _)| val_set)
    }

    pub fn get_cert_pubkeys(
        &self,
        epoch: &Epoch,
    ) -> Option<
        &ValidatorMapping<
            <VTF::ValidatorSetType as ValidatorSetType>::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    > {
        self.validator_map
            .get(epoch)
            .map(|(_, cert_pubkeys)| cert_pubkeys)
    }

    pub fn insert(
        &mut self,
        epoch: Epoch,
        val_stakes: Vec<(
            NodeId<<VTF::ValidatorSetType as ValidatorSetType>::NodeIdPubKey>,
            Stake,
        )>,
        val_cert_pubkeys: ValidatorMapping<
            <VTF::ValidatorSetType as ValidatorSetType>::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) {
        let res = self.validator_map.insert(
            epoch,
            (
                self.validator_set_factory
                    .create(val_stakes)
                    .expect("ValidatorSetData should not have duplicates or invalid entries"),
                val_cert_pubkeys,
            ),
        );

        assert!(res.is_none());
    }
}
