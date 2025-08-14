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
    ) -> &(
        VTF::ValidatorSetType,
        ValidatorMapping<
            <VTF::ValidatorSetType as ValidatorSetType>::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) {
        // On consensus restart, the same validator set might be inserted a
        // second time when we commit the same boundary block again. Assert that
        // value is the same if entry exists
        let value = (
            self.validator_set_factory
                .create(val_stakes)
                .expect("ValidatorSetData should not have duplicates or invalid entries"),
            val_cert_pubkeys,
        );
        match self.validator_map.entry(epoch) {
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(value),
            std::collections::hash_map::Entry::Occupied(entry) => {
                assert_eq!(
                    entry.get().0.get_members(),
                    value.0.get_members(),
                    "Validator set mismatch"
                );
                assert_eq!(entry.get().1.map, value.1.map, "Validator mapping mismatch");

                entry.into_mut()
            }
        }
    }
}
