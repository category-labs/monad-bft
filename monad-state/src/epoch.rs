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

use std::marker::PhantomData;

use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    block::BlockPolicy, block_validator::BlockValidator, validator_data::ValidatorSetDataWithEpoch,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, ConfigFileCommand, MonadEvent, RouterCommand, ValidatorEvent};
use monad_state_backend::StateBackend;
use monad_types::ExecutionProtocol;
use monad_validator::{
    signature_collection::SignatureCollection, validator_mapping::ValidatorMapping,
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct EpochChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    val_epoch_map: &'a mut ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT)>,
}

pub(super) enum EpochCommand<SCT>
where
    SCT: SignatureCollection,
{
    AddEpochValidatorSet(ValidatorSetDataWithEpoch<SCT>),
}

impl<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    EpochChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>,
    ) -> Self {
        Self {
            val_epoch_map: &mut monad_state.val_epoch_map,
            _phantom: PhantomData,
        }
    }
    pub(super) fn update(&mut self, event: ValidatorEvent<SCT>) -> Vec<EpochCommand<SCT>> {
        match event {
            ValidatorEvent::UpdateValidators(vset) => {
                self.val_epoch_map.insert(
                    vset.epoch,
                    vset.validators.get_stakes(),
                    ValidatorMapping::new(vset.validators.get_cert_pubkeys()),
                );
                vec![EpochCommand::AddEpochValidatorSet(vset)]
            }
        }
    }
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> From<EpochCommand<SCT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
            CCT,
            CRT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn from(command: EpochCommand<SCT>) -> Self {
        match command {
            EpochCommand::AddEpochValidatorSet(validator_set_data) => {
                vec![
                    Command::RouterCommand(RouterCommand::AddEpochValidatorSet {
                        epoch: validator_set_data.epoch,
                        validator_set: validator_set_data.validators.get_stakes(),
                    }),
                    Command::ConfigFileCommand(ConfigFileCommand::ValidatorSetData {
                        validator_set_data,
                    }),
                ]
            }
        }
    }
}
