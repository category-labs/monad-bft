use std::marker::PhantomData;

use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{
    block::Block, signature_collection::SignatureCollection, voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, MonadEvent, ValidatorEvent};
use monad_validator::{
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct EpochChildState<'a, ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    val_epoch_map: &'a mut ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(ST, LT, TT, BVT, SVT, ASVT)>,
}

pub(super) struct EpochCommand {}

impl<'a, ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
    EpochChildState<'a, ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            val_epoch_map: &mut monad_state.val_epoch_map,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(&mut self, event: ValidatorEvent<SCT>) -> Vec<EpochCommand> {
        match event {
            ValidatorEvent::UpdateValidators((validator_data, epoch)) => {
                self.val_epoch_map.insert(
                    epoch,
                    validator_data.get_stakes(),
                    ValidatorMapping::new(validator_data.get_cert_pubkeys()),
                );
                vec![]
            }
        }
    }
}

impl<ST, SCT> From<EpochCommand>
    for Vec<
        Command<
            MonadEvent<ST, SCT>,
            VerifiedMonadMessage<ST, SCT>,
            Block<SCT>,
            Checkpoint<SCT>,
            SCT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: EpochCommand) -> Self {
        Vec::new()
    }
}
