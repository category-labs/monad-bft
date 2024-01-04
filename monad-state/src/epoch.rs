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
    validator_set::ValidatorSetType, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct EpochChildState<'a, CP, ST, SCT, VT, LT, TT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    val_epoch_map: &'a mut ValidatorsEpochMapping<VT, SCT>,

    _phantom: PhantomData<(CP, ST, LT, TT)>,
}

pub(super) struct EpochCommand {}

impl<'a, CP, ST, SCT, VT, LT, TT> EpochChildState<'a, CP, ST, SCT, VT, LT, TT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    pub(super) fn new(monad_state: &'a mut MonadState<CP, ST, SCT, VT, LT, TT>) -> Self {
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
                    VT::new(validator_data.get_stakes())
                        .expect("ValidatorData should not have duplicates or invalid entries"),
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
