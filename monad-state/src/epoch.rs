use std::marker::PhantomData;

use monad_consensus_types::{
    block::BlockPolicy, block_validator::BlockValidator, signature_collection::SignatureCollection,
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};
use monad_executor_glue::{Command, MonadEvent, RouterCommand, ValidatorEvent};
use monad_types::{Epoch, NodeId, Stake};
use monad_validator::{
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct EpochChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    val_epoch_map: &'a mut ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(ST, LT, TT, BVT, SVT, ASVT, BPT, SBT, RBCT)>,
}

pub(super) enum EpochCommand<PT>
where
    PT: PubKey,
{
    AddEpochValidatorSet(Epoch, Vec<(NodeId<PT>, Stake)>),
}

impl<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    EpochChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            val_epoch_map: &mut monad_state.val_epoch_map,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: ValidatorEvent<SCT>,
    ) -> Vec<EpochCommand<SCT::NodeIdPubKey>> {
        match event {
            ValidatorEvent::UpdateValidators((validator_data, epoch)) => {
                self.val_epoch_map.insert(
                    epoch,
                    validator_data.get_stakes(),
                    ValidatorMapping::new(validator_data.get_cert_pubkeys()),
                );
                vec![EpochCommand::AddEpochValidatorSet(
                    epoch,
                    validator_data.get_stakes(),
                )]
            }
        }
    }
}

impl<ST, SCT> From<EpochCommand<CertificateSignaturePubKey<ST>>>
    for Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(command: EpochCommand<CertificateSignaturePubKey<ST>>) -> Self {
        match command {
            EpochCommand::AddEpochValidatorSet(epoch, validator_set) => {
                vec![Command::RouterCommand(
                    RouterCommand::AddEpochValidatorSet {
                        epoch,
                        validator_set,
                    },
                )]
            }
        }
    }
}
