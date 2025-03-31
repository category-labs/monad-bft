use std::marker::PhantomData;

use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_compress::CompressionAlgo;
use monad_consensus_types::{
    block::BlockPolicy, block_validator::BlockValidator, signature_collection::SignatureCollection,
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor_glue::{Command, MonadEvent, RouterCommand, ValidatorEvent};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, ExecutionProtocol, NodeId, Stake};
use monad_validator::{
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct EpochChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, CA>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    val_epoch_map: &'a mut ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, CA)>,
}

pub(super) enum EpochCommand<PT>
where
    PT: PubKey,
{
    AddEpochValidatorSet(Epoch, Vec<(NodeId<PT>, Stake)>),
}

impl<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, CA>
    EpochChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, CA>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    CA: CompressionAlgo,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, CA>,
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

impl<ST, SCT, EPT, BPT, SBT> From<EpochCommand<CertificateSignaturePubKey<ST>>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
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
