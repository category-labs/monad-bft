use std::marker::PhantomData;

use monad_async_state_verify::{AsyncStateVerifyCommand, AsyncStateVerifyProcess};
use monad_consensus::{messages::message::PeerStateRootMessage, validation::signing::Validated};
use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::StateRootValidator,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    AsyncStateVerifyEvent, Command, ConsensusEvent, LoopbackCommand, MonadEvent, RouterCommand,
};
use monad_types::{NodeId, RouterTarget};
use monad_validator::{
    epoch_manager::EpochManager, validator_set::ValidatorSetTypeFactory,
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};

pub(super) struct AsyncStateVerifyChildState<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    async_state_verify: &'a mut ASVT,

    /// Needs these to validated message
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    nodeid: NodeId<SCT::NodeIdPubKey>,
    /// Keypair sign the PeerStateRootMessage
    cert_keypair: &'a SignatureCollectionKeyPairType<SCT>,

    metrics: &'a mut Metrics,

    _phantom: PhantomData<(ST, LT, TT, BVT, SVT, BPT)>,
}

impl<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
    AsyncStateVerifyChildState<'a, ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
    SVT: StateRootValidator,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            async_state_verify: &mut monad_state.async_state_verify,
            epoch_manager: &monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,
            nodeid: monad_state.consensus.get_nodeid(),
            cert_keypair: monad_state.consensus.get_cert_keypair(),

            metrics: &mut monad_state.metrics,

            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: AsyncStateVerifyEvent<SCT>,
    ) -> Vec<WrappedAsyncStateVerifyCommand<SCT>> {
        let cmds = match event {
            AsyncStateVerifyEvent::PeerStateRoot {
                sender,
                unvalidated_message,
            } => {
                let validated = match unvalidated_message.validate(
                    &sender,
                    self.epoch_manager,
                    self.val_epoch_map,
                ) {
                    Ok(m) => m.into_inner(),
                    Err(e) => {
                        handle_validation_error(e, self.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                let PeerStateRootMessage { peer, info, sig } = validated;

                let epoch = self.epoch_manager.get_epoch(info.round);
                let valset = self
                    .val_epoch_map
                    .get_val_set(&epoch)
                    .expect("validator set present");

                let valmap = self
                    .val_epoch_map
                    .get_cert_pubkeys(&epoch)
                    .expect("validator mapping present");

                self.async_state_verify
                    .handle_peer_state_root(peer, info, sig, valset, valmap)
            }
            AsyncStateVerifyEvent::LocalStateRoot(info) => {
                self.async_state_verify.handle_local_state_root(
                    self.nodeid,
                    self.cert_keypair,
                    info,
                    self.epoch_manager.get_epoch(info.round),
                )
            }
        };

        cmds.into_iter()
            .map(WrappedAsyncStateVerifyCommand)
            .collect::<Vec<_>>()
    }
}

pub(super) struct WrappedAsyncStateVerifyCommand<SCT: SignatureCollection>(
    AsyncStateVerifyCommand<SCT>,
);

impl<ST, SCT> From<WrappedAsyncStateVerifyCommand<SCT>>
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
    fn from(value: WrappedAsyncStateVerifyCommand<SCT>) -> Self {
        match value.0 {
            AsyncStateVerifyCommand::BroadcastStateRoot {
                peer,
                info,
                sig,
                epoch,
            } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::Broadcast(epoch, info.round),
                    message: VerifiedMonadMessage::PeerStateRootMessage(Validated::new(
                        PeerStateRootMessage { peer, info, sig },
                    )),
                })]
            }
            AsyncStateVerifyCommand::StateRootUpdate(info) => {
                vec![Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::<ST, SCT>::ConsensusEvent(ConsensusEvent::StateUpdate(info)),
                ))]
            }
        }
    }
}
