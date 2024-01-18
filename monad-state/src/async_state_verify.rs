use std::marker::PhantomData;

use monad_async_state_verify::{AsyncStateVerifyCommand, AsyncStateVerifyProcess};
use monad_consensus::{messages::message::PeerStateRootMessage, validation::signing::Validated};
use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{block::Block, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor_glue::{
    AsyncStateVerifyEvent, Command, ConsensusEvent, LoopbackCommand, MonadEvent, RouterCommand,
};
use monad_types::RouterTarget;
use monad_validator::{
    epoch_manager::EpochManager, validator_set::ValidatorSetTypeFactory,
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};
pub(super) struct AsyncStateVerifyChildState<'a, CP, ST, SCT, VTF, LT, TT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    async_state_verify: &'a mut ASVT,

    /// Needs these to validated message
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(CP, ST, LT, TT)>,
}

impl<'a, CP, ST, SCT, VTF, LT, TT, ASVT>
    AsyncStateVerifyChildState<'a, CP, ST, SCT, VTF, LT, TT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ASVT: AsyncStateVerifyProcess<CertificateSignaturePubKey<ST>>,
{
    pub(super) fn new(monad_state: &'a mut MonadState<CP, ST, SCT, VTF, LT, TT, ASVT>) -> Self {
        Self {
            async_state_verify: &mut monad_state.async_state_verify,
            epoch_manager: &monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,

            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: AsyncStateVerifyEvent<SCT::NodeIdPubKey>,
    ) -> Vec<WrappedAsyncStateVerifyCommand<SCT::NodeIdPubKey>> {
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
                        handle_validation_error(e);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                };

                let PeerStateRootMessage {
                    peer,
                    seq_num,
                    round,
                    state_root,
                } = validated;

                self.async_state_verify
                    .handle_peer_state_root(peer, seq_num, round, state_root)
            }
            AsyncStateVerifyEvent::LocalStateRoot {
                seq_num,
                round,
                state_root,
            } => self
                .async_state_verify
                .handle_local_state_root(seq_num, round, state_root),
        };

        cmds.into_iter()
            .map(WrappedAsyncStateVerifyCommand)
            .collect::<Vec<_>>()
    }
}

pub(super) struct WrappedAsyncStateVerifyCommand<P: PubKey>(AsyncStateVerifyCommand<P>);

impl<ST, SCT> From<WrappedAsyncStateVerifyCommand<SCT::NodeIdPubKey>>
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
    fn from(value: WrappedAsyncStateVerifyCommand<SCT::NodeIdPubKey>) -> Self {
        match value.0 {
            AsyncStateVerifyCommand::BroadcastStateRoot {
                self_id,
                seq_num,
                round,
                state_root,
            } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::Broadcast,
                    message: VerifiedMonadMessage::PeerStateRootMessage(Validated::new(
                        PeerStateRootMessage {
                            peer: self_id,
                            seq_num,
                            round,
                            state_root,
                        },
                    )),
                })]
            }
            AsyncStateVerifyCommand::StateRootUpdate {
                seq_num,
                round,
                state_root,
            } => {
                vec![Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::<ST, SCT>::ConsensusEvent(ConsensusEvent::StateUpdate {
                        seq_num,
                        round,
                        state_root,
                    }),
                ))]
            }
        }
    }
}
