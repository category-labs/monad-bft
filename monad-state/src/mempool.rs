use std::marker::PhantomData;

use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{
    block::Block, signature_collection::SignatureCollection, tx_processor::TransactionProcessor,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, MempoolEvent, MonadEvent};
use monad_validator::validator_set::ValidatorSetTypeFactory;

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct MempoolChildState<'a, ST, SCT, VT, LT, TPT, SVT, ASVT> {
    tx_processor: &'a mut TPT,

    _phantom: PhantomData<(ST, SCT, VT, LT, SVT, ASVT)>,
}

pub(super) struct MempoolCommand {}

impl<'a, ST, SCT, VT, LT, TPT, SVT, ASVT> MempoolChildState<'a, ST, SCT, VT, LT, TPT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
    TPT: TransactionProcessor,
{
    pub(super) fn new(monad_state: &'a mut MonadState<ST, SCT, VT, LT, TPT, SVT, ASVT>) -> Self {
        Self {
            tx_processor: &mut monad_state.tx_processor,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(&mut self, event: MempoolEvent<SCT>) -> Vec<MempoolCommand> {
        match event {
            MempoolEvent::UserTxns(txns) => {
                for tx in txns {
                    self.tx_processor.insert_tx(tx);
                }
                vec![]
            }
            MempoolEvent::CascadeTxns { sender, txns } => {
                self.tx_processor.handle_cascading_txns();
                vec![]
            }
        }
    }
}

impl<ST, SCT> From<MempoolCommand>
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
    fn from(value: MempoolCommand) -> Self {
        Vec::new()
    }
}
