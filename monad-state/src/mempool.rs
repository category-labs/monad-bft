use std::marker::PhantomData;

use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    signature_collection::SignatureCollection,
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, MempoolEvent, MonadEvent};
use monad_validator::validator_set::ValidatorSetTypeFactory;

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct MempoolChildState<'a, ST, SCT, BPT, VT, LT, TT, BVT, SVT, ASVT> {
    txpool: &'a mut TT,

    _phantom: PhantomData<(ST, SCT, BPT, VT, LT, BVT, SVT, ASVT)>,
}

pub(super) struct MempoolCommand {}

impl<'a, ST, SCT, BPT, VT, LT, TT, BVT, SVT, ASVT>
    MempoolChildState<'a, ST, SCT, BPT, VT, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
    VT: ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
    TT: TxPool<SCT, BPT>,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, VT, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            txpool: &mut monad_state.txpool,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(&mut self, event: MempoolEvent<SCT>) -> Vec<MempoolCommand> {
        match event {
            MempoolEvent::UserTxns(txns) => {
                for tx in txns {
                    self.txpool.insert_tx(tx);
                }
                vec![]
            }
            MempoolEvent::CascadeTxns { sender, txns } => {
                self.txpool.handle_cascading_txns();
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
