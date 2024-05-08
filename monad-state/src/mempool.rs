use std::marker::PhantomData;

use monad_blocktree::blocktree::HashPolicy;
use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{
    block::Block, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, MempoolEvent, MonadEvent};
use monad_validator::validator_set::ValidatorSetTypeFactory;

use crate::{MonadState, VerifiedMonadMessage};

pub(super) struct MempoolChildState<'a, ST, SCT, VT, LT, TT, BVT, SVT, ASVT, HP> {
    txpool: &'a mut TT,

    _phantom: PhantomData<(ST, SCT, VT, LT, BVT, SVT, ASVT, HP)>,
}

pub(super) struct MempoolCommand {}

impl<'a, ST, SCT, VT, LT, TT, BVT, SVT, ASVT, HP>
    MempoolChildState<'a, ST, SCT, VT, LT, TT, BVT, SVT, ASVT, HP>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
    TT: TxPool,
    HP: HashPolicy,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, VT, LT, TT, BVT, SVT, ASVT, HP>,
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
