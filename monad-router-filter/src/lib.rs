use std::{marker::PhantomData, task::Poll};

use futures::{Stream, StreamExt};
use itertools::Itertools;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::Executor;
use monad_executor_glue::{ConsensusEvent, MonadEvent, RouterCommand};
use monad_state::VerifiedMonadMessage;

pub struct FullNodeRouterFilter<ST, SCT, R> {
    router: R,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT, R> FullNodeRouterFilter<ST, SCT, R>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    R: Executor<
            Command = RouterCommand<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT>>,
        > + Stream<Item = MonadEvent<ST, SCT>>
        + Unpin,
{
    pub fn new(router: R) -> Self {
        Self {
            router,
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, R> Executor for FullNodeRouterFilter<ST, SCT, R>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    R: Executor<
        Command = RouterCommand<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT>>,
    >,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT>>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let filtered = commands
            .into_iter()
            .filter_map(|cmd| match &cmd {
                RouterCommand::Publish { target: _, message } => match message {
                    VerifiedMonadMessage::Consensus(_) => None,
                    VerifiedMonadMessage::BlockSyncRequest(_) => Some(cmd),
                    VerifiedMonadMessage::BlockSyncResponse(_) => Some(cmd),
                    VerifiedMonadMessage::PeerStateRootMessage(_) => None,
                    VerifiedMonadMessage::ForwardedTx(_) => Some(cmd),
                    VerifiedMonadMessage::StateSyncMessage(_) => Some(cmd),
                    VerifiedMonadMessage::PingRequest(_) => Some(cmd),
                    VerifiedMonadMessage::PingResponse(_) => Some(cmd),
                },
                RouterCommand::AddEpochValidatorSet { .. } => Some(cmd),
                RouterCommand::UpdateCurrentRound(..) => Some(cmd),
                RouterCommand::GetPeers => Some(cmd),
                RouterCommand::UpdatePeers(_) => Some(cmd),
                RouterCommand::GetFullNodes => Some(cmd),
                RouterCommand::UpdateFullNodes(_vec) => Some(cmd),
            })
            .collect_vec();

        self.router.exec(filtered);
    }

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain {
        self.router.metrics()
    }
}

impl<ST, SCT, R> Stream for FullNodeRouterFilter<ST, SCT, R>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    R: Stream<Item = MonadEvent<ST, SCT>> + Unpin,

    Self: Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            let polled = self.router.poll_next_unpin(cx);
            let polled_event = match polled {
                Poll::Ready(maybe_event) => {
                    let Some(event) = maybe_event else {
                        return Poll::Ready(None);
                    };
                    event
                }
                Poll::Pending => return Poll::Pending,
            };

            let emit_event = match &polled_event {
                MonadEvent::ConsensusEvent(consensus_event) => match consensus_event {
                    ConsensusEvent::Message {
                        sender: _,
                        unverified_message,
                    } => {
                        if unverified_message.is_proposal() {
                            polled_event
                        } else {
                            continue;
                        }
                    }
                    _ => unreachable!(),
                },
                MonadEvent::MempoolEvent(_) => continue,
                _ => polled_event,
            };

            return Poll::Ready(Some(emit_event));
        }
    }
}
