use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use monad_executor::{
    Executor, InnerMempoolCommand, InnerMempoolEvent, Mempool, MempoolSupervisor,
};
use monad_executor_glue::MempoolCommand;
use monad_gossip::Gossip;

use crate::service::{QuinnConfig, Service};

pub struct NetworkServiceMempoolExecutor<QC, G, ME>
where
    Self: Unpin,
    QC: QuinnConfig,
    G: Gossip,
    ME: Mempool + Unpin,
{
    service: Service<QC, G, ME::InboundMessage, ME::OutboundMessage>,
    mempool: ME,
}

impl<QC, G, ME> MempoolSupervisor for NetworkServiceMempoolExecutor<QC, G, ME>
where
    Self: Unpin,
    QC: QuinnConfig,
    G: Gossip,
    ME: Mempool,
{
    type Config = Service<QC, G, ME::InboundMessage, ME::OutboundMessage>;
    type SignatureCollection = ME::SignatureCollection;
    type Mempool = ME;
    type Event = ME::Event;

    fn new(service: Self::Config, mempool_config: ME::Config) -> Self {
        Self {
            service,
            mempool: ME::new(mempool_config),
        }
    }
}

impl<QC, G, ME> Executor for NetworkServiceMempoolExecutor<QC, G, ME>
where
    Self: Unpin,
    QC: QuinnConfig,
    G: Gossip,
    ME: Mempool,
{
    type Command = MempoolCommand<<Self as MempoolSupervisor>::SignatureCollection>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.mempool.exec(
            commands
                .into_iter()
                .map(InnerMempoolCommand::Command)
                .collect(),
        )
    }
}

impl<QC, G, ME> Stream for NetworkServiceMempoolExecutor<QC, G, ME>
where
    Self: Unpin,
    QC: QuinnConfig,
    G: Gossip,
    ME: Mempool,
{
    type Item = <Self as MempoolSupervisor>::Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.mempool.poll_next_unpin(cx) {
            match result {
                None => return Poll::Ready(None),
                Some(event) => match event {
                    InnerMempoolEvent::StateEvent(event) => return Poll::Ready(Some(event)),
                    InnerMempoolEvent::Tx(vec) => todo!(),
                },
            }
        }

        Poll::Pending
    }
}
