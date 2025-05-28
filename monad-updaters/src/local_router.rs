use std::{
    collections::HashMap,
    marker::PhantomData,
    ops::DerefMut,
    task::Poll,
    time::{Duration, Instant},
};

use futures::Stream;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_types::{NodeId, RouterTarget};

/// Implementation of the Router which uses tokio channels (as opposed to network links)
/// to communicate between nodes. Useful for local testing
pub struct LocalRouterConfig<PT: PubKey> {
    pub all_peers: Vec<NodeId<PT>>,
    pub external_latency: Duration,
}

impl<PT: PubKey> LocalRouterConfig<PT> {
    pub fn build<ST, M, OM>(self) -> HashMap<NodeId<PT>, LocalPeerRouter<ST, M, OM>>
    where
        ST: CertificateSignatureRecoverable,
        M: Message<NodeIdPubKey = PT> + Send + 'static,
    {
        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();
        for peer in &self.all_peers {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            rxs.insert(*peer, rx);
            txs.insert(*peer, tx);
        }

        let mut peer_txs = HashMap::new(); // for each peer, represents tx map
        for from_peer in &self.all_peers {
            let mut to_peer_txs = HashMap::new();
            for to_peer in &self.all_peers {
                let (delayed_tx, mut delayed_rx) =
                    tokio::sync::mpsc::unbounded_channel::<(Instant, _, _)>();
                let to_tx = txs.get(to_peer).unwrap().clone();
                let delay = if from_peer == to_peer {
                    Duration::ZERO
                } else {
                    self.external_latency
                };
                tokio::spawn(async move {
                    while let Some((instant, from, message)) = delayed_rx.recv().await {
                        let now = Instant::now();
                        // we wake up 2ms early because tokio's timer granularity is 1ms
                        // (1ms is inherited from epoll_wait)
                        let delayed = instant + delay - Duration::from_millis(2);
                        if delayed > now {
                            tokio::time::sleep_until(delayed.into()).await;
                        }
                        // spin until ready
                        while Instant::now() <= delayed {}
                        to_tx.send((from, message)).unwrap();
                    }
                });
                to_peer_txs.insert(*to_peer, delayed_tx);
            }
            peer_txs.insert(*from_peer, to_peer_txs);
        }

        rxs.into_iter()
            .map(|(me, rx)| {
                let router = LocalPeerRouter::new(me, peer_txs.remove(&me).unwrap(), rx);

                (me, router)
            })
            .collect()
    }
}

pub struct LocalPeerRouter<ST, M: Message, OM> {
    me: NodeId<M::NodeIdPubKey>,
    txs: HashMap<
        NodeId<M::NodeIdPubKey>,
        tokio::sync::mpsc::UnboundedSender<(Instant, NodeId<M::NodeIdPubKey>, M)>,
    >,
    rx: tokio::sync::mpsc::UnboundedReceiver<(NodeId<M::NodeIdPubKey>, M)>,

    metrics: ExecutorMetrics,

    _pd: PhantomData<(ST, OM)>,
}

impl<ST, M: Message, OM> LocalPeerRouter<ST, M, OM> {
    fn new(
        me: NodeId<M::NodeIdPubKey>,
        txs: HashMap<
            NodeId<M::NodeIdPubKey>,
            tokio::sync::mpsc::UnboundedSender<(Instant, NodeId<M::NodeIdPubKey>, M)>,
        >,
        rx: tokio::sync::mpsc::UnboundedReceiver<(NodeId<M::NodeIdPubKey>, M)>,
    ) -> Self {
        Self {
            me,
            rx,
            txs,
            metrics: Default::default(),
            _pd: PhantomData,
        }
    }
}

impl<ST, M, OM> Executor for LocalPeerRouter<ST, M, OM>
where
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ST: CertificateSignatureRecoverable,
    OM: Into<M>,
{
    type Command = RouterCommand<ST, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            let now = Instant::now();
            match command {
                RouterCommand::AddEpochValidatorSet { .. } => {}
                RouterCommand::UpdateCurrentRound(_, _) => {}
                RouterCommand::PublishToFullNodes { .. } => {}
                RouterCommand::Publish { target, message } => match target {
                    RouterTarget::Broadcast(_) | RouterTarget::Raptorcast(_) => {
                        let message = message.into();
                        for tx in self.txs.values() {
                            tx.send((now, self.me, message.clone())).unwrap();
                        }
                    }
                    RouterTarget::PointToPoint(peer) => {
                        self.txs
                            .get(&peer)
                            .unwrap()
                            .send((now, self.me, message.into()))
                            .unwrap();
                    }
                    RouterTarget::TcpPointToPoint { to, completion } => {
                        if let Some(completion) = completion {
                            let _ = completion.send(());
                        }
                        self.txs
                            .get(&to)
                            .unwrap()
                            .send((now, self.me, message.into()))
                            .unwrap();
                    }
                },
                RouterCommand::GetPeers => {}
                RouterCommand::UpdatePeers(_) => {}
                RouterCommand::GetFullNodes => {}
                RouterCommand::UpdateFullNodes(_vec) => {}
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, M, OM> Stream for LocalPeerRouter<ST, M, OM>
where
    M: Message,
    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        this.rx
            .poll_recv(cx)
            .map(|maybe_message| maybe_message.map(|(from, message)| message.event(from)))
    }
}
