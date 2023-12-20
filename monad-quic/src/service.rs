use std::{
    collections::HashMap,
    error::Error,
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    task::{Poll, Waker},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{future::BoxFuture, stream::SelectAll, FutureExt, Stream, StreamExt};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::{Gossip, GossipEvent};
use monad_types::{Deserializable, NodeId, Serializable};
use quinn::{Connecting, RecvStream};
use quinn_proto::ClientConfig;
use tokio::sync::mpsc::error::TrySendError;

use crate::quinn_config::QuinnConfig;

/// Service is an implementation of a RouterCommand updater that's backed by Quic
/// It can be parameterized by a Gossip algorithm
pub struct Service<QC, G, M, OM>
where
    G: Gossip,
    M: Message,
{
    /// An arbitrary starting time - used for computing internally consistent relative times,
    /// which are in std::time::Duration, for the Gossip trait
    zero_instant: Instant,
    /// The NodeId of self
    me: NodeId,
    /// known_addresses is used for knowing what address to dial to connect to a given PeerId.
    ///
    /// The dialed peer's certificate MUST be validated to ensure that it matches the expected
    /// PeerId.
    ///
    /// This might be replaced in the future once we support peer discovery
    known_addresses: HashMap<NodeId, SocketAddr>,

    /// The gossip implementation
    gossip: G,

    /// The main entrypoint into Quinn's API
    endpoint: quinn::Endpoint,
    /// Configuration generator used for Quinn initialization and connections
    quinn_config: QC,

    /// Future that yields on the next inbound connection attempt
    accept: BoxFuture<'static, Connecting>,
    /// SelectAll over InboundConnection streams
    /// Each InboundConnection corresponds to a single inbound connection
    /// Polling from inbound_connections yields the next ready inbound connection event
    inbound_connections: SelectAll<InboundConnection>,
    /// Sender channels for each currently open outbound connection
    outbound_messages: HashMap<NodeId, tokio::sync::mpsc::Sender<Bytes>>,

    /// Future that yields whenever the gossip implementation wants to be woken up
    gossip_timeout: Pin<Box<tokio::time::Sleep>>,
    waker: Option<Waker>,

    _pd: PhantomData<(M, OM)>,
}

/// Configuration for Service
pub struct ServiceConfig<QC> {
    /// The NodeId of self
    pub me: NodeId,

    /// The UDP address to bind the quic endpoint to
    pub server_address: SocketAddr,
    /// Quinn configuration
    pub quinn_config: QC,

    /// Lookup table for addresses of peers
    ///
    /// Currently, the entire validator set must be present here, because peer discovery is not
    /// supported
    pub known_addresses: HashMap<NodeId, SocketAddr>,
}

impl<QC, G, M, OM> Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message,
{
    pub fn new(config: ServiceConfig<QC>, gossip: G) -> Self {
        let mut server_config = quinn::ServerConfig::with_crypto(config.quinn_config.server());
        server_config.transport_config(config.quinn_config.transport());
        let endpoint = quinn::Endpoint::server(server_config, config.server_address)
            .unwrap_or_else(|_| {
                panic!(
                    "Endpoint initialization shouldn't fail: {:?}",
                    config.server_address
                )
            });

        let accept = {
            let endpoint = endpoint.clone();
            #[allow(clippy::async_yields_async)]
            async move { endpoint.accept().await.expect("endpoint is never closed") }.boxed()
        };

        Self {
            zero_instant: Instant::now(),
            me: config.me,
            known_addresses: config.known_addresses,

            gossip,

            endpoint,
            quinn_config: config.quinn_config,

            accept,
            inbound_connections: SelectAll::new(),
            outbound_messages: HashMap::new(),

            gossip_timeout: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waker: None,

            _pd: PhantomData,
        }
    }
}

impl<QC, G, M, OM> Executor for Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<Bytes> + Send + Sync + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    OM: Into<M> + Clone,
{
    type Command = RouterCommand<OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let time = self.zero_instant.elapsed();

        for command in commands {
            match command {
                RouterCommand::Publish { target, message } => {
                    let message = {
                        let mut _ser_span = tracing::info_span!("serialize_span").entered();
                        message.serialize()
                    };
                    let mut _publish_span =
                        tracing::info_span!("publish_span", message_len = message.len()).entered();
                    self.gossip.send(time, target, message);

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }
}

impl<QC, G, M, OM> Stream for Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<Bytes> + Send + Sync + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,

    OM: Into<M> + Clone,

    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut _router_poll_span = tracing::info_span!("router_poll_span").entered();

        let this = self.deref_mut();
        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }
        let time = this.zero_instant.elapsed();

        loop {
            if let Poll::Ready(connecting) = this.accept.poll_unpin(cx) {
                let endpoint = this.endpoint.clone();
                this.accept = {
                    #[allow(clippy::async_yields_async)]
                    async move { endpoint.accept().await.expect("endpoint is never closed") }
                        .boxed()
                };

                this.inbound_connections
                    .push(InboundConnection::pending::<QC>(connecting));
                continue;
            }

            if !this.inbound_connections.is_empty() {
                if let Poll::Ready(message) = this.inbound_connections.poll_next_unpin(cx) {
                    let (from, gossip_messages) =
                        message.expect("inbound stream should never be exhausted");
                    for gossip_message in gossip_messages {
                        this.gossip
                            .handle_gossip_message(time, from, gossip_message);
                    }
                    continue;
                }
            }

            if let Some(timeout) = this.gossip.peek_tick() {
                let deadline = this.zero_instant + timeout;
                if deadline > this.gossip_timeout.deadline().into_std() {
                    tokio::time::Sleep::reset(this.gossip_timeout.as_mut(), deadline.into());
                }
                if this.gossip_timeout.poll_unpin(cx).is_ready() || timeout <= time {
                    match this.gossip.poll(time) {
                        Some(GossipEvent::Send(to, gossip_message)) => {
                            if to == this.me {
                                this.gossip
                                    .handle_gossip_message(time, this.me, gossip_message);
                            } else {
                                let maybe_unsent_gossip_message = match this
                                    .outbound_messages
                                    .get_mut(&to)
                                {
                                    Some(sender) => {
                                        let result = sender.try_send(gossip_message);

                                        match result {
                                            Ok(()) => None,
                                            Err(TrySendError::Full(gossip_message)) => {
                                                todo!("channel full, how should we handle this?")
                                            }
                                            Err(TrySendError::Closed(gossip_message)) => {
                                                // this implies that the connection died
                                                this.outbound_messages.remove(&to);
                                                Some(gossip_message)
                                            }
                                        }
                                    }
                                    None => Some(gossip_message),
                                };

                                if let Some(unsent_gossip_message) = maybe_unsent_gossip_message {
                                    const MAX_BUFFERED_MESSAGES: usize = 100;
                                    let (sender, mut receiver) =
                                        tokio::sync::mpsc::channel(MAX_BUFFERED_MESSAGES);
                                    sender
                                        .try_send(unsent_gossip_message)
                                        .expect("try_send should always succeed on new chan");
                                    this.outbound_messages.insert(to, sender);

                                    let known_address = match this.known_addresses.get(&to) {
                                        Some(address) => *address,
                                        None => todo!("what do we do for peer discovery?"),
                                    };

                                    let endpoint = this.endpoint.clone();
                                    let client_config = {
                                        let mut c = ClientConfig::new(this.quinn_config.client());
                                        c.transport_config(this.quinn_config.transport());
                                        c
                                    };
                                    tokio::spawn(async move {
                                        let fut = async move {
                                            let connection = endpoint
                                                .connect_with(
                                                    client_config,
                                                    known_address,
                                                    "MONAD", // server_name doesn't matter because we're verifying the peer_id that signed the certificate
                                                )?
                                                .await?;

                                            if QC::remote_peer_id(&connection) != to {
                                                todo!("unexpected peer_id, return and retry?");
                                            }

                                            let mut stream = connection.open_uni().await?;

                                            while let Some(gossip_message) = receiver.recv().await {
                                                stream.write_all(&gossip_message).await?;
                                            }
                                            Ok::<_, OutboundConnectionError>(())
                                        };
                                        if let Err(e) = fut.await {
                                            todo!("handle outbound connection err: {:?}", e);
                                        }
                                    });
                                }
                            }
                        }
                        Some(GossipEvent::Emit(from, app_message)) => {
                            let message = {
                                let mut _deser_span = tracing::info_span!(
                                    "deserialize_span",
                                    message_len = app_message.len()
                                )
                                .entered();
                                match M::deserialize(&app_message) {
                                    Ok(m) => m,
                                    Err(e) => todo!("err deserializing message: {:?}", e),
                                }
                            };
                            let event = {
                                let mut _message_to_event_span = tracing::info_span!(
                                    "message_to_event_span",
                                    message_len = app_message.len()
                                )
                                .entered();
                                message.event(from)
                            };
                            return Poll::Ready(Some(event));
                        }
                        None => {}
                    }
                    // loop if don't return value, because need to re-poll timeout
                    continue;
                }
            }

            return Poll::Pending;
        }
    }
}

type InboundConnectionError = Box<dyn Error>;
enum InboundConnection {
    Pending(BoxFuture<'static, Result<(NodeId, RecvStream), InboundConnectionError>>),
    Active(BoxFuture<'static, Result<(NodeId, RecvStream, Vec<Bytes>), InboundConnectionError>>),
}

impl InboundConnection {
    fn pending<QC: QuinnConfig>(connecting: Connecting) -> Self {
        let fut = async move {
            let connection = connecting.await?;
            let peer_id = QC::remote_peer_id(&connection);

            let stream = connection.accept_uni().await?;
            Ok((peer_id, stream))
        }
        .boxed();
        Self::Pending(fut)
    }
    fn active(peer: NodeId, mut stream: RecvStream) -> Self {
        let fut = async move {
            let mut bufs = vec![Bytes::new(); 32];
            let num_chunks = stream.read_chunks(&mut bufs).await?.unwrap_or(0);
            bufs.truncate(num_chunks);
            Ok((peer, stream, bufs))
        };
        Self::Active(fut.boxed())
    }
}

impl Stream for InboundConnection
where
    Self: Unpin,
{
    type Item = (NodeId, Vec<Bytes>);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        loop {
            match this {
                InboundConnection::Pending(pending) => {
                    if let Poll::Ready(maybe_stream) = pending.poll_unpin(cx) {
                        match maybe_stream {
                            Ok((peer_id, stream)) => {
                                *this = InboundConnection::active(peer_id, stream);
                                continue;
                            }
                            Err(e) => todo!("TODO error accepting connection, should we ignore and log this? err={:?}", e),
                        }
                    }
                }
                InboundConnection::Active(stream) => {
                    if let Poll::Ready(maybe_bytes) = stream.poll_unpin(cx) {
                        match maybe_bytes {
                            Ok((peer, stream, chunks)) => {
                                if chunks.is_empty() {
                                    return Poll::Ready(None);
                                }
                                *this = InboundConnection::active(peer, stream);
                                return Poll::Ready(Some((peer, chunks)));
                            }
                            Err(e) => todo!("connection read stream err: {:?}", e),
                        }
                    }
                }
            };
            return Poll::Pending;
        }
    }
}

type OutboundConnectionError = Box<dyn Error>;
