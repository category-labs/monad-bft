use std::{
    collections::HashMap,
    error::Error,
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
    time::{Duration, Instant},
};

use futures::{future::BoxFuture, stream::SelectAll, FutureExt, Stream, StreamExt};
use monad_crypto::{
    rustls::{self, UnsafeTlsVerifier},
    secp256k1::PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::{Gossip, GossipEvent};
use monad_types::{Deserializable, NodeId, Serializable};
use quinn::{Connecting, RecvStream};
use quinn_proto::{congestion::CubicConfig, ClientConfig, TransportConfig};
use tokio::sync::mpsc::error::TrySendError;

// used for TLS self-signed cert
const SERVER_NAME: &str = "MONAD";

pub struct Service<QC, G, M, OM>
where
    G: Gossip,
{
    zero_instant: Instant,
    me: NodeId,
    known_addresses: HashMap<NodeId, SocketAddr>,

    gossip: G,

    endpoint: quinn::Endpoint,
    quinn_config: QC,

    accept: BoxFuture<'static, Option<Connecting>>,
    inbound_connections: SelectAll<InboundConnection>,
    outbound_messages: HashMap<NodeId, tokio::sync::mpsc::Sender<Vec<u8>>>,

    gossip_timeout: Pin<Box<tokio::time::Sleep>>,
    waker: Option<Waker>,

    _pd: PhantomData<(M, OM)>,
}

pub trait QuinnConfig {
    fn transport(&self) -> Arc<TransportConfig>;
    fn client(&self) -> Arc<dyn quinn_proto::crypto::ClientConfig>;
    fn server(&self) -> Arc<dyn quinn_proto::crypto::ServerConfig>;

    fn remote_peer_id(connection: &quinn::Connection) -> Result<NodeId, Box<dyn Error>>;
}

pub struct ServiceConfig<QC> {
    pub zero_instant: Instant,
    pub me: NodeId,

    pub server_address: SocketAddr,
    pub quinn_config: QC,

    pub known_addresses: HashMap<NodeId, SocketAddr>,
}

impl<QC, G, M, OM> Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
{
    pub fn new(config: ServiceConfig<QC>, gossip: G) -> Self {
        let mut server_config = quinn::ServerConfig::with_crypto(config.quinn_config.server());
        server_config.transport_config(config.quinn_config.transport());
        let endpoint =
            quinn::Endpoint::server(server_config, config.server_address).expect(&format!(
                "Endpoint initialization shouldn't fail: {:?}",
                config.server_address
            ));

        let accept = {
            let endpoint = endpoint.clone();
            async move { endpoint.accept().await }.boxed()
        };

        Self {
            zero_instant: config.zero_instant,
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
    M: Deserializable<[u8]> + Send + Sync + 'static,
    <M as Deserializable<[u8]>>::ReadError: 'static,
    OM: Serializable<Vec<u8>> + Send + Sync + 'static,

    OM: Into<M> + AsRef<M> + Clone,
{
    type Command = RouterCommand<OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let time = self.zero_instant.elapsed();

        for command in commands {
            match command {
                RouterCommand::Publish { target, message } => {
                    let message = message.serialize();
                    self.gossip.send(time, target, &message);

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
    M: Deserializable<[u8]> + Send + Sync + 'static,
    <M as Deserializable<[u8]>>::ReadError: 'static,
    OM: Serializable<Vec<u8>> + Send + Sync + 'static,

    OM: Into<M> + AsRef<M> + Clone,

    Self: Unpin,
{
    type Item = (NodeId, M);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }
        let time = this.zero_instant.elapsed();

        loop {
            if let Poll::Ready(connecting) = this.accept.poll_unpin(cx) {
                let endpoint = this.endpoint.clone();
                this.accept = async move { endpoint.accept().await }.boxed();

                let connecting = connecting.expect("endpoint shouldn't close");
                this.inbound_connections
                    .push(InboundConnection::pending::<QC>(connecting));
                continue;
            }

            if !this.inbound_connections.is_empty() {
                if let Poll::Ready(message) = this.inbound_connections.poll_next_unpin(cx) {
                    let (from, gossip_message) =
                        message.expect("inbound stream should never be exhausted");
                    this.gossip
                        .handle_gossip_message(time, from, &gossip_message);
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
                                    .handle_gossip_message(time, this.me, &gossip_message);
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
                                    const MAX_BUFFERED_MESSAGES: usize = 10;
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
                                                    SERVER_NAME,
                                                )?
                                                .await?;

                                            if QC::remote_peer_id(&connection)? != to {
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
                            let message = match M::deserialize(&app_message) {
                                Ok(m) => m,
                                Err(e) => todo!("err deserializing message: {:?}", e),
                            };
                            return Poll::Ready(Some((from, message)));
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
    Active(BoxFuture<'static, Result<(NodeId, RecvStream, Vec<u8>), InboundConnectionError>>),
}

impl InboundConnection {
    fn pending<QC: QuinnConfig>(connecting: Connecting) -> Self {
        let fut = async move {
            let connection = connecting.await?;
            let peer_id = QC::remote_peer_id(&connection)?;

            let stream = connection.accept_uni().await?;
            Ok((peer_id, stream))
        }
        .boxed();
        Self::Pending(fut)
    }
    fn active(peer: NodeId, mut stream: RecvStream) -> Self {
        const RX_MESSAGE_BUFFER_SIZE: usize = 64 * 1024;
        let fut = async move {
            let mut buf = vec![0_u8; RX_MESSAGE_BUFFER_SIZE];
            let bytes = stream.read(&mut buf).await?;
            buf.truncate(bytes.unwrap_or(0));
            Ok((peer, stream, buf))
        };
        Self::Active(fut.boxed())
    }
}

impl Stream for InboundConnection
where
    Self: Unpin,
{
    type Item = (NodeId, Vec<u8>);

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
                            Ok((peer, stream, bytes)) => {
                                if bytes.is_empty() {
                                    return Poll::Ready(None);
                                }

                                *this = InboundConnection::active(peer, stream);
                                return Poll::Ready(Some((peer, bytes)));
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

pub struct UnsafeNoAuthQuinnConfig {
    me: NodeId,
    transport: Arc<TransportConfig>,
    client: Arc<dyn quinn_proto::crypto::ClientConfig>,
}

impl UnsafeNoAuthQuinnConfig {
    /// bandwidth_Mbps is in Megabit/s
    pub fn new(me: NodeId, max_rtt: Duration, bandwidth_Mbps: u16) -> Self {
        let mut transport_config = TransportConfig::default();
        let bandwidth_Bps = bandwidth_Mbps as u64 * 125_000;
        let rwnd = bandwidth_Bps * max_rtt.as_millis() as u64 / 1000;
        transport_config
            .stream_receive_window(u32::try_from(rwnd).unwrap().into())
            .send_window(8 * rwnd)
            .initial_rtt(max_rtt) // not exactly initial.... because of quinn pacer
            .congestion_controller_factory(Arc::new({
                // this is necessary for seeding the quinn pacer correctly on init
                let mut cubic_config = CubicConfig::default();
                cubic_config.initial_window(rwnd);
                cubic_config
            }));
        Self {
            me,
            client: Arc::new(UnsafeTlsVerifier::make_client_config(me.0.bytes(), &[])),
            transport: Arc::new(transport_config),
        }
    }
}

impl QuinnConfig for UnsafeNoAuthQuinnConfig {
    fn transport(&self) -> Arc<TransportConfig> {
        self.transport.clone()
    }

    fn client(&self) -> Arc<dyn quinn_proto::crypto::ClientConfig> {
        self.client.clone()
    }

    fn server(&self) -> Arc<dyn quinn_proto::crypto::ServerConfig> {
        Arc::new(UnsafeTlsVerifier::make_server_config(
            self.me.0.bytes(),
            &[],
        ))
    }

    fn remote_peer_id(connection: &quinn::Connection) -> Result<NodeId, Box<dyn Error>> {
        let identity = connection
            .peer_identity()
            .expect("all quic sessions have TLS identity");
        let certificates: Box<Vec<rustls::Certificate>> = identity
            .downcast()
            .expect("always is rustls cert for default quinn");

        let raw_cert = certificates
            .first()
            .ok_or("no attached certificates".to_owned())?;

        use x509_parser::prelude::*;
        let (_, cert) = X509Certificate::from_der(&raw_cert.0)?;

        let extension = cert
            .extensions()
            .first()
            .ok_or("no extensions".to_owned())?;

        let peer_id = NodeId(PubKey::from_slice(extension.value)?);
        Ok(peer_id)
    }
}

pub struct MessageService<QC, G, M, OM>
where
    G: Gossip,
{
    service: Service<QC, G, M, OM>,
}

impl<QC, G, M, OM> Executor for MessageService<QC, G, M, OM>
where
    G: Gossip,
    Service<QC, G, M, OM>: Executor,
{
    type Command = <Service<QC, G, M, OM> as Executor>::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.service.exec(commands)
    }
}

impl<QC, G, M, OM> Stream for MessageService<QC, G, M, OM>
where
    G: Gossip,
    M: Message,

    Service<QC, G, M, OM>: Unpin + Stream<Item = (NodeId, M)>,
{
    type Item = M::Event;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(data) = self.service.poll_next_unpin(cx) {
            return Poll::Ready(data.map(|(from, msg)| msg.event(from)));
        }

        Poll::Pending
    }
}

impl<QC, G, M, OM> From<Service<QC, G, M, OM>> for MessageService<QC, G, M, OM>
where
    G: Gossip,
{
    fn from(service: Service<QC, G, M, OM>) -> Self {
        Self { service }
    }
}
