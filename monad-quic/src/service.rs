use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    error::Error,
    marker::PhantomData,
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, SelectAll},
    Future, FutureExt, Stream, StreamExt,
};
use monad_crypto::{
    rustls::{self, UnsafeTlsVerifier},
    secp256k1::PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, PeerId, RouterCommand};
use monad_gossip::{Gossip, GossipEvent};
use monad_types::{Deserializable, Serializable};
use quinn::{Connecting, Connection, Endpoint, RecvStream, SendStream};
use quinn_proto::{ClientConfig, TransportConfig};

// used for TLS self-signed cert
const SERVER_NAME: &str = "MONAD";

pub struct Service<QC, G, M, OM>
where
    G: Gossip,
    M: Message,
{
    zero_instant: Instant,
    me: PeerId,
    known_addresses: HashMap<PeerId, SocketAddr>,

    gossip: G,

    endpoint: quinn::Endpoint,
    quinn_config: QC,

    accept: BoxFuture<'static, Option<Connecting>>,
    inbound_connections: SelectAll<InboundConnection>,

    idle_outbound_connections: HashMap<PeerId, IdleOutboundConnection>,
    busy_outbound_connections: FuturesUnordered<BusyOutboundConnection>,
    // invariant: if outbound connection to a peer exists in {idle | busy}, there must exist an
    // entry in pending_outbound_messages
    pending_outbound_messages: HashMap<PeerId, VecDeque<Vec<u8>>>,

    gossip_timeout: Pin<Box<tokio::time::Sleep>>,

    _pd: PhantomData<(M, OM)>,
}

pub trait QuinnConfig {
    fn transport(&self) -> Arc<TransportConfig>;
    fn client(&self) -> Arc<dyn quinn_proto::crypto::ClientConfig>;
    fn server(&self) -> Arc<dyn quinn_proto::crypto::ServerConfig>;

    fn remote_peer_id(connection: &quinn::Connection) -> Result<PeerId, Box<dyn Error>>;
}

pub struct ServiceConfig<QC> {
    pub zero_instant: Instant,
    pub me: PeerId,

    pub server_address: SocketAddr,
    pub quinn_config: QC,

    pub known_addresses: HashMap<PeerId, SocketAddr>,
}

impl<QC, G, M, OM> Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message,
{
    pub fn new(config: ServiceConfig<QC>, gossip_config: G::Config) -> Self {
        let mut server_config = quinn::ServerConfig::with_crypto(config.quinn_config.server());
        server_config.transport_config(config.quinn_config.transport());
        let endpoint = quinn::Endpoint::server(server_config, config.server_address)
            .expect("Endpoint initialization shouldn't fail");

        let accept = {
            let endpoint = endpoint.clone();
            async move { endpoint.accept().await }.boxed()
        };

        let gossip = G::new(gossip_config);

        Self {
            zero_instant: config.zero_instant,
            me: config.me,
            known_addresses: config.known_addresses,

            gossip,

            endpoint,
            quinn_config: config.quinn_config,

            accept,
            inbound_connections: SelectAll::new(),

            idle_outbound_connections: HashMap::new(),
            busy_outbound_connections: FuturesUnordered::new(),
            pending_outbound_messages: HashMap::new(),

            gossip_timeout: Box::pin(tokio::time::sleep(Duration::ZERO)),

            _pd: PhantomData,
        }
    }
}

impl<QC, G, M, OM> Executor for Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<[u8]> + Send + Sync + 'static,
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
                }
            }
        }
    }
}

impl<QC, G, M, OM> Stream for Service<QC, G, M, OM>
where
    QC: QuinnConfig,
    G: Gossip,
    M: Message + Deserializable<[u8]> + Send + Sync + 'static,
    <M as Deserializable<[u8]>>::ReadError: 'static,
    OM: Serializable<Vec<u8>> + Send + Sync + 'static,

    OM: Into<M> + AsRef<M> + Clone,

    Self: Unpin,
{
    type Item = M::Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        let time = this.zero_instant.elapsed();

        loop {
            if let Poll::Ready(connecting) = this.accept.poll_unpin(cx) {
                let endpoint = this.endpoint.clone();
                this.accept = async move { endpoint.accept().await }.boxed();

                let connecting = connecting.expect("endpoint shouldn't close");
                this.inbound_connections
                    .push(InboundConnection::pending::<QC>(connecting));
            }

            if !this.inbound_connections.is_empty() {
                if let Poll::Ready(message) = this.inbound_connections.poll_next_unpin(cx) {
                    let (from, gossip_message) =
                        message.expect("inbound stream should never be exhausted");
                    this.gossip
                        .handle_gossip_message(time, from, &gossip_message);
                }
            }

            if !this.busy_outbound_connections.is_empty() {
                if let Poll::Ready(idle_connection) =
                    this.busy_outbound_connections.poll_next_unpin(cx)
                {
                    let idle_connection =
                        idle_connection.expect("outbound connections stream shouldn't close");
                    let pending_messages = this.pending_outbound_messages.get_mut(&idle_connection.peer_id).expect("invariant: if connection exists, pending_outbound_messages should be non_empty");
                    if let Some(outbound_message) = pending_messages.pop_front() {
                        this.busy_outbound_connections
                            .push(BusyOutboundConnection::send(
                                idle_connection,
                                outbound_message,
                            ));
                        continue;
                    } else {
                        let replaced = this
                            .idle_outbound_connections
                            .insert(idle_connection.peer_id, idle_connection);
                        assert!(replaced.is_none())
                    }
                }
            }

            if let Some(timeout) = this.gossip.peek_tick() {
                if this.zero_instant + timeout > this.gossip_timeout.deadline().into_std() {
                    tokio::time::Sleep::reset(
                        this.gossip_timeout.as_mut(),
                        (this.zero_instant + timeout).into(),
                    );
                }
                if let Poll::Ready(()) = this.gossip_timeout.poll_unpin(cx) {
                    match this.gossip.poll(time) {
                        Some(GossipEvent::Send(to, gossip_message)) => {
                            if to == this.me {
                                this.gossip
                                    .handle_gossip_message(time, this.me, &gossip_message)
                            } else {
                                match this.pending_outbound_messages.entry(to) {
                                    Entry::Occupied(mut e) => {
                                        // connection is either in idle_outbound_connections or
                                        // in busy_outbound_connections

                                        if let Some(active_connection) =
                                            this.idle_outbound_connections.remove(&to)
                                        {
                                            this.busy_outbound_connections.push(
                                                BusyOutboundConnection::send(
                                                    active_connection,
                                                    gossip_message,
                                                ),
                                            );
                                            continue;
                                        } else {
                                            // currently in process of connecting, queue up message
                                            e.get_mut().push_back(gossip_message);
                                        }
                                    }
                                    Entry::Vacant(e) => {
                                        // no active connection - must dial
                                        e.insert(Default::default()).push_back(gossip_message);

                                        let known_address = match this.known_addresses.get(&to) {
                                            Some(address) => address,
                                            None => todo!("what do we do for peer discovery?"),
                                        };

                                        this.busy_outbound_connections.push(
                                            BusyOutboundConnection::connect(
                                                &this.quinn_config,
                                                &this.endpoint,
                                                *known_address,
                                                to,
                                            ),
                                        );
                                        continue;
                                    }
                                }
                            }
                        }
                        Some(GossipEvent::Emit(from, app_message)) => {
                            let message = match M::deserialize(&app_message) {
                                Ok(m) => m,
                                Err(e) => todo!("err deserializing message: {:?}", e),
                            };
                            return Poll::Ready(Some(message.event(from)));
                        }
                        None => {}
                    }
                }
                // loop if don't return value, because need to re-poll timeout
                continue;
            }

            return Poll::Pending;
        }
    }
}

type InboundConnectionError = Box<dyn Error>;
enum InboundConnection {
    Pending(BoxFuture<'static, Result<(PeerId, RecvStream), InboundConnectionError>>),
    Active(BoxFuture<'static, Result<(PeerId, RecvStream, Vec<u8>), InboundConnectionError>>),
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
    fn active(peer: PeerId, mut stream: RecvStream) -> Self {
        let fut = async move {
            let mut buf = vec![0_u8; 1024];
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
    type Item = (PeerId, Vec<u8>);

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

struct IdleOutboundConnection {
    peer_id: PeerId,
    connection: Connection,
    stream: SendStream,
}

struct BusyOutboundConnection(
    BoxFuture<'static, Result<IdleOutboundConnection, OutboundConnectionError>>,
);

impl BusyOutboundConnection {
    fn connect<QC: QuinnConfig>(
        config: &QC,
        endpoint: &Endpoint,
        address: SocketAddr,
        expected_peer_id: PeerId,
    ) -> Self {
        let endpoint = endpoint.clone();
        let client_config = {
            let mut c = ClientConfig::new(config.client());
            c.transport_config(config.transport());
            c
        };
        let fut = async move {
            let connection = endpoint
                .connect_with(client_config, address, SERVER_NAME)?
                .await?;

            if QC::remote_peer_id(&connection)? != expected_peer_id {
                return Err("unexpected peer_id".into());
            }

            let stream = connection.open_uni().await?;

            Ok(IdleOutboundConnection {
                peer_id: expected_peer_id,
                connection,
                stream,
            })
        }
        .boxed();
        Self(fut)
    }

    fn send(mut connection: IdleOutboundConnection, message: Vec<u8>) -> Self {
        let fut = async move {
            connection.stream.write_all(&message).await?;
            Ok(connection)
        }
        .boxed();
        Self(fut)
    }
}

impl Future for BusyOutboundConnection
where
    Self: Unpin,
{
    type Output = IdleOutboundConnection;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.deref_mut();

        if let Poll::Ready(maybe_connection) = this.0.poll_unpin(cx) {
            match maybe_connection {
                Ok(connection) => {
                    return Poll::Ready(connection);
                }
                Err(e) => todo!(
                    "TODO error connecting/sending, should we ignore and log this? err={:?}",
                    e
                ),
            }
        }

        Poll::Pending
    }
}

pub struct UnsafeNoAuthQuinnConfig {
    me: PeerId,
    transport: Arc<TransportConfig>,
    client: Arc<dyn quinn_proto::crypto::ClientConfig>,
}

impl UnsafeNoAuthQuinnConfig {
    pub fn new(me: PeerId) -> Self {
        Self {
            me,
            client: Arc::new(UnsafeTlsVerifier::make_client_config(me.0.bytes(), &[])),
            transport: Arc::new(TransportConfig::default()),
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

    fn remote_peer_id(connection: &quinn::Connection) -> Result<PeerId, Box<dyn Error>> {
        let identity = connection
            .peer_identity()
            .expect("all quic sessions have TLS identity");
        let certificates: Box<Vec<rustls::Certificate>> = identity
            .downcast()
            .expect("always is rustls cert for default quinn");

        let raw_cert = certificates
            .get(0)
            .ok_or("no attached certificates".to_owned())?;

        use x509_parser::prelude::*;
        let (_, cert) = X509Certificate::from_der(&raw_cert.0)?;

        let extension = cert.extensions().get(0).ok_or("no extensions".to_owned())?;

        let peer_id = PeerId(PubKey::from_slice(extension.value)?);
        Ok(peer_id)
    }
}
