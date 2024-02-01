use std::{
    ops::DerefMut,
    pin::Pin,
    task::{ready, Poll},
};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, Stream, TryFutureExt};
use monad_crypto::certificate_signature::PubKey;
use monad_types::NodeId;
use quinn::{Connecting, ReadError, RecvStream, SendDatagramError, SendStream, WriteError};
use quinn_proto::ConnectionError;

use crate::QuinnConfig;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ConnectionId(u64);

pub(crate) enum Connection<PT: PubKey> {
    Pending(
        BoxFuture<
            'static,
            Result<
                (quinn::Connection, NodeId<PT>, SendStream, RecvStream),
                (Option<NodeId<PT>>, ConnectionFailure),
            >,
        >,
    ),
    Active(
        quinn::Connection,
        BoxFuture<'static, Result<(RecvStream, Inbound), ConnectionFailure>>,
    ),
    Closed,
}

pub(crate) enum Inbound {
    Stream(Vec<Bytes>),
    Datagram(Bytes),
}

pub(crate) struct ConnectionWriter {
    connection: quinn::Connection,
    send_stream: SendStream,
}

impl ConnectionWriter {
    fn new(connection: quinn::Connection, send_stream: SendStream) -> Self {
        Self {
            connection,
            send_stream,
        }
    }

    pub fn connection_id(&self) -> ConnectionId {
        ConnectionId(self.connection.stable_id() as u64)
    }

    pub async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), ConnectionFailure> {
        self.send_stream
            .write_chunk(chunk)
            .await
            .map_err(ConnectionFailure::WriteError)
    }

    pub async fn write_chunks(&mut self, chunks: &mut [Bytes]) -> Result<usize, ConnectionFailure> {
        let written = self
            .send_stream
            .write_chunks(chunks)
            .await
            .map_err(ConnectionFailure::WriteError)?;

        Ok(written.chunks)
    }

    pub fn write_datagram(&mut self, chunk: Bytes) -> Result<(), ConnectionFailure> {
        self.connection
            .send_datagram(chunk)
            .map_err(|err| match err {
                SendDatagramError::UnsupportedByPeer | SendDatagramError::TooLarge => {
                    ConnectionFailure::DatagramError
                }
                SendDatagramError::Disabled => unreachable!("datagrams disabled locally?"),
                SendDatagramError::ConnectionLost(err) => ConnectionFailure::ConnectionError(err),
            })
    }
}

impl Drop for ConnectionWriter {
    fn drop(&mut self) {
        self.connection.close(
            quinn_proto::VarInt::from_u32(1),
            b"outbound connection err, disconnecting",
        );
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionFailure {
    /// Dialed peer did not have expected node_id
    UnexpectedPeerId,

    ConnectionError(ConnectionError),
    WriteError(WriteError),
    ReadError(ReadError),

    /// The remote closed their outbound stream
    RemoteStreamClosed,

    DatagramError,
}

pub(crate) enum ConnectionEvent<PT: PubKey> {
    /// Contains a NodeId if it was an outbound connection failure
    ConnectionFailure(Option<NodeId<PT>>, ConnectionFailure),

    Connected(ConnectionId, NodeId<PT>, ConnectionWriter),
    InboundMessage(ConnectionId, Vec<Bytes>),
    InboundDatagram(ConnectionId, Bytes),
    Disconnected(ConnectionId, ConnectionFailure),
}

impl<PT: PubKey> Connection<PT> {
    pub fn outbound<QC: QuinnConfig<NodeIdPubKey = PT>>(
        connecting: Connecting,
        expected_peer_id: NodeId<PT>,
    ) -> Self {
        let fut = async move {
            tracing::info!("attempting to connect to={:?}", connecting.remote_address());
            let connection = connecting
                .await
                .map_err(ConnectionFailure::ConnectionError)?;
            tracing::info!(
                "connected to={:?}, connection_id={:?}, opening stream",
                connection.remote_address(),
                connection.stable_id(),
            );
            let peer_id = QC::remote_peer_id(&connection);
            if peer_id != expected_peer_id {
                return Err(ConnectionFailure::UnexpectedPeerId);
            }

            let (mut send_stream, recv_stream) = connection
                .open_bi()
                .await
                .map_err(ConnectionFailure::ConnectionError)?;

            // TODO delete this hack
            send_stream
                .write(&[0])
                .await
                .map_err(ConnectionFailure::WriteError)?;

            tracing::info!(
                "connected to={:?}, connection_id={:?}, stream ready",
                connection.remote_address(),
                connection.stable_id(),
            );
            Ok((connection, peer_id, send_stream, recv_stream))
        }
        .map_err(move |err| (Some(expected_peer_id), err))
        .boxed();
        Self::Pending(fut)
    }
    pub fn inbound<QC: QuinnConfig<NodeIdPubKey = PT>>(connecting: Connecting) -> Self {
        let fut = async move {
            tracing::info!(
                "inbound connection attempt from={:?}",
                connecting.remote_address()
            );
            let connection = connecting
                .await
                .map_err(ConnectionFailure::ConnectionError)?;
            let peer_id = QC::remote_peer_id(&connection);

            tracing::info!(
                "connected to={:?}, connection_id={:?}, waiting for stream open",
                connection.remote_address(),
                connection.stable_id(),
            );

            let (send_stream, mut recv_stream) = connection
                .accept_bi()
                .await
                .map_err(ConnectionFailure::ConnectionError)?;

            // TODO delete this hack
            let mut buf = [0_u8];
            let read = recv_stream
                .read(&mut buf)
                .await
                .map_err(ConnectionFailure::ReadError)?;
            if read != Some(1) {
                return Err(ConnectionFailure::RemoteStreamClosed);
            }

            tracing::info!(
                "connected to={:?}, connection_id={:?}, stream ready",
                connection.remote_address(),
                connection.stable_id(),
            );
            Ok((connection, peer_id, send_stream, recv_stream))
        }
        .map_err(|err| (None, err))
        .boxed();
        Self::Pending(fut)
    }

    fn active(connection: quinn::Connection, mut recv_stream: RecvStream) -> Self {
        let connection_clone = connection.clone();
        let fut = async move {
            let stream = async {
                let mut bufs = vec![Bytes::new(); 32];
                let num_chunks = recv_stream
                    .read_chunks(&mut bufs)
                    .await
                    .map_err(ConnectionFailure::ReadError)?
                    .ok_or(ConnectionFailure::RemoteStreamClosed)?;
                bufs.truncate(num_chunks);
                Ok(Inbound::Stream(bufs))
            }
            .boxed();
            let datagram = async move {
                let datagram = connection
                    .read_datagram()
                    .await
                    .map_err(ConnectionFailure::ConnectionError)?;
                Ok(Inbound::Datagram(datagram))
            }
            .boxed();
            let (bufs, _, _) = futures::future::select_all(vec![stream, datagram]).await;
            Ok((recv_stream, bufs?))
        };
        Self::Active(connection_clone, fut.boxed())
    }
}

impl<PT: PubKey> Stream for Connection<PT>
where
    Self: Unpin,
{
    type Item = ConnectionEvent<PT>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        match this {
            Connection::Pending(pending) => match ready!(pending.poll_unpin(cx)) {
                Ok((connection, node_id, send_stream, recv_stream)) => {
                    let connection_id = ConnectionId(connection.stable_id() as u64);
                    *this = Connection::active(connection.clone(), recv_stream);
                    Poll::Ready(Some(ConnectionEvent::Connected(
                        connection_id,
                        node_id,
                        ConnectionWriter::new(connection, send_stream),
                    )))
                }
                Err((maybe_expected_peer, err)) => {
                    *this = Connection::Closed;
                    Poll::Ready(Some(ConnectionEvent::ConnectionFailure(
                        maybe_expected_peer,
                        err,
                    )))
                }
            },
            Connection::Active(connection, active) => {
                let connection_id = ConnectionId(connection.stable_id() as u64);
                match ready!(active.poll_unpin(cx)) {
                    Ok((recv_stream, Inbound::Stream(chunks))) => {
                        assert!(!chunks.is_empty());
                        *this = Connection::active(connection.clone(), recv_stream);
                        Poll::Ready(Some(ConnectionEvent::InboundMessage(connection_id, chunks)))
                    }
                    Ok((recv_stream, Inbound::Datagram(chunk))) => {
                        *this = Connection::active(connection.clone(), recv_stream);
                        Poll::Ready(Some(ConnectionEvent::InboundDatagram(connection_id, chunk)))
                    }
                    Err(err) => {
                        connection.close(
                            quinn_proto::VarInt::from_u32(1),
                            b"inbound connection err, disconnecting",
                        );
                        *this = Connection::Closed;
                        Poll::Ready(Some(ConnectionEvent::Disconnected(connection_id, err)))
                    }
                }
            }
            Connection::Closed => Poll::Ready(None),
        }
    }
}
