use std::{
    ops::DerefMut,
    pin::Pin,
    task::{ready, Poll},
};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, Stream, TryFutureExt};
use monad_types::NodeId;
use quinn::{Connecting, ReadError, RecvStream, SendStream, WriteError};
use quinn_proto::ConnectionError;

use crate::QuinnConfig;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ConnectionId(u64);

pub(crate) enum Connection {
    Pending(
        BoxFuture<
            'static,
            Result<
                (quinn::Connection, NodeId, SendStream, RecvStream),
                (Option<NodeId>, ConnectionFailure),
            >,
        >,
    ),
    Active(
        quinn::Connection,
        BoxFuture<'static, Result<(RecvStream, Vec<Bytes>), ConnectionFailure>>,
    ),
    Closed,
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
}

pub(crate) enum ConnectionEvent {
    /// Contains a NodeId if it was an outbound connection failure
    ConnectionFailure(Option<NodeId>, ConnectionFailure),

    Connected(ConnectionId, NodeId, ConnectionWriter),
    InboundMessage(ConnectionId, Vec<Bytes>),
    Disconnected(ConnectionId, ConnectionFailure),
}

impl Connection {
    pub fn outbound<QC: QuinnConfig>(connecting: Connecting, expected_peer_id: NodeId) -> Self {
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

            let (send_stream, recv_stream) = connection
                .open_bi()
                .await
                .map_err(ConnectionFailure::ConnectionError)?;
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
    pub fn inbound<QC: QuinnConfig>(connecting: Connecting) -> Self {
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

            let (send_stream, recv_stream) = connection
                .accept_bi()
                .await
                .map_err(ConnectionFailure::ConnectionError)?;

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
        let fut = async move {
            let mut bufs = vec![Bytes::new(); 32];
            let num_chunks = recv_stream
                .read_chunks(&mut bufs)
                .await
                .map_err(ConnectionFailure::ReadError)?
                .ok_or(ConnectionFailure::RemoteStreamClosed)?;
            bufs.truncate(num_chunks);
            Ok((recv_stream, bufs))
        };
        Self::Active(connection, fut.boxed())
    }
}

impl Stream for Connection
where
    Self: Unpin,
{
    type Item = ConnectionEvent;

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
                    Ok((recv_stream, chunks)) => {
                        assert!(!chunks.is_empty());
                        *this = Connection::active(connection.clone(), recv_stream);
                        Poll::Ready(Some(ConnectionEvent::InboundMessage(connection_id, chunks)))
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
