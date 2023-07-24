use std::{io::ErrorKind, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use monad_executor::{Message, PeerId};
use monad_types::{Deserializable, Serializable};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::Codec;

pub(crate) struct ReliableMessageCodec<M, OM> {
    _marker: PhantomData<fn(OM) -> M>,
}
impl<M, OM> Clone for ReliableMessageCodec<M, OM> {
    fn clone(&self) -> Self {
        Self {
            _marker: self._marker,
        }
    }
}

impl<M, OM> Default for ReliableMessageCodec<M, OM> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// FIXME this is a hack to get around only 1 type for Codec::Request
pub enum WrappedMessage<M, OM> {
    Receive(M),
    Send(OM),
}
impl<M, OM> WrappedMessage<M, OM>
where
    M: Message,
    OM: Into<M> + AsRef<M>,
{
    pub fn id(&self) -> M::Id {
        match self {
            WrappedMessage::Receive(m) => m.id(),
            WrappedMessage::Send(om) => om.as_ref().id(),
        }
    }

    pub fn event(self, peer_id: PeerId) -> M::Event {
        match self {
            WrappedMessage::Receive(m) => m.event(peer_id),
            WrappedMessage::Send(om) => om.into().event(peer_id),
        }
    }
}

impl<M, OM> Serializable for WrappedMessage<M, OM>
where
    OM: Serializable,
{
    fn serialize(&self) -> Vec<u8> {
        match self {
            WrappedMessage::Send(om) => om.serialize(),
            WrappedMessage::Receive(_) => unreachable!("only Send is serializable"),
        }
    }
}
impl<M, OM> Deserializable for WrappedMessage<M, OM>
where
    M: Deserializable,
{
    type ReadError = M::ReadError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(WrappedMessage::Receive(M::deserialize(message)?))
    }
}

type LengthEncoding = u32;

#[async_trait]
impl<M, OM> Codec for ReliableMessageCodec<M, OM>
where
    M: Deserializable + Send + Sync,
    <M as Deserializable>::ReadError: 'static,
    OM: Serializable + Send + Sync,
{
    type Protocol = String;
    // TODO can we avoid Arc?
    type Request = Arc<WrappedMessage<M, OM>>;
    type Response = ();

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut size = [0; std::mem::size_of::<LengthEncoding>()];
        io.read_exact(&mut size).await?;

        let mut bytes = Vec::new();
        bytes.resize(LengthEncoding::from_le_bytes(size) as usize, 0);
        io.read_exact(&mut bytes).await?;
        let message = WrappedMessage::deserialize(&bytes)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;
        Ok(Arc::new(message))
    }

    async fn read_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // TODO read something, record ack
        Ok(())
    }

    async fn write_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes = req.serialize();
        io.write_all((bytes.len() as LengthEncoding).to_le_bytes().as_slice())
            .await?;
        io.write_all(&bytes).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // TODO write something
        Ok(())
    }
}
