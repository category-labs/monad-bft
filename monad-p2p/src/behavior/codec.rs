use std::{io::ErrorKind, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use monad_executor::{Deserializable, Serializable};

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

#[async_trait]
impl<M, OM> Codec for ReliableMessageCodec<M, OM>
where
    M: Deserializable + Send + Sync,
    <M as Deserializable>::ReadError: 'static,
    OM: Serializable + Send + Sync,

    M: Serializable, // FIXME - do we need to fork request_response to have different
                     // OutboundRequest and InboundRequest types...?
{
    type Protocol = String;
    // TODO can we avoid Arc?
    type Request = Arc<M>;
    type Response = ();

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut bytes = Vec::new();
        io.read_to_end(&mut bytes).await?;
        let message =
            M::deserialize(&bytes).map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;
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
        Ok(())
    }
}
