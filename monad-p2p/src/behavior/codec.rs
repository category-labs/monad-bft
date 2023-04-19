use std::{io::ErrorKind, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use monad_executor::{Deserializable, Message, Serializable};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::Codec;

#[derive(Clone)]
pub(crate) struct ReliableMessageCodec<M>
where
    M: Message + Serializable,
{
    _marker: PhantomData<M>,
}

impl<M> Default for ReliableMessageCodec<M>
where
    M: Message + Serializable,
{
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<M> Codec for ReliableMessageCodec<M>
where
    M: Message + Serializable + Deserializable,
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
