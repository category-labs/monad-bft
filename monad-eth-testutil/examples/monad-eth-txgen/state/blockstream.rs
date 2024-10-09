use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use alloy_json_rpc::RpcError;
use alloy_rpc_client::ReqwestClient;
use alloy_transport::TransportErrorKind;
use futures::{stream::FusedStream, FutureExt, Stream};
use reth_rpc_types::Block;
use thiserror::Error;
use tokio::time::Interval;

#[derive(Debug, Error)]
pub enum BlockStreamError {
    #[error(transparent)]
    RpcError(#[from] RpcError<TransportErrorKind>),
}

pub struct BlockStream {
    client: ReqwestClient,
    next_block_number: u64,
    interval: Interval,
    retries: u64,
    request: Option<Pin<Box<dyn Future<Output = Option<Result<Block, BlockStreamError>>> + Send>>>,
}

impl BlockStream {
    pub async fn new(client: ReqwestClient, interval: Duration) -> Self {
        let next_block_number_str: String = client
            .request("eth_blockNumber", ())
            .await
            .expect("block number never fails");

        let next_block_number = u64::from_str_radix(
            next_block_number_str
                .strip_prefix("0x")
                .expect("block number string always prefixed by 0x"),
            16,
        )
        .expect("block number string is always valid number");

        Self {
            client,
            next_block_number,
            interval: tokio::time::interval(interval),
            retries: 0,
            request: None,
        }
    }

    fn request_next_block(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<Result<Block, BlockStreamError>>> + Send>> {
        Self::request_next_block_impl(self.client.clone(), self.next_block_number).boxed()
    }

    async fn request_next_block_impl(
        client: ReqwestClient,
        next_block_number: u64,
    ) -> Option<Result<Block, BlockStreamError>> {
        match client
            .request::<_, Option<Block>>(
                "eth_getBlockByNumber",
                (format!("0x{next_block_number:x}"), true),
            )
            .await
        {
            Ok(block) => block.map(Result::Ok),
            Err(error) => Some(Err(error.into())),
        }
    }

    pub fn get_current_block_number(&self) -> Option<u64> {
        self.next_block_number.checked_sub(1)
    }
}

impl Stream for BlockStream {
    type Item = Result<Block, BlockStreamError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let Some(request) = self.request.as_mut() else {
                let Poll::Ready(_) = self.interval.poll_tick(cx) else {
                    return Poll::Pending;
                };

                self.request = Some(self.request_next_block());

                continue;
            };

            let Poll::Ready(result) = request.poll_unpin(cx) else {
                return Poll::Pending;
            };

            self.request = None;

            let Some(result) = result else {
                continue;
            };

            match result {
                Ok(block) => {
                    let [block_number, 0, 0, 0] = block
                        .header
                        .number
                        .expect("block header has number")
                        .into_limbs()
                    else {
                        panic!("block number not u64???");
                    };

                    println!("received block {block_number}");

                    self.next_block_number = block_number
                        .checked_add(1)
                        .expect("block number does not overflow");

                    return Poll::Ready(Some(Ok(block)));
                }
                Err(error) => {
                    self.retries = self
                        .retries
                        .checked_add(1)
                        .expect("retries does not overflow");

                    if self.retries >= 5 {
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
    }
}

impl FusedStream for BlockStream {
    fn is_terminated(&self) -> bool {
        false
    }
}
