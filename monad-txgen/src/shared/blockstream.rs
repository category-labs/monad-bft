// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    future::Future,
    pin::Pin,
    task::{self, Poll},
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use alloy_json_rpc::RpcError;
use alloy_rpc_types::{Block as RpcBlock, Header, Transaction};
use alloy_transport::TransportErrorKind;
use futures::{stream::FusedStream, FutureExt, Stream};
use thiserror::Error;

use crate::prelude::*;

type Block = RpcBlock<Transaction<TxEnvelope>, Header>;

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
    query_full_txs: bool,
}

impl BlockStream {
    pub async fn new(
        client: ReqwestClient,
        interval: Duration,
        query_full_txs: bool,
    ) -> Result<Self> {
        let next_block_number: U64 = client
            .request("eth_blockNumber", ())
            .await
            .context("Failed to fetch initial block number")?;
        let next_block_number: u64 = next_block_number.to();

        Ok(Self {
            client,
            next_block_number,
            interval: tokio::time::interval(interval),
            retries: 0,
            request: None,
            query_full_txs,
        })
    }

    fn request_next_block(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<Result<Block, BlockStreamError>>> + Send>> {
        Self::request_next_block_impl(
            self.client.clone(),
            self.next_block_number,
            self.query_full_txs,
        )
        .boxed()
    }

    async fn request_next_block_impl(
        client: ReqwestClient,
        next_block_number: u64,
        query_full_txs: bool,
    ) -> Option<Result<Block, BlockStreamError>> {
        match client
            .request::<_, Option<Block>>(
                "eth_getBlockByNumber",
                (format!("0x{next_block_number:x}"), query_full_txs),
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
        cx: &mut task::Context<'_>,
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
                    let block_number: u64 = block.header.number;
                    debug!("received block {block_number}");

                    self.next_block_number = block_number + 1;

                    return Poll::Ready(Some(Ok(block)));
                }
                Err(error) => {
                    self.retries += 1;

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
