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
    task::{Context, Poll},
};

use futures::FutureExt;
use tokio::task::JoinHandle;

pub struct EthTxPoolBridgeHandle {
    handle: JoinHandle<()>,
}

impl EthTxPoolBridgeHandle {
    pub fn new(handle: JoinHandle<()>) -> Self {
        Self { handle }
    }
}

impl Future for EthTxPoolBridgeHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Poll::Ready(result) = self.handle.poll_unpin(cx) else {
            return Poll::Pending;
        };

        Poll::Ready(result.unwrap())
    }
}
