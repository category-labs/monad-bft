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
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::Stream;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, TimestampCommand};
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;
use tokio::time::{Duration, Interval};

use crate::timestamp::TimestampAdjuster;

pub struct TokioTimestamp<ST, SCT, EPT> {
    /// create timestamp events at this interval
    interval: Interval,
    adjuster: TimestampAdjuster,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> TokioTimestamp<ST, SCT, EPT> {
    pub fn new(period: Duration, max_delta_ns: u128, adjustment_period: usize) -> Self {
        Self {
            interval: tokio::time::interval(period),
            adjuster: TimestampAdjuster::new(max_delta_ns, adjustment_period),
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Executor for TokioTimestamp<ST, SCT, EPT> {
    type Command = TimestampCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                TimestampCommand::AdjustDelta(t) => self.adjuster.handle_adjustment(t),
            }
        }
    }
    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT, EPT> Stream for TokioTimestamp<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => {
                let start = SystemTime::now();
                let epoch_time = start
                    .duration_since(UNIX_EPOCH)
                    .expect("Clock may have gone backwards");
                let t = epoch_time.as_nanos();
                // t += self.adjuster.get_adjustment();
                Poll::Ready(Some(MonadEvent::TimestampUpdateEvent(t)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
