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
    collections::{hash_map::Entry, HashMap},
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{TimeoutVariant, TimerCommand};
use tokio::task::{AbortHandle, JoinSet};

/// This updater allows timer events to be scheduled to fire in the future
pub struct TokioTimer<E> {
    timers: JoinSet<Option<E>>,
    aborts: HashMap<TimeoutVariant, AbortHandle>,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
}
impl<E> Default for TokioTimer<E> {
    fn default() -> Self {
        Self {
            timers: JoinSet::new(),
            aborts: HashMap::new(),
            waker: None,
            metrics: Default::default(),
        }
    }
}
impl<E> Executor for TokioTimer<E>
where
    E: Send + 'static,
{
    type Command = TimerCommand<E>;

    fn exec(&mut self, commands: Vec<TimerCommand<E>>) {
        let mut wake = false;
        for command in commands {
            match command {
                TimerCommand::Schedule {
                    duration,
                    variant,
                    on_timeout,
                } => {
                    wake = true;
                    let cb = async move {
                        tokio::time::sleep(duration).await;
                        Some(on_timeout)
                    };
                    let handle = self.timers.spawn(cb);
                    match self.aborts.entry(variant) {
                        Entry::Occupied(mut entry) => {
                            let old_handle = entry.get_mut();
                            old_handle.abort();
                            *old_handle = handle;
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(handle);
                        }
                    }
                }
                TimerCommand::ScheduleReset(variant) => {
                    wake = false;
                    if let Some(abort_handle) = self.aborts.remove(&variant) {
                        abort_handle.abort();
                    }
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.metrics.as_ref().into()
    }
}

impl<E> Stream for TokioTimer<E>
where
    E: 'static,
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut _timer_poll_span = tracing::trace_span!("timer_poll_span").entered();

        let this = self.deref_mut();

        // its possible to get Poll::Ready(None) because the join_set might be empty
        while let Poll::Ready(Some(poll_result)) = this.timers.poll_join_next(cx) {
            match poll_result {
                Ok(e) => {
                    return Poll::Ready(e);
                }
                Err(join_error) => {
                    // only case where this happen is when task is aborted
                    assert!(join_error.is_cancelled());
                }
            };
        }

        if let Some(waker) = this.waker.as_mut() {
            waker.clone_from(cx.waker());
        } else {
            this.waker = Some(cx.waker().clone());
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use futures::StreamExt;
    use monad_blocksync::messages::message::BlockSyncRequestMessage;
    use monad_consensus_types::{block::BlockRange, payload::ConsensusBlockBodyId};
    use monad_crypto::hasher::Hash;
    use monad_types::{BlockId, SeqNum};
    use ntest::timeout;

    use super::*;

    fn get_blocksync_requests() -> [BlockSyncRequestMessage; 10] {
        [
            BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x00_u8; 32])),
                num_blocks: SeqNum(1),
            }),
            BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                num_blocks: SeqNum(1),
            }),
            BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x02_u8; 32])),
                num_blocks: SeqNum(1),
            }),
            BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x03_u8; 32])),
                num_blocks: SeqNum(1),
            }),
            BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x04_u8; 32])),
                num_blocks: SeqNum(1),
            }),
            BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(Hash([0x05_u8; 32]))),
            BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(Hash([0x06_u8; 32]))),
            BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(Hash([0x07_u8; 32]))),
            BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(Hash([0x08_u8; 32]))),
            BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(Hash([0x09_u8; 32]))),
        ]
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_double_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);
        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_reset() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);

        timer.exec(vec![TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker)]);

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_inline_double_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
        ]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_inline_reset() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
            TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
        ]);

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_inline_reset_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
        ]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_noop_exec() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(1),
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);
        timer.exec(Vec::new());

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_multi_variant() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(1),
            variant: TimeoutVariant::Pacemaker,
            on_timeout: TimeoutVariant::Pacemaker,
        }]);

        let mut requests = HashSet::from(get_blocksync_requests());

        for (i, req) in requests.iter().enumerate() {
            timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i + 100) as u64),
                variant: TimeoutVariant::BlockSync(*req),
                on_timeout: TimeoutVariant::BlockSync(*req),
            }]);
        }

        let mut regular_tmo_observed = false;
        for _ in 0..11 {
            println!("found");
            match timer.next().await {
                Some(TimeoutVariant::Pacemaker) => {
                    if regular_tmo_observed {
                        panic!("regular tmo observed twice");
                    } else {
                        regular_tmo_observed = true
                    }
                }
                Some(TimeoutVariant::BlockSync(req)) => {
                    assert!(requests.remove(&req));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(regular_tmo_observed);
        assert!(requests.is_empty());

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(timer.timers.is_empty());
    }

    #[tokio::test]
    #[should_panic]
    #[timeout(200)]
    async fn test_duplicate_block_id() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        let mut requests = HashSet::from(get_blocksync_requests());

        for i in 0..3 {
            for req in requests.iter() {
                timer.exec(vec![TimerCommand::Schedule {
                    duration: Duration::from_millis(i * 10),
                    variant: TimeoutVariant::BlockSync(*req),
                    on_timeout: TimeoutVariant::BlockSync(*req),
                }]);
            }
        }

        for _ in 0..10 {
            match timer.next().await {
                Some(TimeoutVariant::BlockSync(req)) => {
                    assert!(requests.remove(&req));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(requests.is_empty());
        assert!(timer.timers.is_empty());
        // this call never returns, test would timeout
        timer.next().await;
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_reset_block_id() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        // fetch reset submitted earlier should have no impact.
        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x00_u8; 32])),
                num_blocks: SeqNum(1),
            })),
        )]);
        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(
                Hash([0x05_u8; 32]),
            ))),
        )]);

        let mut requests = HashSet::from(get_blocksync_requests());

        for (i, req) in requests.iter().enumerate() {
            timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i + 100) as u64),
                variant: TimeoutVariant::BlockSync(*req),
                on_timeout: TimeoutVariant::BlockSync(*req),
            }]);
        }
        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                num_blocks: SeqNum(1),
            })),
        )]);
        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(
                Hash([0x05_u8; 32]),
            ))),
        )]);

        for _ in 0..8 {
            match timer.next().await {
                Some(TimeoutVariant::BlockSync(bid)) => {
                    assert!(requests.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(requests.len(), 2);
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(
            requests.contains(&BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                num_blocks: SeqNum(1),
            }))
        );
        assert!(
            requests.contains(&BlockSyncRequestMessage::Payload(ConsensusBlockBodyId(
                Hash([0x05_u8; 32])
            )))
        );
        assert!(timer.timers.is_empty());
    }

    #[tokio::test]
    #[timeout(200)]
    async fn test_retrieval_in_order() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        let requests = get_blocksync_requests();

        for (i, id) in requests.iter().enumerate() {
            timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i as u64) + 3),
                variant: TimeoutVariant::BlockSync(*id),
                on_timeout: TimeoutVariant::BlockSync(*id),
            }]);
        }

        for request in requests {
            match timer.next().await {
                Some(TimeoutVariant::BlockSync(timer_req)) => {
                    assert_eq!(request, timer_req);
                }
                _ => panic!("not receiving timeout"),
            }
        }
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(timer.timers.is_empty());
    }
}
