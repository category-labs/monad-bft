use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Future, FutureExt, Stream};
use monad_executor::Executor;
use monad_executor_glue::TimerCommand;
use monad_types::TimeoutVariant;
use tokio::{
    task::{AbortHandle, JoinSet},
    time::{sleep, Sleep},
};

struct TMOEventWrapper<E> {
    sleep: Pin<Box<Sleep>>,
    event: Option<E>,
}

impl<E> TMOEventWrapper<E> {
    pub fn new(d: Duration, event: E) -> Self {
        Self {
            sleep: Box::pin(sleep(d)),
            event: Some(event),
        }
    }
}

impl<E> Future for TMOEventWrapper<E>
where
    Self: Unpin,
{
    type Output = Option<E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<E>> {
        let this: &mut TMOEventWrapper<E> = self.deref_mut();
        if this.sleep.poll_unpin(cx).is_ready() {
            return Poll::Ready(this.event.take());
        }
        Poll::Pending
    }
}

pub struct TokioTimer<E> {
    timers: JoinSet<Option<E>>,
    aborts: HashMap<TimeoutVariant, AbortHandle>,
    waker: Option<Waker>,
}
impl<E> Default for TokioTimer<E>
where
    E: 'static,
{
    fn default() -> Self {
        Self {
            timers: JoinSet::new(),
            aborts: HashMap::new(),
            waker: None,
        }
    }
}
impl<E> Executor for TokioTimer<E>
where
    E: Send + Unpin + 'static,
{
    type Command = TimerCommand<E>;
    fn exec(&mut self, commands: Vec<TimerCommand<E>>) {
        let mut wake = false;
        for command in commands {
            match command {
                TimerCommand::Schedule {
                    duration,
                    event,
                    on_timeout,
                } => {
                    wake = true;
                    let handle = self
                        .timers
                        .spawn(TMOEventWrapper::<E>::new(duration, on_timeout(event)));
                    match self.aborts.entry(event) {
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
                TimerCommand::ScheduleReset(e) => {
                    wake = false;
                    if let Some(abort_handle) = self.aborts.remove(&e) {
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
}
impl<E> Stream for TokioTimer<E>
where
    E: 'static + Debug,
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use futures::StreamExt;
    use monad_types::{BlockId, Hash};
    use ntest::timeout;

    use super::*;

    #[tokio::test]
    #[timeout(3000)]
    async fn test_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
        }]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_double_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
        }]);
        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
        }]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_reset() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(0),
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
        }]);
        timer.exec(vec![TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker)]);

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_inline_double_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
            },
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
            },
        ]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_inline_reset() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
            },
            TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
        ]);

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_inline_reset_schedule() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![
            TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
            TimerCommand::Schedule {
                duration: Duration::from_millis(0),
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
            },
        ]);

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_noop_exec() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(1),
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
        }]);
        timer.exec(Vec::new());

        assert_eq!(timer.next().await, Some(()));
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_multi_variant() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::from_millis(1),
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| TimeoutVariant::Pacemaker),
        }]);

        let mut bids = HashSet::from([
            BlockId(Hash([0x00_u8; 32])),
            BlockId(Hash([0x01_u8; 32])),
            BlockId(Hash([0x02_u8; 32])),
            BlockId(Hash([0x03_u8; 32])),
            BlockId(Hash([0x04_u8; 32])),
            BlockId(Hash([0x05_u8; 32])),
            BlockId(Hash([0x06_u8; 32])),
            BlockId(Hash([0x07_u8; 32])),
            BlockId(Hash([0x08_u8; 32])),
            BlockId(Hash([0x09_u8; 32])),
        ]);

        for (i, id) in bids.iter().enumerate() {
            timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i + 100) as u64),
                event: TimeoutVariant::BlockSyncTimerExpire(*id),
                on_timeout: Box::new(|tmo_var| (tmo_var)),
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
                Some(TimeoutVariant::BlockSyncTimerExpire(bid)) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(regular_tmo_observed, true);
        assert!(bids.is_empty());

        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(timer.timers.is_empty());
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_duplicate_block_id() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        let mut bids = HashSet::from([
            BlockId(Hash([0x00_u8; 32])),
            BlockId(Hash([0x01_u8; 32])),
            BlockId(Hash([0x02_u8; 32])),
            BlockId(Hash([0x03_u8; 32])),
            BlockId(Hash([0x04_u8; 32])),
            BlockId(Hash([0x05_u8; 32])),
            BlockId(Hash([0x06_u8; 32])),
            BlockId(Hash([0x07_u8; 32])),
            BlockId(Hash([0x08_u8; 32])),
            BlockId(Hash([0x09_u8; 32])),
        ]);

        for _ in 0..3 {
            for id in bids.iter() {
                timer.exec(vec![TimerCommand::Schedule {
                    duration: Duration::ZERO,
                    event: TimeoutVariant::BlockSyncTimerExpire(*id),
                    on_timeout: Box::new(|tmo_var| (tmo_var)),
                }]);
            }
        }

        for _ in 0..10 {
            match timer.next().await {
                Some(TimeoutVariant::BlockSyncTimerExpire(bid)) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(bids.is_empty());
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(timer.timers.is_empty());
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_reset_block_id() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        // fetch reset submitted earlier should have no impact.
        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSyncTimerExpire(BlockId(Hash([0x00_u8; 32]))),
        )]);

        let mut bids = HashSet::from([
            BlockId(Hash([0x00_u8; 32])),
            BlockId(Hash([0x01_u8; 32])),
            BlockId(Hash([0x02_u8; 32])),
            BlockId(Hash([0x03_u8; 32])),
            BlockId(Hash([0x04_u8; 32])),
            BlockId(Hash([0x05_u8; 32])),
            BlockId(Hash([0x06_u8; 32])),
            BlockId(Hash([0x07_u8; 32])),
            BlockId(Hash([0x08_u8; 32])),
            BlockId(Hash([0x09_u8; 32])),
        ]);

        for (i, id) in bids.iter().enumerate() {
            timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i + 100) as u64),
                event: TimeoutVariant::BlockSyncTimerExpire(*id),
                on_timeout: Box::new(|tmo_var| (tmo_var)),
            }]);
        }
        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSyncTimerExpire(BlockId(Hash([0x01_u8; 32]))),
        )]);

        timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSyncTimerExpire(BlockId(Hash([0x02_u8; 32]))),
        )]);

        for _ in 0..8 {
            match timer.next().await {
                Some(TimeoutVariant::BlockSyncTimerExpire(bid)) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(bids.len(), 2);
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(bids.contains(&BlockId(Hash([0x01_u8; 32]))));
        assert!(bids.contains(&BlockId(Hash([0x02_u8; 32]))));
        assert!(timer.timers.is_empty());
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_retrieval_in_order() {
        let mut timer = TokioTimer::default();
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);

        let mut bids = vec![
            BlockId(Hash([0x00_u8; 32])),
            BlockId(Hash([0x01_u8; 32])),
            BlockId(Hash([0x02_u8; 32])),
            BlockId(Hash([0x03_u8; 32])),
            BlockId(Hash([0x04_u8; 32])),
            BlockId(Hash([0x05_u8; 32])),
            BlockId(Hash([0x06_u8; 32])),
            BlockId(Hash([0x07_u8; 32])),
            BlockId(Hash([0x08_u8; 32])),
            BlockId(Hash([0x09_u8; 32])),
        ];

        for (i, id) in bids.iter().enumerate() {
            timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i as u64) + 3),
                event: TimeoutVariant::BlockSyncTimerExpire(*id),
                on_timeout: Box::new(|tmo_var| (tmo_var)),
            }]);
        }

        for _ in 0..10 {
            match timer.next().await {
                Some(TimeoutVariant::BlockSyncTimerExpire(bid)) => {
                    assert_eq!(*bids.first().expect("bids are empty"), bid);
                    bids.remove(0);
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(bids.len(), 0);
        assert_eq!(futures::poll!(timer.next()), Poll::Pending);
        assert!(timer.timers.is_empty());
    }
}
