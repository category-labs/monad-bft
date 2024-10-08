use std::{
    collections::VecDeque,
    ops::DerefMut,
    pin::pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{Stream, StreamExt};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};

pub struct DelayTimedEvent<E> {
    deliver_time: Duration,
    event: E,
}

pub struct DelayDriver<E>
where
    E: Executor + Stream,
{
    executor: E,
    commands: UnboundedReceiver<E::Command>,
    delayed_events: VecDeque<DelayTimedEvent<E::Item>>,
    event_delay: Duration,
    last_metrics_update: Duration,
}

impl<E> DelayDriver<E>
where
    E: Executor + Stream,
{
    pub fn new(executor: E, delay: Duration, command_rx: UnboundedReceiver<E::Command>) -> Self {
        Self {
            executor,
            commands: command_rx,
            delayed_events: VecDeque::new(),
            event_delay: delay,
            last_metrics_update: Duration::ZERO,
        }
    }
}

pub enum DelayDriverItem<O> {
    MetricsUpdate(Vec<(&'static str, u64)>),
    Item(O),
}

impl<E> Stream for DelayDriver<E>
where
    E: Executor + Stream + Unpin,

    Self: Unpin,
{
    type Item = DelayDriverItem<E::Item>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let now = UNIX_EPOCH.elapsed().unwrap();
        loop {
            let mut commands = Vec::new();
            let _count = self
                .deref_mut()
                .commands
                .poll_recv_many(cx, &mut commands, 10);
            if !commands.is_empty() {
                self.executor.exec(commands);
                continue;
            }

            if let Some(DelayTimedEvent {
                deliver_time,
                event: _,
            }) = self.delayed_events.front()
            {
                if &now > deliver_time {
                    let DelayTimedEvent {
                        deliver_time: _,
                        event,
                    } = self.delayed_events.pop_front().unwrap();
                    return Poll::Ready(Some(DelayDriverItem::Item(event)));
                }
            }

            if let Poll::Ready(Some(event)) = self.executor.poll_next_unpin(cx) {
                let deliver_time = now + self.event_delay;
                self.delayed_events.push_back(DelayTimedEvent {
                    deliver_time,
                    event,
                });
                continue;
            }

            if let Some(diff) = now.checked_sub(self.last_metrics_update) {
                if diff > Duration::from_secs(1) {
                    self.last_metrics_update = now;
                    let metrics = self.executor.metrics();
                    return Poll::Ready(Some(DelayDriverItem::MetricsUpdate(metrics.into_inner())));
                }
            }
        }
    }
}
