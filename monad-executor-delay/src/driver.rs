use std::{
    collections::BinaryHeap,
    ops::DerefMut,
    pin::Pin,
    task::Poll,
    time::{Duration, Instant, UNIX_EPOCH},
};

use futures::{FutureExt, Stream, StreamExt};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};

const METRICS_UPDATE_PERIOD: Duration = Duration::from_secs(1);

enum Trigger<E> {
    UpdateMetrics,
    EmitEvent(E),
}

pub struct ScheduledEvent<E> {
    deliver: Duration,
    trigger: Trigger<E>,
}

impl<E> PartialOrd for ScheduledEvent<E> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.deliver.cmp(&self.deliver))
    }
}

impl<E> Ord for ScheduledEvent<E> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deliver.cmp(&self.deliver)
    }
}

impl<E> PartialEq for ScheduledEvent<E> {
    fn eq(&self, other: &Self) -> bool {
        self.deliver.eq(&other.deliver)
    }
}

impl<E> Eq for ScheduledEvent<E> {}

pub struct DelayDriver<E>
where
    E: Executor + Stream,
{
    executor: E,
    commands: UnboundedReceiver<E::Command>,
    events: BinaryHeap<ScheduledEvent<E::Item>>,
    event_delay: Duration,
    timeout: Pin<Box<tokio::time::Sleep>>,
    now: Duration,
}

impl<E> DelayDriver<E>
where
    E: Executor + Stream,
{
    pub fn new(executor: E, delay: Duration, command_rx: UnboundedReceiver<E::Command>) -> Self {
        let mut events = BinaryHeap::new();
        events.push(ScheduledEvent {
            deliver: Duration::ZERO,
            trigger: Trigger::UpdateMetrics,
        });
        Self {
            executor,
            commands: command_rx,
            events,
            event_delay: delay,
            timeout: Box::pin(tokio::time::sleep(Duration::ZERO)),
            now: Duration::ZERO,
        }
    }

    fn now(&mut self) -> Duration {
        self.now = self.now.max(UNIX_EPOCH.elapsed().unwrap());
        self.now
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.events.peek().unwrap().deliver)
        } else {
            None
        }
    }

    fn poll(&mut self, now: Duration) -> Option<Trigger<E::Item>> {
        if let Some(event) = self.events.peek() {
            if event.deliver <= now {
                return self.events.pop().map(|event| event.trigger);
            }
        }
        None
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
        let now = self.now();
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

            if let Some(timeout) = self.peek_tick() {
                let duration_until_timeout = timeout.checked_sub(now).unwrap_or(Duration::ZERO);
                let current_instant = Instant::now();

                let deadline = current_instant + duration_until_timeout;
                let old_deadline = self.timeout.deadline().into_std();
                if deadline < old_deadline - Duration::from_millis(1)
                    || deadline > old_deadline - Duration::from_millis(1)
                {
                    tokio::time::Sleep::reset(self.timeout.as_mut(), deadline.into());
                }

                if self.timeout.poll_unpin(cx).is_ready()
                    || duration_until_timeout == Duration::ZERO
                {
                    if let Some(trigger) = self.poll(now) {
                        match trigger {
                            Trigger::UpdateMetrics => {
                                self.events.push(ScheduledEvent {
                                    deliver: now + METRICS_UPDATE_PERIOD,
                                    trigger: Trigger::UpdateMetrics,
                                });
                                let metrics = self.executor.metrics();
                                return Poll::Ready(Some(DelayDriverItem::MetricsUpdate(
                                    metrics.into_inner(),
                                )));
                            }
                            Trigger::EmitEvent(event) => {
                                return Poll::Ready(Some(DelayDriverItem::Item(event)));
                            }
                        }
                    }
                }
            }

            if let Poll::Ready(Some(event)) = self.executor.poll_next_unpin(cx) {
                let deliver_time = now + self.event_delay;
                self.events.push(ScheduledEvent {
                    deliver: deliver_time,
                    trigger: Trigger::EmitEvent(event),
                });
                continue;
            }

            return Poll::Pending;
        }
    }
}
