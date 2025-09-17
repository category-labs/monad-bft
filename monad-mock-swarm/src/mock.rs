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
    cmp::Reverse,
    collections::VecDeque,
    hash::{Hash, Hasher},
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{
    Command, Message, MonadEvent, RouterCommand, TimeoutVariant, TimerCommand, TimestampCommand,
};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_state::VerifiedMonadMessage;
use monad_types::NodeId;
use monad_updaters::{
    config_file::MockConfigFile, ledger::MockableLedger, loopback::LoopbackExecutor,
    statesync::MockableStateSync, timestamp::TimestampAdjuster, txpool::MockableTxPool,
    val_set::MockableValSetUpdater,
};
use priority_queue::PriorityQueue;

use crate::swarm_relation::SwarmRelation;

pub struct MockExecutor<S: SwarmRelation> {
    ledger: S::Ledger,
    config_file:
        MockConfigFile<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
    val_set: S::ValSetUpdater,
    loopback: LoopbackExecutor<
        MonadEvent<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
    >,
    txpool: S::TxPoolExecutor,
    statesync: S::StateSyncExecutor,

    tick: Duration,

    timer: PriorityQueue<
        TimerEvent<
            MonadEvent<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
        >,
        Reverse<Duration>,
    >,

    timestamper: Timestamper,

    router: S::RouterScheduler,
}

#[derive(Debug)]
pub struct TimerEvent<E> {
    pub variant: TimeoutVariant,
    pub callback: Option<E>,
    pub was_scheduled_at: Duration,
}

impl<E> TimerEvent<E> {
    pub fn new(variant: TimeoutVariant) -> Self {
        Self {
            variant,
            // you don't need callback to perform indexing
            callback: None,
            // you don't need scheduled to perform indexing
            was_scheduled_at: Duration::ZERO,
        }
    }

    pub fn with_call_back(mut self, callback: E) -> Self {
        self.callback = Some(callback);
        self
    }

    pub fn was_scheduled_at(mut self, at: Duration) -> Self {
        self.was_scheduled_at = at;
        self
    }
}

impl<E> Hash for TimerEvent<E> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.variant.hash(state);
    }
}

impl<E> PartialEq for TimerEvent<E> {
    fn eq(&self, other: &Self) -> bool {
        self.variant == other.variant
    }
}

impl<E> Eq for TimerEvent<E> {}

pub struct Timestamper {
    events: VecDeque<Duration>,
    period: Duration,
    timestamp_drift: Duration,
    drift_adjustment: Duration,

    adjuster: TimestampAdjuster,
}

impl Timestamper {
    pub fn new(start_time: Duration, config: TimestamperConfig) -> Self {
        Self {
            events: VecDeque::from([start_time]),
            period: config.period,
            timestamp_drift: config.timestamp_drift,
            drift_adjustment: Duration::from_millis(0),
            adjuster: TimestampAdjuster::new(config.max_adjust_delta_ns, config.adjust_period),
        }
    }

    pub fn next_tick(&mut self) -> Duration {
        let t = self.events.pop_front().unwrap();
        self.events.push_back(t + self.period);

        self.drift_adjustment += self.timestamp_drift;
        self.adjusted_time(t)
    }

    pub fn peek_next(&self) -> Option<&Duration> {
        self.events.front()
    }

    fn adjusted_time(&self, t: Duration) -> Duration {
        let adjust = self.adjuster.get_adjustment();
        let delta = Duration::from_millis(adjust.unsigned_abs());
        if adjust.is_negative() {
            (t + self.drift_adjustment).saturating_sub(delta)
        } else {
            t + self.drift_adjustment + delta
        }
    }
}

pub struct TimestamperConfig {
    pub period: Duration,
    pub timestamp_drift: Duration,

    pub max_adjust_delta_ns: u128,
    pub adjust_period: usize,
}

impl Default for TimestamperConfig {
    fn default() -> Self {
        Self {
            period: Duration::from_millis(10),
            timestamp_drift: Duration::from_millis(0),

            max_adjust_delta_ns: 10_000_000_000,
            adjust_period: 9,
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ExecutorEventType {
    Router,
    Ledger,
    Timer,
    ValSet,
    TxPool,
    Loopback,
    Timestamp,
    StateSync,
}

pub struct RoundTimer {
    pub was_scheduled_at: Duration,
    pub times_out_at: Duration,
}

impl<S: SwarmRelation> MockExecutor<S> {
    pub fn new(
        router: S::RouterScheduler,
        val_set: S::ValSetUpdater,
        txpool: S::TxPoolExecutor,
        ledger: S::Ledger,
        statesync: S::StateSyncExecutor,
        timestamp_config: TimestamperConfig,
        tick: Duration,
    ) -> Self {
        Self {
            config_file: Default::default(),
            ledger,
            val_set,
            txpool,
            loopback: Default::default(),
            statesync,

            tick,

            timer: PriorityQueue::new(),
            timestamper: Timestamper::new(tick, timestamp_config),
            router,
        }
    }

    pub fn checkpoint(
        &self,
    ) -> Option<Checkpoint<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>>
    {
        self.config_file.checkpoint.clone()
    }

    pub fn next_round_timeout(&self) -> Option<RoundTimer> {
        self.timer
            .iter()
            .filter(|(event, _)| matches!(event.variant, TimeoutVariant::Pacemaker))
            .map(|(event, &Reverse(times_out_at))| RoundTimer {
                was_scheduled_at: event.was_scheduled_at,
                times_out_at,
            })
            .next()
    }

    pub fn tick(&self) -> Duration {
        self.tick
    }

    pub fn send_message(
        &mut self,
        tick: Duration,
        from: NodeId<CertificateSignaturePubKey<S::SignatureType>>,
        message: <S::RouterScheduler as RouterScheduler>::TransportMessage,
    ) {
        assert!(tick >= self.tick);

        self.router.process_inbound(tick, from, message);
    }

    pub fn send_transaction(&mut self, tx: Bytes) {
        self.txpool.send_transaction(tx);
    }

    fn peek_event(&self) -> Option<(Duration, ExecutorEventType)> {
        std::iter::empty()
            .chain(
                self.router
                    .peek_tick()
                    .map(|tick| (tick, ExecutorEventType::Router)),
            )
            .chain(
                self.timer
                    .peek()
                    .map(|(_, tick)| (tick.0, ExecutorEventType::Timer)),
            )
            .chain(
                self.timestamper
                    .peek_next()
                    .map(|tick| (*tick, ExecutorEventType::Timestamp)),
            )
            .chain(
                self.ledger
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Ledger)),
            )
            .chain(
                self.txpool
                    .ready()
                    .then_some((self.tick, ExecutorEventType::TxPool)),
            )
            .chain(
                self.val_set
                    .ready()
                    .then_some((self.tick, ExecutorEventType::ValSet)),
            )
            .chain(
                self.loopback
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Loopback)),
            )
            .chain(
                self.statesync
                    .ready()
                    .then_some((self.tick, ExecutorEventType::StateSync)),
            )
            .min()
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(duration, _)| duration)
    }
}

impl<S: SwarmRelation> Executor for MockExecutor<S> {
    type Command = Command<
        MonadEvent<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
        VerifiedMonadMessage<
            S::SignatureType,
            S::SignatureCollectionType,
            S::ExecutionProtocolType,
        >,
        S::SignatureType,
        S::SignatureCollectionType,
        S::ExecutionProtocolType,
        S::BlockPolicyType,
        S::StateBackendType,
        S::ChainConfigType,
        S::ChainRevisionType,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            config_file_cmds,
            val_set_cmds,
            timestamp_cmds,
            txpool_cmds,
            _control_panel_cmds,
            loopback_cmds,
            statesync_cmds,
            _config_reload_cmds,
        ) = Self::Command::split_commands(commands);

        for command in timer_cmds {
            match command {
                TimerCommand::ScheduleReset(variant) => {
                    self.timer.remove(&TimerEvent::new(variant));
                }
                TimerCommand::Schedule {
                    duration,
                    variant,
                    on_timeout,
                } => {
                    // only one timeout variant may be armed at any time
                    // scheduling a new timer automatically resets the previous
                    // one
                    self.timer.remove(&TimerEvent::new(variant));
                    self.timer.push(
                        TimerEvent::new(variant)
                            .with_call_back(on_timeout)
                            .was_scheduled_at(self.tick),
                        Reverse(self.tick + duration),
                    );
                }
            }
        }

        for command in timestamp_cmds {
            match command {
                TimestampCommand::AdjustDelta(t) => self.timestamper.adjuster.handle_adjustment(t),
            }
        }

        self.ledger.exec(ledger_cmds);
        self.txpool.exec(txpool_cmds);
        self.config_file.exec(config_file_cmds);
        self.val_set.exec(val_set_cmds);
        self.loopback.exec(loopback_cmds);
        self.statesync.exec(statesync_cmds);

        for command in router_cmds {
            match command {
                RouterCommand::Publish { target, message } => {
                    self.router.send_outbound(self.tick, target, message);
                }
                RouterCommand::AddEpochValidatorSet { .. } => {
                    // TODO
                }
                RouterCommand::UpdateCurrentRound(_, _) => {
                    // TODO
                }
                RouterCommand::GetPeers => {
                    // TODO
                }
                RouterCommand::UpdatePeers { .. } => {
                    // TODO
                }
                RouterCommand::GetFullNodes => {
                    // TODO
                }
                RouterCommand::UpdateFullNodes { .. } => {
                    // TODO
                }
                RouterCommand::PublishToFullNodes { .. } => {
                    // TODO
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        // TODO do we want to see executor metrics in mock?
        Default::default()
    }
}

pub enum MockExecutorEvent<E, PT: PubKey, TransportMessage> {
    Event(E),
    Send(NodeId<PT>, TransportMessage),
}

impl<S: SwarmRelation> MockExecutor<S> {
    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<
        MockExecutorEvent<
            MonadEvent<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
            CertificateSignaturePubKey<S::SignatureType>,
            S::TransportMessage,
        >,
    > {
        while let Some((tick, event_type)) = self.peek_event() {
            if tick > until {
                break;
            }

            self.tick = tick;

            let event = match event_type {
                ExecutorEventType::Router => {
                    let maybe_router_event = self.router.step_until(tick);

                    match maybe_router_event {
                        None => continue, // try next tick
                        Some(RouterEvent::Rx(from, message)) => {
                            MockExecutorEvent::Event(message.event(from))
                        }
                        Some(RouterEvent::Tx(to, ser)) => MockExecutorEvent::Send(to, ser),
                    }
                }
                ExecutorEventType::Timer => {
                    MockExecutorEvent::Event(self.timer.pop().unwrap().0.callback.unwrap())
                }
                ExecutorEventType::Ledger => {
                    return futures::executor::block_on(self.ledger.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::ValSet => {
                    return futures::executor::block_on(self.val_set.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Loopback => {
                    return futures::executor::block_on(self.loopback.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::TxPool => {
                    return futures::executor::block_on(self.txpool.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Timestamp => {
                    let event = self.timestamper.next_tick();
                    MockExecutorEvent::Event(MonadEvent::TimestampUpdateEvent(event.as_nanos()))
                }
                ExecutorEventType::StateSync => {
                    return self.statesync.pop().map(MockExecutorEvent::Event)
                }
            };

            return Some(event);
        }

        None
    }
}

impl<S: SwarmRelation> MockExecutor<S> {
    pub fn ledger(&self) -> &S::Ledger {
        &self.ledger
    }

    pub fn val_set_updater(&self) -> &S::ValSetUpdater {
        &self.val_set
    }
}

pub struct MockTimer<E> {
    // MockTimer isn't actually a timer, it just return the lowest tick item when polled.
    event: PriorityQueue<TimerEvent<E>, Reverse<Duration>>,
    waker: Option<Waker>,
}
impl<E> Default for MockTimer<E> {
    fn default() -> Self {
        Self {
            event: PriorityQueue::new(),
            waker: None,
        }
    }
}
impl<E> Executor for MockTimer<E>
where
    E: PartialEq + Eq,
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
                    self.event.push(
                        TimerEvent::new(variant).with_call_back(on_timeout),
                        Reverse(duration),
                    );
                }
                TimerCommand::ScheduleReset(variant) => {
                    self.event.remove(&TimerEvent::new(variant));
                }
            };
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }
    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}
impl<E> Stream for MockTimer<E>
where
    E: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some((e, _)) = this.event.pop() {
            Poll::Ready(e.callback)
        } else {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{collections::HashSet, time::Duration};

    use futures::{FutureExt, StreamExt};
    use monad_blocksync::messages::message::BlockSyncRequestMessage;
    use monad_consensus_types::{block::BlockRange, payload::ConsensusBlockBodyId};
    use monad_crypto::hasher::Hash;
    use monad_executor::Executor;
    use monad_executor_glue::TimerCommand;
    use monad_types::{BlockId, SeqNum};

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

    #[test]
    fn test_mock_timer_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);

        assert_eq!(mock_timer.next().now_or_never(), Some(Some(())));
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_double_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);
        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);

        assert_eq!(mock_timer.next().now_or_never(), Some(Some(())));
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_reset() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);
        mock_timer.exec(vec![TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker)]);

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_inline_double_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::ZERO,
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
            TimerCommand::Schedule {
                duration: Duration::ZERO,
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
        ]);

        assert_eq!(mock_timer.next().now_or_never(), Some(Some(())));
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_inline_reset() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::ZERO,
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
            TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
        ]);

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_inline_reset_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![
            TimerCommand::ScheduleReset(TimeoutVariant::Pacemaker),
            TimerCommand::Schedule {
                duration: Duration::ZERO,
                variant: TimeoutVariant::Pacemaker,
                on_timeout: (),
            },
        ]);

        assert_eq!(mock_timer.next().now_or_never(), Some(Some(())));
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_noop_exec() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            variant: TimeoutVariant::Pacemaker,
            on_timeout: (),
        }]);
        mock_timer.exec(Vec::new());

        assert_eq!(mock_timer.next().now_or_never(), Some(Some(())));
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_multi_variant() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            variant: TimeoutVariant::Pacemaker,
            on_timeout: TimeoutVariant::Pacemaker,
        }]);

        let mut requests = HashSet::from(get_blocksync_requests());

        for (i, req) in requests.iter().enumerate() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis(i as u64),
                variant: TimeoutVariant::BlockSync(*req),
                on_timeout: TimeoutVariant::BlockSync(*req),
            }]);
        }

        let mut regular_tmo_observed = false;
        for _ in 0..11 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::Pacemaker)) => {
                    if regular_tmo_observed {
                        panic!("regular tmo observed twice");
                    } else {
                        regular_tmo_observed = true
                    }
                }
                Some(Some(TimeoutVariant::BlockSync(req))) => {
                    assert!(requests.remove(&req));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(regular_tmo_observed);
        assert!(requests.is_empty());

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_duplicate_block_id() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        let mut requests = HashSet::from(get_blocksync_requests());

        for _ in 0..3 {
            for req in requests.iter() {
                mock_timer.exec(vec![TimerCommand::Schedule {
                    duration: Duration::ZERO,
                    variant: TimeoutVariant::BlockSync(*req),
                    on_timeout: TimeoutVariant::BlockSync(*req),
                }]);
            }
        }

        for _ in 0..10 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSync(req))) => {
                    assert!(requests.remove(&req));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(requests.is_empty());
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_reset_block_id() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        // fetch reset submitted earlier should have no impact.
        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x00_u8; 32])),
                num_blocks: SeqNum(1),
            })),
        )]);

        let mut requests = HashSet::from(get_blocksync_requests());

        for req in requests.iter() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::ZERO,
                variant: TimeoutVariant::BlockSync(*req),
                on_timeout: TimeoutVariant::BlockSync(*req),
            }]);
        }

        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                num_blocks: SeqNum(1),
            })),
        )]);
        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x02_u8; 32])),
                num_blocks: SeqNum(1),
            })),
        )]);

        for _ in 0..8 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSync(req))) => {
                    assert!(requests.remove(&req));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(requests.len(), 2);
        assert_eq!(mock_timer.next().now_or_never(), None);
        assert!(
            requests.contains(&BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                num_blocks: SeqNum(1),
            }))
        );
        assert!(
            requests.contains(&BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: BlockId(Hash([0x02_u8; 32])),
                num_blocks: SeqNum(1),
            }))
        );
    }

    #[test]
    fn test_mock_timer_retrieval_in_order() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        let requests = get_blocksync_requests();

        for (i, req) in requests.iter().enumerate() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i as u64) + 10),
                variant: TimeoutVariant::BlockSync(*req),
                on_timeout: TimeoutVariant::BlockSync(*req),
            }]);
        }

        for req in requests {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSync(timer_req))) => {
                    assert_eq!(req, timer_req);
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(mock_timer.next().now_or_never(), None);
    }
}
