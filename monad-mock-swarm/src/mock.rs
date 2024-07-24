use std::{
    cmp::Reverse,
    hash::{Hash, Hasher},
    marker::{PhantomData, Unpin},
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_consensus_state::command::Checkpoint;
use monad_consensus_types::{block::Block, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{
    Command, ExecutionLedgerCommand, Message, MonadEvent, RouterCommand, TimerCommand,
};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_state::VerifiedMonadMessage;
use monad_types::{NodeId, TimeoutVariant};
use monad_updaters::{
    checkpoint::MockCheckpoint, ipc::MockIpcReceiver, ledger::MockableLedger,
    loopback::LoopbackExecutor, state_root_hash::MockableStateRootHash,
};
use priority_queue::PriorityQueue;

use crate::swarm_relation::SwarmRelation;

pub struct MockExecutor<S: SwarmRelation> {
    ledger: S::Ledger,
    execution_ledger: MockExecutionLedger<S::SignatureCollectionType>,
    checkpoint: MockCheckpoint<Checkpoint<S::SignatureCollectionType>>,
    state_root_hash: S::StateRootHashExecutor,
    loopback: LoopbackExecutor<MonadEvent<S::SignatureType, S::SignatureCollectionType>>,
    ipc: MockIpcReceiver<S::SignatureType, S::SignatureCollectionType>,
    tick: Duration,

    timer: PriorityQueue<
        TimerEvent<MonadEvent<S::SignatureType, S::SignatureCollectionType>>,
        Reverse<Duration>,
    >,
    router: S::RouterScheduler,
}

pub struct TimerEvent<E> {
    pub variant: TimeoutVariant,
    pub callback: Option<E>,
}

impl<E> TimerEvent<E> {
    pub fn new(variant: TimeoutVariant) -> Self {
        Self {
            variant,
            // you don't need callback to perform indexing
            callback: None,
        }
    }

    pub fn with_call_back(mut self, callback: E) -> Self {
        self.callback = Some(callback);
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

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ExecutorEventType {
    Router,
    Ledger,
    Timer,
    StateRootHash,
    Ipc,
    Loopback,
}

impl<S: SwarmRelation> MockExecutor<S> {
    pub fn new(
        router: S::RouterScheduler,
        state_root_hash: S::StateRootHashExecutor,
        ledger: S::Ledger,
        tick: Duration,
    ) -> Self {
        Self {
            checkpoint: Default::default(),
            ledger,
            execution_ledger: Default::default(),
            state_root_hash,
            ipc: Default::default(),
            loopback: Default::default(),

            tick,

            timer: PriorityQueue::new(),
            router,
        }
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

    pub fn send_transaction(&mut self, txn: Bytes) {
        self.ipc.add_transaction(txn);
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
                self.ledger
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Ledger)),
            )
            .chain(
                self.state_root_hash
                    .ready()
                    .then_some((self.tick, ExecutorEventType::StateRootHash)),
            )
            .chain(
                self.loopback
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Loopback)),
            )
            .chain(
                self.ipc
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Ipc)),
            )
            .min()
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(duration, _)| duration)
    }
}

impl<S: SwarmRelation> Executor for MockExecutor<S> {
    type Command = Command<
        MonadEvent<S::SignatureType, S::SignatureCollectionType>,
        VerifiedMonadMessage<S::SignatureType, S::SignatureCollectionType>,
        Block<S::SignatureCollectionType>,
        Checkpoint<S::SignatureCollectionType>,
        S::SignatureCollectionType,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
            loopback_cmds,
            control_panel_cmds,
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
                    self.timer.push(
                        TimerEvent::new(variant).with_call_back(on_timeout),
                        Reverse(self.tick + duration),
                    );
                }
            }
        }

        self.ledger.exec(ledger_cmds);
        self.execution_ledger.exec(execution_ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);
        self.loopback.exec(loopback_cmds);

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
            MonadEvent<S::SignatureType, S::SignatureCollectionType>,
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
                ExecutorEventType::StateRootHash => {
                    return futures::executor::block_on(self.state_root_hash.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Loopback => {
                    return futures::executor::block_on(self.loopback.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Ipc => {
                    return futures::executor::block_on(self.ipc.next())
                        .map(MockExecutorEvent::Event)
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

    pub fn state_root_hash_executor(&self) -> &S::StateRootHashExecutor {
        &self.state_root_hash
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

pub struct MockExecutionLedger<SCT> {
    phantom: PhantomData<SCT>,
}

impl<SCT: SignatureCollection> Executor for MockExecutionLedger<SCT> {
    type Command = ExecutionLedgerCommand<SCT>;

    fn exec(&mut self, _: Vec<Self::Command>) {}
    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}

impl<O> Default for MockExecutionLedger<O> {
    fn default() -> Self {
        Self {
            phantom: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{collections::HashSet, time::Duration};

    use futures::{FutureExt, StreamExt};
    use monad_crypto::hasher::Hash;
    use monad_executor::Executor;
    use monad_executor_glue::TimerCommand;
    use monad_types::{BlockId, TimeoutVariant};

    use super::*;

    fn get_bids() -> [BlockId; 10] {
        [
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

        let mut bids = HashSet::from(get_bids());

        for (i, id) in bids.iter().enumerate() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis(i as u64),
                variant: TimeoutVariant::BlockSync(*id),
                on_timeout: TimeoutVariant::BlockSync(*id),
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
                Some(Some(TimeoutVariant::BlockSync(bid))) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(regular_tmo_observed);
        assert!(bids.is_empty());

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_duplicate_block_id() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        let mut bids = HashSet::from(get_bids());

        for _ in 0..3 {
            for id in bids.iter() {
                mock_timer.exec(vec![TimerCommand::Schedule {
                    duration: Duration::ZERO,
                    variant: TimeoutVariant::BlockSync(*id),
                    on_timeout: TimeoutVariant::BlockSync(*id),
                }]);
            }
        }

        for _ in 0..10 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSync(bid))) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert!(bids.is_empty());
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_reset_block_id() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        // fetch reset submitted earlier should have no impact.
        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockId(Hash([0x00_u8; 32]))),
        )]);

        let mut bids = HashSet::from(get_bids());

        for id in bids.iter() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::ZERO,
                variant: TimeoutVariant::BlockSync(*id),
                on_timeout: TimeoutVariant::BlockSync(*id),
            }]);
        }

        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockId(Hash([0x01_u8; 32]))),
        )]);

        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSync(BlockId(Hash([0x02_u8; 32]))),
        )]);

        for _ in 0..8 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSync(bid))) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(bids.len(), 2);
        assert_eq!(mock_timer.next().now_or_never(), None);
        assert!(bids.contains(&BlockId(Hash([0x01_u8; 32]))));
        assert!(bids.contains(&BlockId(Hash([0x02_u8; 32]))));
    }

    #[test]
    fn test_mock_timer_retrieval_in_order() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        let bids = get_bids();

        for (i, id) in bids.iter().enumerate() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i as u64) + 10),
                variant: TimeoutVariant::BlockSync(*id),
                on_timeout: TimeoutVariant::BlockSync(*id),
            }]);
        }

        for bid in bids {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSync(id))) => {
                    assert_eq!(bid, id);
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(mock_timer.next().now_or_never(), None);
    }
}
