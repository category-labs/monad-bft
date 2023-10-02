use std::{
    cmp::{Ordering, Reverse},
    collections::{
        btree_map::OccupiedEntry, hash_map::Entry, BTreeMap, BTreeSet, BinaryHeap, HashMap,
        HashSet, VecDeque,
    },
    hash::Hash,
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};
use monad_consensus_types::{
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionList},
    signature_collection::SignatureCollection,
};
use monad_executor::{Executor, State};
use monad_executor_glue::{
    Command, Identifiable, MempoolCommand, Message, MonadEvent, PeerId, RouterCommand,
    RouterTarget, TimerCommand,
};
use monad_types::{Deserializable, Serializable, TimeoutVariant};
use monad_updaters::{
    checkpoint::MockCheckpoint, epoch::MockEpoch, ledger::MockLedger,
    state_root_hash::MockStateRootHash,
};

#[derive(Debug)]
pub enum RouterEvent<M, Serialized> {
    Rx(PeerId, M),
    Tx(PeerId, Serialized),
}

pub trait RouterScheduler {
    type Config;
    type M;
    type Serialized;

    fn new(config: Self::Config) -> Self;

    fn inbound(&mut self, time: Duration, from: PeerId, message: Self::Serialized);
    fn outbound<OM: Into<Self::M>>(&mut self, time: Duration, to: RouterTarget, message: OM);

    fn peek_tick(&self) -> Option<Duration>;
    fn step_until(&mut self, until: Duration) -> Option<RouterEvent<Self::M, Self::Serialized>>;
}

pub struct NoSerRouterScheduler<M> {
    all_peers: BTreeSet<PeerId>,
    events: VecDeque<(Duration, RouterEvent<M, M>)>,
}

#[derive(Clone)]
pub struct NoSerRouterConfig {
    pub all_peers: BTreeSet<PeerId>,
}

impl<M> RouterScheduler for NoSerRouterScheduler<M>
where
    M: Clone,
{
    type Config = NoSerRouterConfig;
    type M = M;
    type Serialized = M;

    fn new(config: NoSerRouterConfig) -> Self {
        Self {
            all_peers: config.all_peers,
            events: Default::default(),
        }
    }

    fn inbound(&mut self, time: Duration, from: PeerId, message: Self::Serialized) {
        assert!(
            time >= self
                .events
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(Duration::ZERO)
        );
        self.events
            .push_back((time, RouterEvent::Rx(from, message)))
    }

    fn outbound<OM: Into<Self::M>>(&mut self, time: Duration, to: RouterTarget, message: OM) {
        assert!(
            time >= self
                .events
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(Duration::ZERO)
        );
        match to {
            RouterTarget::Broadcast => {
                let message: Self::M = message.into();
                self.events.extend(
                    self.all_peers
                        .iter()
                        .map(|to| (time, RouterEvent::Tx(*to, message.clone()))),
                );
            }
            RouterTarget::PointToPoint(to) => {
                self.events
                    .push_back((time, RouterEvent::Tx(to, message.into())));
            }
        }
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.events.front().map(|(tick, _)| *tick)
    }

    fn step_until(&mut self, until: Duration) -> Option<RouterEvent<Self::M, Self::Serialized>> {
        if self.peek_tick().unwrap_or(Duration::MAX) <= until {
            let (_, event) = self.events.pop_front().expect("must exist");
            Some(event)
        } else {
            None
        }
    }
}

pub trait MockableExecutor:
    Executor<Command = MempoolCommand<Self::Event>> + Stream<Item = Self::Event> + Unpin + Default
{
    type Event;

    fn ready(&self) -> bool;
}

pub struct MockExecutor<S, RS, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    ME: MockableExecutor,
{
    mempool: ME,
    ledger: MockLedger<S::Block, S::Event>,
    checkpoint: MockCheckpoint<S::Checkpoint>,
    epoch: MockEpoch<ST, SCT>,
    state_root_hash: MockStateRootHash<S::Block, S::Event>,
    tick: Duration,

    timer: HashMap<TimeoutVariant, (Duration, S::Event)>,
    router: RS,
}

#[derive(PartialEq, Eq)]
pub struct TimerEvent {
    pub tick: Duration,
    pub event: TimeoutVariant,
    // When the event was scheduled - only used for observability
    pub scheduled_tick: Duration,
}

impl Ord for TimerEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.scheduled_tick).cmp(&other.scheduled_tick)
    }
}
impl PartialOrd for TimerEvent
where
    Self: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct SequencedPeerEvent<T> {
    pub tick: Duration,
    pub from: PeerId,
    pub t: T,

    // When the event was sent - only used for observability
    pub tx_tick: Duration,
}

impl<T> PartialEq for SequencedPeerEvent<T> {
    fn eq(&self, other: &Self) -> bool {
        self.tick == other.tick
    }
}
impl<T> Eq for SequencedPeerEvent<T> {}
impl<T> PartialOrd for SequencedPeerEvent<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // reverse ordering - because we want smaller events to be higher priority!
        Some(other.tick.cmp(&self.tick))
    }
}

impl<T> Ord for SequencedPeerEvent<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse ordering - because we want smaller events to be higher priority!
        other.tick.cmp(&self.tick)
    }
}

#[derive(PartialEq, Eq)]
enum ExecutorEventType {
    Router,
    Ledger,
    Epoch,
    Timer(TimeoutVariant),
    Mempool,
    StateRootHash,
}

impl<S, RS, ME, ST, SCT> MockExecutor<S, RS, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    ME: MockableExecutor,
{
    pub fn new(router: RS) -> Self {
        Self {
            checkpoint: Default::default(),
            ledger: Default::default(),
            mempool: Default::default(),
            epoch: Default::default(),
            state_root_hash: Default::default(),

            tick: Duration::default(),

            timer: HashMap::new(),
            router,
        }
    }

    pub fn tick(&self) -> Duration {
        self.tick
    }
    pub fn send_message(&mut self, tick: Duration, from: PeerId, message: RS::Serialized) {
        assert!(tick >= self.tick);

        self.router.inbound(tick, from, message);
    }

    fn peek_event(&self) -> Option<(Duration, ExecutorEventType)> {
        std::iter::empty()
            .chain(
                self.mempool
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Mempool)),
            )
            .chain(
                self.router
                    .peek_tick()
                    .map(|tick| (tick, ExecutorEventType::Router)),
            )
            .chain(
                self.timer
                    .iter()
                    .min_by(|(_, (a, _)), (_, (b, _))| a.cmp(b))
                    .map(|(variant, (tick, _))| (*tick, ExecutorEventType::Timer(*variant))),
            )
            .chain(
                self.epoch
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Epoch)),
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
            .min_by(|(tick_1, _), (tick_2, _)| tick_1.cmp(tick_2))
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(duration, _)| duration)
    }
}

impl<S, RS, ME, ST, SCT> Executor for MockExecutor<S, RS, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    ME: MockableExecutor<Event = S::Event>,

    S::OutboundMessage: Serializable<RS::M>,
{
    type Command = Command<S::Message, S::OutboundMessage, S::Block, S::Checkpoint>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut to_publish = Vec::new();
        let mut to_unpublish = HashSet::new();

        let (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
        ) = Self::Command::split_commands(commands);
        for command in router_cmds {
            match command {
                RouterCommand::Publish { target, message } => {
                    to_publish.push((target, message));
                }
                RouterCommand::Unpublish { target, id } => {
                    to_unpublish.insert((target, id));
                }
            }
        }
        for command in timer_cmds {
            match command {
                TimerCommand::ScheduleReset(e) => {
                    self.timer.remove(&e);
                }
                TimerCommand::Schedule {
                    duration,
                    event,
                    on_timeout,
                } => match self.timer.entry(event) {
                    Entry::Occupied(mut entry) => {
                        let (tick, e) = entry.get_mut();
                        *tick = self.tick + duration;
                        *e = on_timeout(event);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((self.tick + duration, on_timeout(event)));
                    }
                },
            }
        }
        self.mempool.exec(mempool_cmds);
        self.ledger.exec(ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);

        for (target, message) in to_publish {
            let id = message.as_ref().id();
            if to_unpublish.contains(&(target, id)) {
                continue;
            }
            self.router.outbound(self.tick, target, message.serialize());
        }
    }
}

pub enum MockExecutorEvent<E, Ser> {
    Event(E),
    Send(PeerId, Ser),
}

impl<S, RS, ME, ST, SCT> MockExecutor<S, RS, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
    RS: RouterScheduler,
    ME: MockableExecutor<Event = S::Event>,

    S::Message: Deserializable<RS::M>,
    S::Block: Unpin,
    Self: Unpin,
{
    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<MockExecutorEvent<S::Event, RS::Serialized>> {
        while let Some((tick, event_type)) = self.peek_event() {
            if tick > until {
                break;
            }
            self.tick = tick;
            let event: Option<
                MockExecutorEvent<<S as State>::Event, <RS as RouterScheduler>::Serialized>,
            > = match event_type {
                ExecutorEventType::Router => {
                    let maybe_router_event = self.router.step_until(tick);

                    match maybe_router_event {
                        None => continue, // try next tick
                        Some(RouterEvent::Rx(from, message)) => {
                            let message =
                                <S::Message as Deserializable<RS::M>>::deserialize(&message)
                                    .expect("all messages should deserialize in mock executor");
                            Some(MockExecutorEvent::Event(message.event(from)))
                        }
                        Some(RouterEvent::Tx(to, ser)) => Some(MockExecutorEvent::Send(to, ser)),
                    }
                }
                ExecutorEventType::Epoch => {
                    return futures::executor::block_on(self.epoch.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Timer(variant) => self
                    .timer
                    .remove(&variant)
                    .map(|(_, e)| MockExecutorEvent::Event(e)),
                ExecutorEventType::Mempool => {
                    return futures::executor::block_on(self.mempool.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Ledger => {
                    return futures::executor::block_on(self.ledger.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::StateRootHash => {
                    return futures::executor::block_on(self.state_root_hash.next())
                        .map(MockExecutorEvent::Event)
                }
            };
            return event;
        }

        None
    }
}

impl<S, RS, ME, ST, SCT> MockExecutor<S, RS, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    ME: MockableExecutor,
{
    pub fn ledger(&self) -> &MockLedger<S::Block, S::Event> {
        &self.ledger
    }
}

pub struct MockTimer<E> {
    event: HashMap<TimeoutVariant, (Duration, E)>, // MockTimer isn't actually a timer
    waker: Option<Waker>,
}
impl<E> Default for MockTimer<E> {
    fn default() -> Self {
        Self {
            event: HashMap::new(),
            waker: None,
        }
    }
}
impl<E> Executor for MockTimer<E>
where
    E: Hash + PartialEq + Eq,
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
                    self.event.insert(event, (duration, on_timeout(event)));
                }
                TimerCommand::ScheduleReset(e) => {
                    self.event.remove(&e);
                }
            };
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }
}
impl<E> Stream for MockTimer<E>
where
    E: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.event.is_empty() {
            this.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let variant = this
            .event
            .iter()
            .min_by(|(_, (a, _)), (_, (b, _))| a.cmp(b))
            .map(|(variant, (_, _))| variant.clone())
            .expect("There is no entry");

        let (_, e) = this.event.remove(&variant).expect("variant must exists");
        return Poll::Ready(Some(e));
    }
}

pub struct MockMempool<E> {
    fetch_txs_state: Option<Box<dyn (FnOnce(TransactionList) -> E) + Send + Sync>>,
    fetch_full_txs_state: Option<Box<dyn (FnOnce(Option<FullTransactionList>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<E> Default for MockMempool<E> {
    fn default() -> Self {
        Self {
            fetch_txs_state: None,
            fetch_full_txs_state: None,
            waker: None,
        }
    }
}

impl<E> Executor for MockMempool<E> {
    type Command = MempoolCommand<E>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                MempoolCommand::FetchTxs(_, _, cb) => {
                    self.fetch_txs_state = Some(cb);
                    wake = true;
                }
                MempoolCommand::FetchReset => {
                    self.fetch_txs_state = None;
                    wake = self.fetch_full_txs_state.is_some();
                }
                MempoolCommand::FetchFullTxs(_, cb) => {
                    self.fetch_full_txs_state = Some(cb);
                    wake = true;
                }
                MempoolCommand::FetchFullReset => {
                    self.fetch_full_txs_state = None;
                    wake = self.fetch_txs_state.is_some();
                }
                MempoolCommand::DrainTxs(_) => {}
            }
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

impl<E> Stream for MockMempool<E>
where
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(cb) = this.fetch_txs_state.take() {
            return Poll::Ready(Some(cb(TransactionList(Vec::new()))));
        }

        if let Some(cb) = this.fetch_full_txs_state.take() {
            return Poll::Ready(Some(cb(Some(FullTransactionList(Vec::new())))));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<E> MockableExecutor for MockMempool<E> {
    type Event = E;

    fn ready(&self) -> bool {
        self.fetch_txs_state.is_some() || self.fetch_full_txs_state.is_some()
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::time::Duration;

    use futures::{FutureExt, StreamExt};
    use monad_executor::Executor;
    use monad_executor_glue::TimerCommand;
    use monad_types::{BlockId, Hash};

    use super::*;

    #[test]
    fn test_mock_timer_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
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
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
        }]);
        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
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
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
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
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
            },
            TimerCommand::Schedule {
                duration: Duration::ZERO,
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
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
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
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
                event: TimeoutVariant::Pacemaker,
                on_timeout: Box::new(|_| ()),
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
            event: TimeoutVariant::Pacemaker,
            on_timeout: Box::new(|_| ()),
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
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis(i as u64),
                event: TimeoutVariant::BlockSyncTimerExpire(*id),
                on_timeout: Box::new(|tmo_var| (tmo_var)),
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
                Some(Some(TimeoutVariant::BlockSyncTimerExpire(bid))) => {
                    assert!(bids.remove(&bid));
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(regular_tmo_observed, true);
        assert!(bids.is_empty());

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_duplicate_block_id() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

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
                mock_timer.exec(vec![TimerCommand::Schedule {
                    duration: Duration::ZERO,
                    event: TimeoutVariant::BlockSyncTimerExpire(*id),
                    on_timeout: Box::new(|tmo_var| (tmo_var)),
                }]);
            }
        }

        for _ in 0..10 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSyncTimerExpire(bid))) => {
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

        for id in bids.iter() {
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::ZERO,
                event: TimeoutVariant::BlockSyncTimerExpire(*id),
                on_timeout: Box::new(|tmo_var| (tmo_var)),
            }]);
        }

        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSyncTimerExpire(BlockId(Hash([0x01_u8; 32]))),
        )]);

        mock_timer.exec(vec![TimerCommand::ScheduleReset(
            TimeoutVariant::BlockSyncTimerExpire(BlockId(Hash([0x02_u8; 32]))),
        )]);

        for _ in 0..8 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSyncTimerExpire(bid))) => {
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
            mock_timer.exec(vec![TimerCommand::Schedule {
                duration: Duration::from_millis((i as u64) + 10),
                event: TimeoutVariant::BlockSyncTimerExpire(*id),
                on_timeout: Box::new(|tmo_var| (tmo_var)),
            }]);
        }

        for _ in 0..10 {
            match mock_timer.next().now_or_never() {
                Some(Some(TimeoutVariant::BlockSyncTimerExpire(bid))) => {
                    assert_eq!(*bids.first().expect("bids are empty"), bid);
                    bids.remove(0);
                }
                _ => panic!("not receiving timeout"),
            }
        }

        assert_eq!(bids.len(), 0);
        assert_eq!(mock_timer.next().now_or_never(), None);
    }
    #[test]
    fn test_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_double_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_reset() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchReset]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_double_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {})),
            MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {})),
            MempoolCommand::FetchReset,
        ]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![
            MempoolCommand::FetchReset,
            MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_noop_exec() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, vec![], Box::new(|_| {}))]);
        mempool.exec(Vec::new());
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }
}
