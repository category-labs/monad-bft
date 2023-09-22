use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet, VecDeque},
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};
use monad_consensus_types::payload::{FullTransactionList, TransactionList};
use monad_types::{Deserializable, Serializable};

use super::{
    checkpoint::MockCheckpoint, epoch::MockEpoch, ledger::MockLedger,
    state_root_hash::MockStateRootHash,
};
use crate::{
    state::PeerId, Command, Executor, Identifiable, MempoolCommand, Message, RouterCommand,
    RouterTarget, State, TimerCommand,
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

pub struct MockExecutor<S, RS, ME>
where
    S: State,
    ME: MockableExecutor,
{
    mempool: ME,
    ledger: MockLedger<S::Block, S::Event>,
    checkpoint: MockCheckpoint<S::Checkpoint>,
    epoch: MockEpoch<S::Event>,
    state_root_hash: MockStateRootHash<S::Block, S::Event>,
    tick: Duration,

    timer: Option<TimerEvent<S::Event>>,

    router: RS,
}

pub struct TimerEvent<E> {
    pub tick: Duration,
    pub event: E,

    // When the event was scheduled - only used for observability
    pub scheduled_tick: Duration,
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

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ExecutorEventType {
    Router,
    Ledger,
    Epoch,
    Timer,
    Mempool,
    StateRootHash,
}

impl<S, RS, ME> MockExecutor<S, RS, ME>
where
    S: State,
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

            timer: None,

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
                    .as_ref()
                    .map(|TimerEvent { tick, .. }| (*tick, ExecutorEventType::Timer)),
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
            .min()
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(duration, _)| duration)
    }
}

impl<S, RS, ME> Executor for MockExecutor<S, RS, ME>
where
    S: State,
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
                TimerCommand::ScheduleReset => self.timer = None,
                TimerCommand::Schedule {
                    duration,
                    on_timeout,
                } => {
                    self.timer = Some(TimerEvent {
                        event: on_timeout,
                        tick: self.tick + duration,

                        scheduled_tick: self.tick,
                    })
                }
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

impl<S, RS, ME> MockExecutor<S, RS, ME>
where
    S: State,
    RS: RouterScheduler,
    ME: MockableExecutor<Event = S::Event>,

    S::Event: Unpin,
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
            let event = match event_type {
                ExecutorEventType::Router => {
                    let maybe_router_event = self.router.step_until(tick);

                    match maybe_router_event {
                        None => continue, // try next tick
                        Some(RouterEvent::Rx(from, message)) => {
                            let message =
                                <S::Message as Deserializable<RS::M>>::deserialize(&message)
                                    .expect("all messages should deserialize in mock executor");
                            MockExecutorEvent::Event(message.event(from))
                        }
                        Some(RouterEvent::Tx(to, ser)) => MockExecutorEvent::Send(to, ser),
                    }
                }
                ExecutorEventType::Epoch => {
                    return futures::executor::block_on(self.epoch.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Timer => {
                    MockExecutorEvent::Event(self.timer.take().unwrap().event)
                }
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
            return Some(event);
        }

        None
    }
}

impl<S, RS, ME> MockExecutor<S, RS, ME>
where
    S: State,
    ME: MockableExecutor,
{
    pub fn ledger(&self) -> &MockLedger<S::Block, S::Event> {
        &self.ledger
    }
}

pub struct MockTimer<E> {
    event: Option<E>,
    waker: Option<Waker>,
}
impl<E> Default for MockTimer<E> {
    fn default() -> Self {
        Self {
            event: None,
            waker: None,
        }
    }
}
impl<E> Executor for MockTimer<E> {
    type Command = TimerCommand<E>;
    fn exec(&mut self, commands: Vec<TimerCommand<E>>) {
        let mut wake = false;
        for command in commands {
            self.event = match command {
                TimerCommand::Schedule {
                    duration: _,
                    on_timeout,
                } => {
                    wake = true;
                    Some(on_timeout)
                }
                TimerCommand::ScheduleReset => {
                    wake = false;
                    None
                }
            }
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
        if let Some(event) = this.event.take() {
            return Poll::Ready(Some(event));
        }

        this.waker = Some(cx.waker().clone());
        Poll::Pending
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
                MempoolCommand::FetchTxs(max_num_txs, cb) => {
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
    use std::{collections::HashSet, fmt::Debug, time::Duration};

    use futures::{FutureExt, StreamExt};
    use monad_crypto::secp256k1::KeyPair;
    use monad_testutil::{
        block::MockBlock,
        signing::{create_keys, node_id},
    };
    use monad_types::{Deserializable, Serializable};
    use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

    use super::*;
    use crate::{
        mock_swarm::Nodes,
        state::{Command, Executor, PeerId, RouterCommand, State, TimerCommand},
        transformer::{GenericTransformer, LatencyTransformer},
        Message,
    };

    #[test]
    fn test_mock_timer_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
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
            on_timeout: (),
        }]);
        mock_timer.exec(vec![TimerCommand::Schedule {
            duration: Duration::ZERO,
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
            on_timeout: (),
        }]);
        mock_timer.exec(vec![TimerCommand::ScheduleReset]);

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_inline_double_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![
            TimerCommand::Schedule {
                duration: Duration::ZERO,
                on_timeout: (),
            },
            TimerCommand::Schedule {
                duration: Duration::ZERO,
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
                on_timeout: (),
            },
            TimerCommand::ScheduleReset,
        ]);

        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    #[test]
    fn test_mock_timer_inline_reset_schedule() {
        let mut mock_timer = MockTimer::default();
        assert_eq!(mock_timer.next().now_or_never(), None);

        mock_timer.exec(vec![
            TimerCommand::ScheduleReset,
            TimerCommand::Schedule {
                duration: Duration::ZERO,
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
            on_timeout: (),
        }]);
        mock_timer.exec(Vec::new());

        assert_eq!(mock_timer.next().now_or_never(), Some(Some(())));
        assert_eq!(mock_timer.next().now_or_never(), None);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    /// EXAMPLE SWARM TEST
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    const NUM_NODES: u64 = 5;

    #[derive(Debug, PartialEq, Eq)]
    struct SimpleChainState {
        me: PeerId,
        peers: Vec<PeerId>,

        chain: Vec<HashSet<PeerId>>,

        // outbound votes for given round (self.chain.len() - 1)
        outbound_votes: Vec<HashSet<PeerId>>,
    }
    #[derive(Debug, Clone)]
    enum SimpleChainEvent {
        Vote { peer: PeerId, round: u64 },
    }

    impl Serializable<Vec<u8>> for SimpleChainEvent {
        fn serialize(&self) -> Vec<u8> {
            unreachable!("not used")
        }
    }

    impl Deserializable<[u8]> for SimpleChainEvent {
        type ReadError = ReadError;
        fn deserialize(_buf: &[u8]) -> Result<Self, Self::ReadError> {
            unreachable!("not used")
        }
    }

    #[derive(Debug)]
    struct ReadError {}

    impl std::fmt::Display for ReadError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            <Self as Debug>::fmt(self, f)
        }
    }

    impl std::error::Error for ReadError {}

    impl State for SimpleChainState {
        type Config = (Vec<PeerId>, PeerId);
        type Event = SimpleChainEvent;
        type OutboundMessage = SimpleChainMessage;
        type Message = SimpleChainMessage;
        type Block = MockBlock;
        type Checkpoint = ();

        fn init(
            config: Self::Config,
        ) -> (
            Self,
            Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::Checkpoint>>,
        ) {
            let (pubkeys, _me) = config;

            let init_cmds = pubkeys
                .iter()
                .map(|peer| {
                    Command::RouterCommand(RouterCommand::Publish {
                        target: RouterTarget::PointToPoint(*peer),
                        message: SimpleChainMessage { round: 0 },
                    })
                })
                .collect();

            let init_self = Self {
                me: PeerId(node_id().0),
                peers: pubkeys.clone(),

                chain: vec![HashSet::new()],
                outbound_votes: vec![pubkeys.into_iter().collect()],
            };

            (init_self, init_cmds)
        }
        fn update(
            &mut self,
            event: Self::Event,
        ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::Checkpoint>>
        {
            let mut commands = Vec::new();
            match event {
                SimpleChainEvent::Vote { peer, round } => {
                    self.chain[round as usize].insert(peer);

                    if self.chain.last().unwrap().len() > self.peers.len() / 2
                        && self.chain.len() < self.peers.len()
                    {
                        // max NUM_NODES blocks
                        self.chain.push(HashSet::new());

                        commands.extend(self.peers.iter().map(|peer| {
                            Command::RouterCommand(RouterCommand::Publish {
                                target: RouterTarget::PointToPoint(*peer),
                                message: SimpleChainMessage {
                                    round: self.chain.len() as u64 - 1,
                                },
                            })
                        }));
                        self.outbound_votes
                            .push(self.peers.iter().cloned().collect());
                    }
                }
            };
            commands
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    struct SimpleChainMessage {
        round: u64,
    }
    impl AsRef<Self> for SimpleChainMessage {
        fn as_ref(&self) -> &Self {
            self
        }
    }

    impl Identifiable for SimpleChainMessage {
        type Id = u64;

        fn id(&self) -> Self::Id {
            self.round
        }
    }

    impl Message for SimpleChainMessage {
        type Event = SimpleChainEvent;

        fn event(self, from: PeerId) -> Self::Event {
            Self::Event::Vote {
                round: self.round,
                peer: from,
            }
        }
    }

    #[test]
    fn test_nodes() {
        use crate::timed_event::TimedEvent;
        let pubkeys = create_keys(NUM_NODES as u32)
            .iter()
            .map(KeyPair::pubkey)
            .map(PeerId)
            .collect::<Vec<_>>();
        let state_configs = (0..NUM_NODES)
            .map(|idx| (pubkeys.clone(), pubkeys[idx as usize]))
            .collect::<Vec<_>>();
        let peers = pubkeys
            .iter()
            .copied()
            .map(|peer_id| peer_id.0)
            .zip(state_configs)
            .map(|(a, b)| {
                (
                    a,
                    b,
                    MockWALoggerConfig {},
                    NoSerRouterConfig {
                        all_peers: pubkeys.iter().copied().collect(),
                    },
                    vec![GenericTransformer::Latency(LatencyTransformer(
                        Duration::from_millis(50),
                    ))],
                )
            })
            .collect();
        let mut nodes = Nodes::<
            SimpleChainState,
            NoSerRouterScheduler<SimpleChainMessage>,
            _,
            MockWALogger<TimedEvent<SimpleChainEvent>>,
            MockMempool<SimpleChainEvent>,
        >::new(peers);

        while let Some((duration, id, event)) = nodes.step() {
            println!("{duration:?} => {id:?} => {event:?}")
        }
    }

    #[test]
    fn test_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_double_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchTxs(0, Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_reset() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchReset]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_double_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(0, Box::new(|_| {})),
            MempoolCommand::FetchTxs(0, Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(0, Box::new(|_| {})),
            MempoolCommand::FetchReset,
        ]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset_fetch() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![
            MempoolCommand::FetchReset,
            MempoolCommand::FetchTxs(0, Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_noop_exec() {
        let mut mempool = MockMempool::<()>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(0, Box::new(|_| {}))]);
        mempool.exec(Vec::new());
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }
}
