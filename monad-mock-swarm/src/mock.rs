use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet, VecDeque},
    marker::{PhantomData, Unpin},
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};
use monad_consensus_types::{
    command::{FetchFullTxParams, FetchTxParams},
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionList},
    signature_collection::SignatureCollection,
};
use monad_executor::{Executor, State};
use monad_executor_glue::{
    Command, Identifiable, MempoolCommand, Message, MonadEvent, PeerId, RouterCommand,
    RouterTarget, TimerCommand,
};
use monad_types::{Deserializable, Serializable};
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
    Executor<Command = MempoolCommand<Self::SignatureCollection>>
    + Stream<Item = Self::Event>
    + Unpin
    + Default
{
    type Event;
    type SignatureCollection;

    fn ready(&self) -> bool;
}

pub struct MockExecutor<S, RS, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    ME: MockableExecutor<SignatureCollection = SCT>,
{
    mempool: ME,
    ledger: MockLedger<S::Block, S::Event>,
    checkpoint: MockCheckpoint<S::Checkpoint>,
    epoch: MockEpoch<ST, SCT>,
    state_root_hash: MockStateRootHash<S::Block, ST, SCT>,
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

impl<S, RS, ME, ST, SCT> MockExecutor<S, RS, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    ME: MockableExecutor<SignatureCollection = SCT>,
{
    pub fn new(router: RS, tick: Duration) -> Self {
        Self {
            checkpoint: Default::default(),
            ledger: Default::default(),
            mempool: Default::default(),
            epoch: Default::default(),
            state_root_hash: Default::default(),

            tick,

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

impl<S, RS, ME, ST, SCT> Executor for MockExecutor<S, RS, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    ME: MockableExecutor<SignatureCollection = SCT, Event = S::Event>,

    S::OutboundMessage: Serializable<RS::M>,
{
    type Command = Command<S::Message, S::OutboundMessage, S::Block, S::Checkpoint, SCT>;
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

impl<S, RS, ME, ST, SCT> MockExecutor<S, RS, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
    RS: RouterScheduler,
    ME: MockableExecutor<SignatureCollection = S::SignatureCollection, Event = S::Event>,

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

impl<S, RS, ME, ST, SCT> MockExecutor<S, RS, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    ME: MockableExecutor<SignatureCollection = SCT>,
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

pub struct MockMempool<ST, SCT> {
    fetch_txs_state: Option<FetchTxParams<SCT>>,
    fetch_full_txs_state: Option<FetchFullTxParams<SCT>>,
    waker: Option<Waker>,
    phantom: PhantomData<ST>,
}

impl<ST, SCT> Default for MockMempool<ST, SCT> {
    fn default() -> Self {
        Self {
            fetch_txs_state: None,
            fetch_full_txs_state: None,
            waker: None,
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT> Executor for MockMempool<ST, SCT> {
    type Command = MempoolCommand<SCT>;

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

impl<ST, SCT> Stream for MockMempool<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(s) = this.fetch_txs_state.take() {
            return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::FetchedTxs(s, TransactionList(Vec::new())),
            )));
        }

        if let Some(s) = this.fetch_full_txs_state.take() {
            return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::FetchedFullTxs(
                    s,
                    Some(FullTransactionList(Vec::new())),
                ),
            )));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<ST, SCT> MockableExecutor for MockMempool<ST, SCT>
where
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        self.fetch_txs_state.is_some() || self.fetch_full_txs_state.is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{FutureExt, StreamExt};
    use monad_consensus_types::{
        multi_sig::MultiSig, quorum_certificate::QuorumCertificate, validation::Sha256Hash,
    };
    use monad_crypto::NopSignature;
    use monad_executor::Executor;
    use monad_executor_glue::TimerCommand;
    use monad_testutil::signing::node_id;
    use monad_types::Round;

    use super::*;

    fn dont_care_fetched_tx_param<S: SignatureCollection>() -> FetchTxParams<S> {
        FetchTxParams {
            node_id: node_id(),
            round: Round(0),
            seq_num: 0,
            state_root_hash: Default::default(),
            high_qc: QuorumCertificate::genesis_prime_qc::<Sha256Hash>(),
            last_round_tc: None,
        }
    }

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

    #[test]
    fn test_fetch() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(
            0,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_double_fetch() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(
            0,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        mempool.exec(vec![MempoolCommand::FetchTxs(
            0,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_reset() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(
            0,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        mempool.exec(vec![MempoolCommand::FetchReset]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_double_fetch() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(0, vec![], dont_care_fetched_tx_param()),
            MempoolCommand::FetchTxs(0, vec![], dont_care_fetched_tx_param()),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(0, vec![], dont_care_fetched_tx_param()),
            MempoolCommand::FetchReset,
        ]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset_fetch() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![
            MempoolCommand::FetchReset,
            MempoolCommand::FetchTxs(0, vec![], dont_care_fetched_tx_param()),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_noop_exec() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(
            0,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        mempool.exec(Vec::new());
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }
}
