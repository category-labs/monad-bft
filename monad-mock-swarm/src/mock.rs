use std::{
    cmp::Ordering,
    collections::{BTreeSet, VecDeque},
    marker::{PhantomData, Unpin},
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{FutureExt, Stream, StreamExt};
use monad_consensus_types::{
    command::{FetchFullTxParams, FetchTxParams},
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionList},
    signature_collection::SignatureCollection,
};
use monad_eth_types::EMPTY_RLP_TX_LIST;
use monad_executor::{Executor, State};
use monad_executor_glue::{
    Command, ExecutionLedgerCommand, MempoolCommand, Message, MonadEvent, PeerId, RouterCommand,
    RouterTarget, TimerCommand,
};
use monad_types::{Deserializable, Serializable};
use monad_updaters::{
    checkpoint::MockCheckpoint, epoch::MockEpoch, ledger::MockLedger,
    state_root_hash::MockStateRootHash,
};
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng, ChaChaRng};

use crate::swarm_relation::{SwarmRelation, SwarmStateType};

const MOCK_DEFAULT_SEED: u64 = 1;

#[derive(Debug)]
pub enum RouterEvent<M, Serialized> {
    Rx(PeerId, M),
    Tx(PeerId, Serialized),
}

/// RouterScheduler describes HOW gossip messages get delivered
pub trait RouterScheduler {
    type Config;
    /// transport-level message type - usually will be bytes
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
    Executor<Command = MempoolCommand<Self::SignatureCollection>> + Stream<Item = Self::Event> + Unpin
{
    type Event;
    type SignatureCollection;
    type Config: Copy;

    fn new(config: Self::Config) -> Self;

    fn ready(&self) -> bool;
}

pub struct MockExecutor<S>
where
    S: SwarmRelation,
{
    mempool: S::ME,
    ledger: MockLedger<<SwarmStateType<S> as State>::Block, <SwarmStateType<S> as State>::Event>,
    execution_ledger: MockExecutionLedger<S::SCT>,
    checkpoint: MockCheckpoint<<SwarmStateType<S> as State>::Checkpoint>,
    epoch: MockEpoch<S::ST, S::SCT>,
    state_root_hash: MockStateRootHash<<SwarmStateType<S> as State>::Block, S::ST, S::SCT>,
    tick: Duration,

    timer: Option<TimerEvent<<SwarmStateType<S> as State>::Event>>,

    router: S::RS,
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

impl<S> MockExecutor<S>
where
    S: SwarmRelation,
{
    pub fn new(
        router: S::RS,
        mempool_config: <S::ME as MockableExecutor>::Config,
        tick: Duration,
    ) -> Self {
        Self {
            checkpoint: Default::default(),
            ledger: Default::default(),
            execution_ledger: Default::default(),
            mempool: <S::ME as MockableExecutor>::new(mempool_config),
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
    pub fn send_message(
        &mut self,
        tick: Duration,
        from: PeerId,
        message: <S::RS as RouterScheduler>::Serialized,
    ) {
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

impl<S> Executor for MockExecutor<S>
where
    S: SwarmRelation,

    <SwarmStateType<S> as State>::OutboundMessage: Serializable<<S::RS as RouterScheduler>::M>,
{
    type Command = Command<
        <SwarmStateType<S> as State>::Event,
        <SwarmStateType<S> as State>::OutboundMessage,
        <SwarmStateType<S> as State>::Block,
        <SwarmStateType<S> as State>::Checkpoint,
        <SwarmStateType<S> as State>::SignatureCollection,
    >;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
        ) = Self::Command::split_commands(commands);

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
        self.execution_ledger.exec(execution_ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);

        for command in router_cmds {
            match command {
                RouterCommand::Publish { target, message } => {
                    self.router.outbound(self.tick, target, message.serialize());
                }
            }
        }
    }
}

pub enum MockExecutorEvent<E, Ser> {
    Event(E),
    Send(PeerId, Ser),
}

impl<S> MockExecutor<S>
where
    S: SwarmRelation,

    <SwarmStateType<S> as State>::Message: Deserializable<<S::RS as RouterScheduler>::M>,
    <SwarmStateType<S> as State>::Block: Unpin,
    Self: Unpin,
{
    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<
        MockExecutorEvent<
            <SwarmStateType<S> as State>::Event,
            <S::RS as RouterScheduler>::Serialized,
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
                            let message =
                                <<SwarmStateType<S> as State>::Message>::deserialize(&message)
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
                ExecutorEventType::Mempool => match self.mempool.next().now_or_never() {
                    Some(e) => {
                        return e.map(MockExecutorEvent::Event);
                    }
                    None => continue,
                },
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

impl<S> MockExecutor<S>
where
    S: SwarmRelation,
{
    pub fn ledger(&self) -> &MockLedger<<S::STATE as State>::Block, <S::STATE as State>::Event> {
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
    num_fetch_txs: usize,
    fetch_full_txs_state: Option<FetchFullTxParams<SCT>>,
    waker: Option<Waker>,
    rng: ChaCha20Rng,
    phantom: PhantomData<ST>,
}

impl<ST, SCT> Default for MockMempool<ST, SCT> {
    fn default() -> Self {
        Self {
            fetch_txs_state: None,
            num_fetch_txs: 0,
            fetch_full_txs_state: None,
            waker: None,
            rng: ChaCha20Rng::seed_from_u64(MOCK_DEFAULT_SEED),
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
                MempoolCommand::FetchTxs(num_txs, _, cb) => {
                    self.fetch_txs_state = Some(cb);
                    self.num_fetch_txs = num_txs;
                    wake = true;
                }
                MempoolCommand::FetchReset => {
                    self.fetch_txs_state = None;
                    self.num_fetch_txs = 0;
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

impl<ST, SCT> MockMempool<ST, SCT> {
    fn get_fetched_txs_list(&mut self) -> TransactionList {
        if self.num_fetch_txs == 0 {
            TransactionList(vec![EMPTY_RLP_TX_LIST])
        } else {
            // Random non-empty value with size = num_fetch_txs
            TransactionList(
                (0..self.num_fetch_txs)
                    .map(|_| self.rng.gen::<u8>())
                    .collect(),
            )
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
                monad_executor_glue::ConsensusEvent::FetchedTxs(s, self.get_fetched_txs_list()),
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

#[derive(Copy, Clone)]
pub struct MockMempoolConfig(pub u64);

impl Default for MockMempoolConfig {
    fn default() -> Self {
        Self(MOCK_DEFAULT_SEED)
    }
}

impl<ST, SCT> MockableExecutor for MockMempool<ST, SCT>
where
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;
    type Config = MockMempoolConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(config.0),
            ..Default::default()
        }
    }

    fn ready(&self) -> bool {
        self.fetch_txs_state.is_some() || self.fetch_full_txs_state.is_some()
    }
}

pub struct MockMempoolRandFail<ST, SCT> {
    mpool: MockMempool<ST, SCT>,
    fail_rate: f32,
    rng: ChaCha20Rng,
}

impl<ST, SCT> MockMempoolRandFail<ST, SCT> {
    pub fn new(seed: u64, fail_rate: f32) -> Self {
        MockMempoolRandFail {
            mpool: Default::default(),
            fail_rate,
            rng: ChaChaRng::seed_from_u64(seed),
        }
    }
}

impl<ST, SCT> Executor for MockMempoolRandFail<ST, SCT> {
    type Command = MempoolCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.mpool.exec(commands)
    }
}

#[derive(Copy, Clone)]
pub struct MockMempoolRandFailConfig {
    pub fail_rate: f32,
    pub seed: u64,
}

impl<ST, SCT> MockableExecutor for MockMempoolRandFail<ST, SCT>
where
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;
    type Config = MockMempoolRandFailConfig;

    fn new(config: Self::Config) -> Self {
        MockMempoolRandFail {
            mpool: Default::default(),
            fail_rate: config.fail_rate,
            rng: ChaChaRng::seed_from_u64(config.seed),
        }
    }

    fn ready(&self) -> bool {
        self.mpool.ready()
    }
}

impl<ST, SCT> Stream for MockMempoolRandFail<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(s) = this.mpool.fetch_txs_state.take() {
            if self.rng.gen_range(0.0..1.0) < self.fail_rate {
                return Poll::Pending;
            }

            return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::FetchedTxs(
                    s,
                    self.mpool.get_fetched_txs_list(),
                ),
            )));
        }

        if let Some(s) = this.mpool.fetch_full_txs_state.take() {
            if self.rng.gen_range(0.0..1.0) < self.fail_rate {
                return Poll::Pending;
            }

            return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::FetchedFullTxs(
                    s,
                    Some(FullTransactionList(Vec::new())),
                ),
            )));
        }

        self.mpool.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub struct MockExecutionLedger<SCT> {
    phantom: PhantomData<SCT>,
}

impl<SCT> Executor for MockExecutionLedger<SCT> {
    type Command = ExecutionLedgerCommand<SCT>;

    fn exec(&mut self, _: Vec<Self::Command>) {}
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
    use std::time::Duration;

    use futures::{FutureExt, StreamExt};
    use monad_consensus_types::{multi_sig::MultiSig, quorum_certificate::QuorumCertificate};
    use monad_crypto::{hasher::HasherType, NopSignature};
    use monad_executor::Executor;
    use monad_executor_glue::{ConsensusEvent, TimerCommand};
    use monad_testutil::signing::node_id;
    use monad_types::Round;

    use super::*;

    fn dont_care_fetched_tx_param<S: SignatureCollection>() -> FetchTxParams<S> {
        FetchTxParams {
            node_id: node_id(),
            round: Round(0),
            seq_num: 0,
            state_root_hash: Default::default(),
            high_qc: QuorumCertificate::genesis_prime_qc::<HasherType>(),
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
    fn test_seeded_fetch() {
        let mut mempool = MockMempool::<NopSignature, MultiSig<NopSignature>>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(
            10,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        let res = futures::executor::block_on(mempool.next());
        assert!(res.is_some());
        assert!(!mempool.ready());
        let txs_list_1 = match res.unwrap() {
            MonadEvent::ConsensusEvent(ConsensusEvent::FetchedTxs(_, txs_list)) => txs_list,
            _ => {
                panic!("wrong event returned")
            }
        };

        let mut mempool =
            MockMempool::<NopSignature, MultiSig<NopSignature>>::new(MockMempoolConfig(100));
        mempool.exec(vec![MempoolCommand::FetchTxs(
            10,
            vec![],
            dont_care_fetched_tx_param(),
        )]);
        let res = futures::executor::block_on(mempool.next());
        assert!(res.is_some());
        assert!(!mempool.ready());
        let txs_list_2 = match res.unwrap() {
            MonadEvent::ConsensusEvent(ConsensusEvent::FetchedTxs(_, txs_list)) => txs_list,
            _ => {
                panic!("wrong event returned")
            }
        };
        assert_eq!(txs_list_2.0.len(), 10);
        assert_eq!(txs_list_2.0.len(), txs_list_1.0.len());
        assert_ne!(txs_list_2.0, txs_list_1.0);
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
