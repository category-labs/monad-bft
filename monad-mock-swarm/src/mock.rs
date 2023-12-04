use std::{
    cmp::{Ordering, Reverse},
    hash::{Hash, Hasher},
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
    payload::{FullTransactionList, TransactionHashList},
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
    validator_data::ValidatorData,
};
use monad_crypto::secp256k1::PubKey;
use monad_eth_types::EMPTY_RLP_TX_LIST;
use monad_executor::{Executor, State};
use monad_executor_glue::{
    Command, ExecutionLedgerCommand, MempoolCommand, Message, MonadEvent, RouterCommand,
    TimerCommand, ValidatorSetCommand,
};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_types::{Epoch, NodeId, Stake, TimeoutVariant};
use monad_updaters::{
    checkpoint::MockCheckpoint, ledger::MockLedger, state_root_hash::MockStateRootHash,
};
use priority_queue::PriorityQueue;
use rand::{Rng, RngCore};
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng, ChaChaRng};

use crate::swarm_relation::SwarmRelation;

pub trait MockableExecutor:
    Executor<Command = MempoolCommand<Self::SignatureCollection>> + Stream<Item = Self::Event> + Unpin
{
    type Config: Copy;
    type Event;
    type SignatureCollection;

    fn new(config: Self::Config) -> Self;

    fn ready(&self) -> bool;
}

pub struct MockExecutor<S>
where
    S: SwarmRelation,
{
    mempool: S::MempoolExecutor,
    ledger: MockLedger<<S::State as State>::Block, <S::State as State>::Event>,
    execution_ledger: MockExecutionLedger<S::SignatureCollectionType>,
    checkpoint: MockCheckpoint<<S::State as State>::Checkpoint>,
    validator_set: S::ValidatorSetExecutor,
    state_root_hash:
        MockStateRootHash<<S::State as State>::Block, S::SignatureType, S::SignatureCollectionType>,
    tick: Duration,

    timer: PriorityQueue<TimerEvent<<S::State as State>::Event>, Reverse<Duration>>,
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

pub struct SequencedPeerEvent<T> {
    pub tick: Duration,
    pub from: NodeId,
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
    ValidatorSet,
    Timer,
    Mempool,
    StateRootHash,
}

impl<S> MockExecutor<S>
where
    S: SwarmRelation,
{
    pub fn new(
        router: S::RouterScheduler,
        mempool_config: <S::MempoolExecutor as MockableExecutor>::Config,
        peers: Vec<(
            PubKey,
            SignatureCollectionPubKeyType<S::SignatureCollectionType>,
        )>,
        tick: Duration,
    ) -> Self {
        Self {
            checkpoint: Default::default(),
            ledger: Default::default(),
            execution_ledger: Default::default(),
            mempool: <S::MempoolExecutor as MockableExecutor>::new(mempool_config),
            validator_set: <S::ValidatorSetExecutor as MockableValidatorSet>::new(peers),
            state_root_hash: Default::default(),

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
        from: NodeId,
        message: <S::RouterScheduler as RouterScheduler>::TransportMessage,
    ) {
        assert!(tick >= self.tick);

        self.router.process_inbound(tick, from, message);
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
                    .peek()
                    .map(|(_, tick)| (tick.0, ExecutorEventType::Timer)),
            )
            .chain(
                self.validator_set
                    .ready()
                    .then_some((self.tick, ExecutorEventType::ValidatorSet)),
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
{
    type Command = Command<
        <S::State as State>::Event,
        <S::State as State>::OutboundMessage,
        <S::State as State>::Block,
        <S::State as State>::Checkpoint,
        <S::State as State>::SignatureCollection,
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
            validator_set_cmds,
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

        self.mempool.exec(mempool_cmds);
        self.ledger.exec(ledger_cmds);
        self.execution_ledger.exec(execution_ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);
        self.validator_set.exec(validator_set_cmds);

        for command in router_cmds {
            match command {
                RouterCommand::Publish { target, message } => {
                    self.router.send_outbound(self.tick, target, message);
                }
            }
        }
    }
}

pub enum MockExecutorEvent<E, TransportMessage> {
    Event(E),
    Send(NodeId, TransportMessage),
}

impl<S> MockExecutor<S>
where
    S: SwarmRelation,
{
    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<MockExecutorEvent<<S::State as State>::Event, S::TransportMessage>> {
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
                ExecutorEventType::ValidatorSet => {
                    return futures::executor::block_on(self.validator_set.next())
                        .map(MockExecutorEvent::Event)
                }
                ExecutorEventType::Timer => {
                    MockExecutorEvent::Event(self.timer.pop().unwrap().0.callback.unwrap())
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
    pub fn ledger(&self) -> &MockLedger<<S::State as State>::Block, <S::State as State>::Event> {
        &self.ledger
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

const MOCK_DEFAULT_SEED: u64 = 1;

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
    fn get_fetched_txs_list(&mut self) -> TransactionHashList {
        if self.num_fetch_txs == 0 {
            TransactionHashList::new(vec![EMPTY_RLP_TX_LIST])
        } else {
            // Random non-empty value with size = num_fetch_txs * hash_size
            let mut buf = Vec::with_capacity(self.num_fetch_txs * 32);
            buf.resize(self.num_fetch_txs * 32, 0);
            self.rng.fill_bytes(buf.as_mut_slice());
            TransactionHashList::new(buf)
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
                    Some(FullTransactionList::new(Vec::new())),
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
                    Some(FullTransactionList::new(Vec::new())),
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

pub trait MockableValidatorSet:
    Executor<Command = ValidatorSetCommand> + Stream<Item = Self::Event> + Unpin
{
    type Event;
    type SignatureCollection: SignatureCollection;

    fn new(
        peers: Vec<(
            PubKey,
            SignatureCollectionPubKeyType<Self::SignatureCollection>,
        )>,
    ) -> Self;

    fn ready(&self) -> bool;
}

pub struct MockValidatorSetUpdaterNop<ST, SCT: SignatureCollection> {
    init_validator_set: ValidatorData<SCT>,
    validator_set: Option<ValidatorData<SCT>>,
    phantom: PhantomData<ST>,
}

impl<ST, SCT> MockableValidatorSet for MockValidatorSetUpdaterNop<ST, SCT>
where
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn new(peers: Vec<(PubKey, SignatureCollectionPubKeyType<SCT>)>) -> Self {
        let val_set = peers
            .into_iter()
            .map(|(pubkey, cert_pubkey)| (NodeId(pubkey), Stake(1), cert_pubkey))
            .collect::<Vec<_>>();

        Self {
            init_validator_set: ValidatorData(val_set),
            validator_set: None,
            phantom: Default::default(),
        }
    }

    fn ready(&self) -> bool {
        self.validator_set.is_some()
    }
}

impl<ST, SCT: SignatureCollection> Default for MockValidatorSetUpdaterNop<ST, SCT> {
    fn default() -> Self {
        Self {
            init_validator_set: ValidatorData(vec![]),
            validator_set: None,
            phantom: Default::default(),
        }
    }
}

impl<ST, SCT: SignatureCollection> Executor for MockValidatorSetUpdaterNop<ST, SCT> {
    type Command = ValidatorSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ValidatorSetCommand::EpochEnd(_) => {
                    self.validator_set = Some(self.init_validator_set.clone());
                }
            }
        }
    }
}

impl<ST, SCT> Stream for MockValidatorSetUpdaterNop<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        Poll::Ready(Some(MonadEvent::ConsensusEvent(
            monad_executor_glue::ConsensusEvent::UpdateNextValSet(
                this.validator_set
                    .take()
                    .expect("there should be a ValidatorData object"),
            ),
        )))
    }
}

pub struct MockValidatorSetUpdater<ST, SCT: SignatureCollection> {
    val_set_1: ValidatorData<SCT>,
    val_set_2: ValidatorData<SCT>,
    epoch_num: Epoch,
    validator_set: Option<ValidatorData<SCT>>,
    phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> MockableValidatorSet for MockValidatorSetUpdater<ST, SCT>
where
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn new(peers: Vec<(PubKey, SignatureCollectionPubKeyType<SCT>)>) -> Self {
        let val_set = peers
            .into_iter()
            .map(|(pubkey, cert_pubkey)| (NodeId(pubkey), Stake(1), cert_pubkey))
            .collect::<Vec<(NodeId, Stake, SignatureCollectionPubKeyType<SCT>)>>();

        let val_set_1: Vec<(NodeId, Stake, SignatureCollectionPubKeyType<SCT>)> = val_set.clone();
        let val_set_2: Vec<(NodeId, Stake, SignatureCollectionPubKeyType<SCT>)> = val_set
            .into_iter()
            .map(|(node_id, _, cert_pubkey)| (node_id, Stake(2), cert_pubkey))
            .collect();

        Self {
            val_set_1: ValidatorData(val_set_1),
            val_set_2: ValidatorData(val_set_2.clone()),
            epoch_num: Epoch(1),
            validator_set: Some(ValidatorData(val_set_2)),
            phantom: Default::default(),
        }
    }

    fn ready(&self) -> bool {
        self.validator_set.is_some()
    }
}

impl<ST, SCT: SignatureCollection> Executor for MockValidatorSetUpdater<ST, SCT> {
    type Command = ValidatorSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ValidatorSetCommand::EpochEnd(_) => {
                    self.epoch_num = self.epoch_num + Epoch(1);

                    if self.epoch_num.0 % 2 == 0 {
                        self.validator_set = Some(self.val_set_1.clone());
                    } else {
                        self.validator_set = Some(self.val_set_2.clone());
                    }
                }
            }
        }
    }
}

impl<ST, SCT> Stream for MockValidatorSetUpdater<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        Poll::Ready(Some(MonadEvent::ConsensusEvent(
            monad_executor_glue::ConsensusEvent::UpdateNextValSet(
                this.validator_set
                    .take()
                    .expect("there should be a ValidatorData object"),
            ),
        )))
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{collections::HashSet, time::Duration};

    use futures::{FutureExt, StreamExt};
    use monad_consensus_types::{multi_sig::MultiSig, quorum_certificate::QuorumCertificate};
    use monad_crypto::{
        hasher::{Hash, HasherType},
        NopSignature,
    };
    use monad_executor::Executor;
    use monad_executor_glue::{ConsensusEvent, TimerCommand};
    use monad_testutil::signing::node_id;
    use monad_types::{BlockId, Round, SeqNum};

    use super::*;

    fn dont_care_fetched_tx_param<S: SignatureCollection>() -> FetchTxParams<S> {
        FetchTxParams {
            node_id: node_id(),
            round: Round(0),
            seq_num: SeqNum(0),
            state_root_hash: Default::default(),
            high_qc: QuorumCertificate::genesis_prime_qc::<HasherType>(),
            last_round_tc: None,
        }
    }

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
        assert_eq!(txs_list_2.as_bytes().len(), 10 * 32);
        assert_eq!(txs_list_2.as_bytes().len(), txs_list_1.as_bytes().len());
        assert_ne!(txs_list_2, txs_list_1);
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
