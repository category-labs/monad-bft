use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet, VecDeque},
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};

use super::{ledger::MockLedger, mempool::MockMempool};
use crate::{
    state::PeerId, Command, Executor, Message, RouterCommand, RouterTarget, State, TimerCommand,
};

pub struct MockExecutor<S>
where
    S: State,
{
    mempool: MockMempool<S::Event>,
    ledger: MockLedger<S::Block>,

    tick: Duration,

    timer: Option<TimerEvent<S::Event>>,

    // caller push_backs inbound stuff here (via MockExecutor::send_*)
    inbound_messages: BinaryHeap<SequencedPeerEvent<S::Message>>,

    // caller pop_front outbounds from this (via MockExecutor::receive_*)
    outbound_messages: VecDeque<(RouterTarget, S::OutboundMessage)>,
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
        other.tick.partial_cmp(&self.tick)
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
    InboundMessage,

    Timer,
    Mempool,
}

impl<S> MockExecutor<S>
where
    S: State,
{
    pub fn tick(&self) -> Duration {
        self.tick
    }
    pub fn send_message(
        &mut self,
        tick: Duration,
        from: PeerId,
        message: S::Message,
        tx_tick: Duration,
    ) {
        assert!(tick >= self.tick);
        self.inbound_messages.push(SequencedPeerEvent {
            tick,
            from,
            t: message,

            tx_tick,
        });
    }
    pub fn receive_message(&mut self) -> Option<(RouterTarget, S::OutboundMessage)> {
        self.outbound_messages.pop_front()
    }

    fn peek_event(&self) -> Option<(Duration, ExecutorEventType)> {
        std::iter::empty()
            .chain(
                self.mempool
                    .ready()
                    .then_some((self.tick, ExecutorEventType::Mempool))
                    .into_iter(),
            )
            .chain(
                self.inbound_messages
                    .peek()
                    .map(|SequencedPeerEvent { tick, .. }| {
                        (*tick, ExecutorEventType::InboundMessage)
                    })
                    .into_iter(),
            )
            .chain(
                self.timer
                    .as_ref()
                    .map(|TimerEvent { tick, .. }| (*tick, ExecutorEventType::Timer))
                    .into_iter(),
            )
            .min()
    }

    pub fn peek_event_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(duration, _)| duration)
    }

    pub fn pending_timer(&self) -> &Option<TimerEvent<S::Event>> {
        &self.timer
    }

    pub fn pending_messages(&self) -> &BinaryHeap<SequencedPeerEvent<S::Message>> {
        &self.inbound_messages
    }
}

impl<S> Default for MockExecutor<S>
where
    S: State,
{
    fn default() -> Self {
        Self {
            mempool: Default::default(),
            ledger: Default::default(),

            tick: Duration::default(),

            timer: None,

            inbound_messages: BinaryHeap::new(),

            outbound_messages: VecDeque::new(),
        }
    }
}

impl<S> Executor for MockExecutor<S>
where
    S: State,
{
    type Command = Command<S::Message, S::OutboundMessage, S::Block>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut to_publish = Vec::new();
        let mut to_unpublish = HashSet::new();

        let (router_cmds, timer_cmds, mempool_cmds, ledger_cmds) =
            Self::Command::split_commands(commands);
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

        for (target, message) in to_publish {
            let id = message.as_ref().id();
            if to_unpublish.contains(&(target, id)) {
                continue;
            }
            self.outbound_messages.push_back((target, message));
        }
    }
}

impl<S> Stream for MockExecutor<S>
where
    S: State,
    Self: Unpin,
{
    type Item = S::Event;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Poll::Ready(Some(event)) = this.mempool.poll_next_unpin(cx) {
            return Poll::Ready(Some(event));
        }

        if let Some((tick, event_type)) = this.peek_event() {
            this.tick = tick;
            let event = match event_type {
                ExecutorEventType::InboundMessage => {
                    let SequencedPeerEvent {
                        from,
                        t: message,
                        tick: _,

                        tx_tick: _,
                    } = this.inbound_messages.pop().unwrap();

                    message.event(from)
                }
                ExecutorEventType::Timer => this.timer.take().unwrap().event,
                ExecutorEventType::Mempool => return this.mempool.poll_next_unpin(cx),
            };
            return Poll::Ready(Some(event));
        }

        Poll::Ready(None)
    }
}

impl<S> MockExecutor<S>
where
    S: State,
{
    pub fn ledger(&self) -> &MockLedger<S::Block> {
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

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fmt::Debug, time::Duration};

    use futures::{FutureExt, StreamExt};
    use monad_crypto::{secp256k1::KeyPair, GenericKeyPair};
    use monad_testutil::signing::{create_keys, node_id};
    use monad_types::{Deserializable, Serializable};
    use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

    use super::*;
    use crate::{
        mock_swarm::{LatencyTransformer, Nodes},
        state::{Command, Executor, PeerId, RouterCommand, State, TimerCommand},
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

    impl Serializable for SimpleChainEvent {
        fn serialize(&self) -> Vec<u8> {
            unreachable!("not used")
        }
    }

    impl Deserializable for SimpleChainEvent {
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
        type Block = ();

        fn init(
            config: Self::Config,
        ) -> (
            Self,
            Vec<Command<Self::Message, Self::OutboundMessage, Self::Block>>,
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
        ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block>> {
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

    #[derive(Clone)]
    struct SimpleChainMessage {
        round: u64,
    }
    impl AsRef<Self> for SimpleChainMessage {
        fn as_ref(&self) -> &Self {
            self
        }
    }

    impl Message for SimpleChainMessage {
        type Event = SimpleChainEvent;
        type Id = u64;

        fn id(&self) -> Self::Id {
            self.round
        }

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
            .into_iter()
            .map(|peer_id| peer_id.0)
            .zip(state_configs)
            .zip(std::iter::repeat(MockWALoggerConfig {}))
            .map(|((a, b), c)| (a, b, c))
            .collect();
        let mut nodes =
            Nodes::<SimpleChainState, _, MockWALogger<TimedEvent<SimpleChainEvent>>>::new(
                peers,
                LatencyTransformer(Duration::from_millis(50)),
            );

        while let Some((duration, id, event)) = nodes.step() {
            println!("{duration:?} => {id:?} => {event:?}")
        }
    }
}
