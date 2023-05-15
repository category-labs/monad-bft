use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet, VecDeque},
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{state::PeerId, Command, Executor, Message, RouterCommand, State, TimerCommand};

use futures::Stream;

pub struct MockExecutor<S>
where
    S: State,
{
    tick: Duration,

    timer: Option<TimerEvent<S::Event>>,

    // caller push_backs inbound stuff here (via MockExecutor::send_*)
    inbound_messages: BinaryHeap<SequencedPeerEvent<S::Message>>,
    inbound_ack: BinaryHeap<SequencedPeerEvent<<S::Message as Message>::Id>>,

    // caller pop_front outbounds from this (via MockExecutor::receive_*)
    outbound_messages: VecDeque<(PeerId, S::OutboundMessage)>,
    outbound_ack: VecDeque<(PeerId, <S::Message as Message>::Id)>,

    sent_messages: HashMap<PeerId, HashMap<<S::Message as Message>::Id, S::Event>>,
    received_messages: Vec<(PeerId, <S::Message as Message>::Id)>,
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
    InboundAck,

    Timer,
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
    pub fn send_ack(
        &mut self,
        tick: Duration,
        from: PeerId,
        ack: <S::Message as Message>::Id,
        tx_tick: Duration,
    ) {
        assert!(tick >= self.tick);
        self.inbound_ack.push(SequencedPeerEvent {
            tick,
            from,
            t: ack,
            tx_tick,
        });
    }
    pub fn receive_message(&mut self) -> Option<(PeerId, S::OutboundMessage)> {
        self.outbound_messages.pop_front()
    }
    pub fn receive_ack(&mut self) -> Option<(PeerId, <S::Message as Message>::Id)> {
        self.outbound_ack.pop_front()
    }

    fn peek_event(&self) -> Option<(Duration, ExecutorEventType)> {
        std::iter::empty()
            .chain(
                self.inbound_messages
                    .peek()
                    .map(|SequencedPeerEvent { tick, .. }| {
                        (*tick, ExecutorEventType::InboundMessage)
                    })
                    .into_iter(),
            )
            .chain(
                self.inbound_ack
                    .peek()
                    .map(|SequencedPeerEvent { tick, .. }| (*tick, ExecutorEventType::InboundAck))
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

    pub fn pending_acks(&self) -> &BinaryHeap<SequencedPeerEvent<<S::Message as Message>::Id>> {
        &self.inbound_ack
    }
}

impl<S> Default for MockExecutor<S>
where
    S: State,
{
    fn default() -> Self {
        Self {
            tick: Duration::default(),

            timer: None,

            inbound_messages: BinaryHeap::new(),
            inbound_ack: BinaryHeap::new(),

            outbound_messages: VecDeque::new(),
            outbound_ack: VecDeque::new(),

            sent_messages: HashMap::new(),
            received_messages: Vec::new(),
        }
    }
}

impl<S> Executor for MockExecutor<S>
where
    S: State,
{
    type Command = Command<S::Message, S::OutboundMessage>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        // we must have processed received messages at this point, so we can send out acks
        self.outbound_ack.extend(
            self.received_messages
                .drain(..)
                .map(|(from, message)| (from, message)),
        );

        let mut to_publish = Vec::new();
        let mut to_unpublish = HashSet::new();
        for command in commands {
            match command {
                Command::TimerCommand(TimerCommand::Unschedule) => self.timer = None,
                Command::TimerCommand(TimerCommand::Schedule {
                    duration,
                    on_timeout,
                }) => {
                    self.timer = Some(TimerEvent {
                        event: on_timeout,
                        tick: self.tick + duration,

                        scheduled_tick: self.tick,
                    })
                }
                Command::RouterCommand(RouterCommand::Publish {
                    to,
                    message,
                    on_ack,
                }) => {
                    to_publish.push((to, message, on_ack));
                }
                Command::RouterCommand(RouterCommand::Unpublish { to, id }) => {
                    to_unpublish.insert((to, id));
                }
            }
        }

        for (to, message, on_ack) in to_publish {
            let id = message.as_ref().id();
            if to_unpublish.contains(&(to, id.clone())) {
                continue;
            }
            self.outbound_messages.push_back((to, message));
            let entry = self.sent_messages.entry(to).or_default().entry(id);
            match entry {
                Entry::Occupied(_) => panic!("can't double publish!"),
                Entry::Vacant(v) => v.insert(on_ack),
            };
        }

        for (to, payload) in to_unpublish {
            self.sent_messages.entry(to).or_default().remove(&payload);
        }

        // remove any unpublished messages
        while self.inbound_ack.peek().map_or(
            false,
            |SequencedPeerEvent {
                 from,
                 t: ack,
                 tick: _,

                 tx_tick: _,
             }| {
                !self
                    .sent_messages
                    .entry(*from)
                    .or_default()
                    .contains_key(ack)
            },
        ) {
            self.inbound_ack.pop();
        }
    }
}

impl<S> Stream for MockExecutor<S>
where
    S: State,
    Self: Unpin,
{
    type Item = S::Event;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

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

                    this.received_messages.push((from, message.id()));

                    message.event(from)
                }
                ExecutorEventType::InboundAck => {
                    let SequencedPeerEvent {
                        from,
                        t: ack,
                        tick: _,

                        tx_tick: _,
                    } = this.inbound_ack.pop().unwrap();

                    this.sent_messages
                        .entry(from)
                        .or_default()
                        .remove(&ack)
                        .unwrap()
                }
                ExecutorEventType::Timer => this.timer.take().unwrap().event,
            };
            Poll::Ready(Some(event))
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct MockTimer<E> {
    event: Option<E>,
}
impl<E> Default for MockTimer<E> {
    fn default() -> Self {
        Self { event: None }
    }
}
impl<E> Executor for MockTimer<E> {
    type Command = TimerCommand<E>;
    fn exec(&mut self, commands: Vec<TimerCommand<E>>) {
        for command in commands {
            match command {
                TimerCommand::Schedule {
                    duration: _,
                    on_timeout,
                } => self.event = Some(on_timeout),
                TimerCommand::Unschedule => self.event = None,
            }
        }
    }
}
impl<E> Stream for MockTimer<E>
where
    E: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.deref_mut().event.take())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use futures::StreamExt;

    use monad_crypto::secp256k1::KeyPair;
    use monad_testutil::signing::{create_keys, node_id};

    use crate::{
        executor::mock::MockExecutor,
        mock_swarm::Nodes,
        state::{Command, Executor, PeerId, RouterCommand, State, TimerCommand},
        Message,
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    /// EXAMPLE APPLICATION-LEVEL STUFF
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, PartialEq, Eq)]
    struct LongAckState {
        num_ack: u64,
        num_timeouts: u64,
    }
    #[derive(Clone)]
    enum LongAckEvent {
        IncrementNumTimeout,
        IncrementNumAck,
    }

    impl State for LongAckState {
        type Config = ();
        type Event = LongAckEvent;
        type OutboundMessage = LongAckMessage;
        type Message = LongAckMessage;

        fn init(
            _config: Self::Config,
        ) -> (Self, Vec<Command<Self::Message, Self::OutboundMessage>>) {
            let init_self = Self {
                num_ack: 0,
                num_timeouts: 0,
            };

            let init_cmds = vec![
                Command::TimerCommand(TimerCommand::Schedule {
                    duration: std::time::Duration::from_secs(1),
                    on_timeout: LongAckEvent::IncrementNumTimeout,
                }),
                Command::RouterCommand(RouterCommand::Publish {
                    to: PeerId(node_id().0),
                    message: LongAckMessage(init_self.num_ack),
                    on_ack: LongAckEvent::IncrementNumAck,
                }),
            ];

            (init_self, init_cmds)
        }
        fn update(
            &mut self,
            event: Self::Event,
        ) -> Vec<Command<Self::Message, Self::OutboundMessage>> {
            let mut commands = Vec::new();
            match event {
                LongAckEvent::IncrementNumAck => {
                    commands.push(Command::RouterCommand(RouterCommand::Unpublish {
                        to: PeerId(node_id().0),
                        id: self.num_ack,
                    }));
                    self.num_ack += 1;
                    commands.push(Command::RouterCommand(RouterCommand::Publish {
                        to: PeerId(node_id().0),
                        message: LongAckMessage(self.num_ack),
                        on_ack: LongAckEvent::IncrementNumAck,
                    }));
                    // reset timer back to 1 second
                    commands.push(Command::TimerCommand(TimerCommand::Schedule {
                        duration: std::time::Duration::from_secs(1),
                        on_timeout: LongAckEvent::IncrementNumTimeout,
                    }));
                }
                LongAckEvent::IncrementNumTimeout => {
                    self.num_timeouts += 1;
                    // reset timer back to 1 second
                    commands.push(Command::TimerCommand(TimerCommand::Schedule {
                        duration: std::time::Duration::from_secs(1),
                        on_timeout: LongAckEvent::IncrementNumTimeout,
                    }));
                }
            };
            commands
        }
    }

    #[derive(Clone)]
    struct LongAckMessage(u64);
    impl AsRef<Self> for LongAckMessage {
        fn as_ref(&self) -> &Self {
            self
        }
    }
    impl Message for LongAckMessage {
        type Event = LongAckEvent;
        type Id = u64;

        fn id(&self) -> Self::Id {
            self.0
        }

        fn event(self, _from: PeerId) -> Self::Event {
            Self::Event::IncrementNumAck
        }
    }

    fn simulate_peer<S: State>(
        executor: &mut MockExecutor<S>,
        message_delays: &mut impl Iterator<Item = Duration>,
    ) {
        while let Some((to, outbound_message)) = executor.receive_message() {
            // ack all messages that executor wants to send out
            executor.send_ack(
                executor.tick() + message_delays.next().unwrap(),
                to,
                outbound_message.into().id(),
                executor.tick(),
            )
        }
        while executor.receive_ack().is_some() {}
    }

    fn run_simulation<S>(
        config: S::Config,
        init_events: Vec<S::Event>,
        mut message_delays: impl Iterator<Item = Duration>,
        terminate: impl Fn(&S) -> bool,
    ) -> (S, Vec<S::Event>)
    where
        S: State,
        MockExecutor<S>: Unpin,
    {
        let mut executor: MockExecutor<S> = MockExecutor::default();

        let (mut state, mut init_commands) = S::init(config);
        let mut event_log = init_events.clone();
        for event in init_events {
            let cmds = state.update(event);
            init_commands.extend(cmds.into_iter());
        }
        executor.exec(init_commands);
        simulate_peer(&mut executor, &mut message_delays);

        while let Some(event) = futures::executor::block_on(executor.next()) {
            event_log.push(event.clone());
            let commands = state.update(event);

            if terminate(&state) {
                break;
            }

            executor.exec(commands);
            simulate_peer(&mut executor, &mut message_delays);
        }

        (state, event_log)
    }

    #[test]
    fn test_100_milli_ack() {
        let (state, events) = run_simulation::<LongAckState>(
            (),
            Vec::new(),
            std::iter::repeat(Duration::from_millis(100)),
            |state| state.num_ack == 1000,
        );
        assert_eq!(state.num_timeouts, 0);

        let mut replay_state = LongAckState::init(()).0;
        for event in events {
            replay_state.update(event);
        }
        assert_eq!(state, replay_state);
    }

    #[test]
    fn test_1000_milli_ack() {
        let (state, events) = run_simulation::<LongAckState>(
            (),
            Vec::new(),
            std::iter::repeat(Duration::from_millis(1_001)),
            |state| state.num_ack == 1000,
        );
        assert_eq!(state.num_timeouts, 1_000);

        let mut replay_state = LongAckState::init(()).0;
        for event in events {
            replay_state.update(event);
        }
        assert_eq!(state, replay_state);
    }

    #[test]
    fn test_half_long_ack() {
        let (state, events) = run_simulation::<LongAckState>(
            (),
            Vec::new(),
            (0..).map(|i| (i % 2) * Duration::from_millis(1_001)),
            |state| state.num_ack == 1000,
        );
        assert_eq!(state.num_timeouts, 500);

        let mut replay_state = LongAckState::init(()).0;
        for event in events {
            replay_state.update(event);
        }
        assert_eq!(state, replay_state);
    }

    #[test]
    fn test_crash() {
        // send 500 messages
        let (_, init_events) = run_simulation::<LongAckState>(
            (),
            Vec::new(),
            // send back 500 acks MAX
            std::iter::repeat(Duration::from_millis(1_001)).take(500),
            |state| state.num_ack == 500,
        );
        // replay those 500 messages, send another 500 messages
        let (state, events) = run_simulation::<LongAckState>(
            (),
            init_events,
            // send back 500 acks MAX
            std::iter::repeat(Duration::from_millis(1_001)).take(500),
            |state| state.num_ack == 1_000,
        );
        // total should be 1_000
        assert_eq!(state.num_timeouts, 1_000);

        let mut replay_state = LongAckState::init(()).0;
        for event in events {
            replay_state.update(event);
        }
        // replaying all 1_000 messages at once should match
        assert_eq!(state, replay_state);
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
        Ack { peer: PeerId, round: u64 },
    }

    impl State for SimpleChainState {
        type Config = (Vec<PeerId>, PeerId);
        type Event = SimpleChainEvent;
        type OutboundMessage = SimpleChainMessage;
        type Message = SimpleChainMessage;

        fn init(
            config: Self::Config,
        ) -> (Self, Vec<Command<Self::Message, Self::OutboundMessage>>) {
            let (pubkeys, me) = config;

            let init_cmds = pubkeys
                .iter()
                .map(|peer| {
                    Command::RouterCommand(RouterCommand::Publish {
                        to: *peer,
                        message: SimpleChainMessage { round: 0 },
                        on_ack: SimpleChainEvent::Ack {
                            peer: *peer,
                            round: 0,
                        },
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
        ) -> Vec<Command<Self::Message, Self::OutboundMessage>> {
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
                                to: *peer,
                                message: SimpleChainMessage {
                                    round: self.chain.len() as u64 - 1,
                                },
                                on_ack: SimpleChainEvent::Ack {
                                    peer: *peer,
                                    round: self.chain.len() as u64 - 1,
                                },
                            })
                        }));
                        self.outbound_votes
                            .push(self.peers.iter().cloned().collect());
                    }
                }
                SimpleChainEvent::Ack { peer, round } => {
                    self.outbound_votes[round as usize].remove(&peer);
                    commands.push(Command::RouterCommand(RouterCommand::Unpublish {
                        to: peer,
                        id: round,
                    }));
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
        let pubkeys = create_keys(NUM_NODES as u32)
            .iter()
            .map(KeyPair::pubkey)
            .map(PeerId)
            .collect::<Vec<_>>();
        let state_configs = (0..NUM_NODES)
            .map(|idx| (pubkeys.clone(), pubkeys[idx as usize]))
            .collect::<Vec<_>>();
        let mut nodes = Nodes::<SimpleChainState, _>::new(
            pubkeys
                .into_iter()
                .map(|peer_id| peer_id.0)
                .zip(state_configs)
                .collect(),
            |_from, _to| Duration::from_millis(50),
            Vec::new(),
        );

        while let Some((duration, id, event)) = nodes.step() {
            println!("{duration:?} => {id:?} => {event:?}")
        }
    }
}
