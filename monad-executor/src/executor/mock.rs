use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    state::{Codec, PeerId},
    Command, Executor, RouterCommand, TimerCommand,
};

use futures::Stream;

pub struct MockExecutor<C>
where
    C: Codec,
{
    tick: Duration,

    timer: Option<(Duration, C::Event)>,

    // caller push_backs inbound stuff here (via MockExecutor::send_*)
    inbound_messages: VecDeque<(Duration, PeerId, Vec<u8>)>,
    inbound_ack: VecDeque<(Duration, PeerId, Vec<u8>)>,

    // caller pop_front outbounds from this (via MockExecutor::receive_*)
    outbound_messages: VecDeque<(PeerId, Vec<u8>)>,
    outbound_ack: VecDeque<(PeerId, Vec<u8>)>,

    sent_messages: HashMap<PeerId, HashMap<Vec<u8>, C::Event>>,
    received_messages: Vec<(PeerId, Vec<u8>)>,
}

impl<C> MockExecutor<C>
where
    C: Codec,
{
    pub fn tick(&self) -> Duration {
        self.tick
    }
    pub fn send_message(&mut self, tick: Duration, from: PeerId, message: Vec<u8>) {
        assert!(tick >= self.tick);
        assert!(
            tick >= self
                .inbound_messages
                .back()
                .map(|(tick, _, _)| *tick)
                .unwrap_or_default()
        );
        self.inbound_messages.push_back((tick, from, message));
    }
    pub fn send_ack(&mut self, tick: Duration, from: PeerId, ack: Vec<u8>) {
        assert!(tick >= self.tick);
        assert!(
            tick >= self
                .inbound_ack
                .back()
                .map(|(tick, _, _)| *tick)
                .unwrap_or_default()
        );
        self.inbound_ack.push_back((tick, from, ack));
    }
    pub fn receive_message(&mut self) -> Option<(PeerId, Vec<u8>)> {
        self.outbound_messages.pop_front()
    }
    pub fn receive_ack(&mut self) -> Option<(PeerId, Vec<u8>)> {
        self.outbound_ack.pop_front()
    }
}

impl<C> MockExecutor<C>
where
    C: Codec,
{
    pub fn new() -> Self {
        Self {
            tick: Duration::default(),

            timer: None,

            inbound_messages: VecDeque::new(),
            inbound_ack: VecDeque::new(),

            outbound_messages: VecDeque::new(),
            outbound_ack: VecDeque::new(),

            sent_messages: HashMap::new(),
            received_messages: Vec::new(),
        }
    }
}

impl<C> Executor for MockExecutor<C>
where
    C: Codec,
{
    type Command = Command<C::Event>;
    fn exec(&mut self, commands: Vec<Command<C::Event>>) {
        // we must have processed received messages at this point, so we can send out acks
        self.outbound_ack.extend(
            self.received_messages
                .drain(..)
                .map(|(from, message)| (from, message)),
        );

        let mut to_publish = HashMap::new();
        let mut to_unpublish = HashSet::new();
        for command in commands {
            match command {
                Command::TimerCommand(TimerCommand::Unschedule) => self.timer = None,
                Command::TimerCommand(TimerCommand::Schedule {
                    duration,
                    on_timeout,
                }) => self.timer = Some((self.tick + duration, on_timeout)),
                Command::RouterCommand(RouterCommand::Publish {
                    to,
                    payload,
                    on_ack,
                }) => {
                    to_publish.insert((to, payload), on_ack);
                }
                Command::RouterCommand(RouterCommand::Unpublish { to, payload }) => {
                    to_unpublish.insert((to, payload));
                }
            }
        }

        for (key, on_ack) in to_publish {
            if to_unpublish.contains(&key) {
                continue;
            }
            let (to, payload) = key;
            self.outbound_messages
                .push_back((to.clone(), payload.clone()));
            let entry = self.sent_messages.entry(to).or_default().entry(payload);
            match entry {
                Entry::Occupied(_) => panic!("can't double publish!"),
                Entry::Vacant(v) => v.insert(on_ack),
            };
        }

        for (to, payload) in to_unpublish {
            self.sent_messages.entry(to).or_default().remove(&payload);
        }
    }
}

impl<C> Stream for MockExecutor<C>
where
    C: Codec,
{
    type Item = C::Event;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        enum ExecutorEventType {
            InboundMessage,
            InboundAck,

            Timer,
        }
        while let Some((tick, event_type)) = std::iter::empty()
            .chain(
                this.inbound_messages
                    .front()
                    .map(|(tick, _, _)| (*tick, ExecutorEventType::InboundMessage))
                    .into_iter(),
            )
            .chain(
                this.inbound_ack
                    .front()
                    .map(|(tick, _, _)| (*tick, ExecutorEventType::InboundAck))
                    .into_iter(),
            )
            .chain(
                this.timer
                    .as_ref()
                    .map(|(tick, _)| (*tick, ExecutorEventType::Timer))
                    .into_iter(),
            )
            .min()
        {
            this.tick = tick;
            match event_type {
                ExecutorEventType::InboundMessage => {
                    let (_, from, message) = this.inbound_messages.pop_front().unwrap();

                    let event = C::parse_peer_payload(from.clone(), &message);
                    this.received_messages.push((from, message));

                    if let Ok(event) = event {
                        return Poll::Ready(Some(event));
                    }
                }
                ExecutorEventType::InboundAck => {
                    let (_, from, ack) = this.inbound_ack.pop_front().unwrap();

                    let maybe_ack = this.sent_messages.entry(from).or_default().remove(&ack);
                    if maybe_ack.is_some() {
                        // when receive ack for a message, call on_ack for that message
                        return Poll::Ready(maybe_ack);
                    }
                }
                ExecutorEventType::Timer => {
                    let (_, event) = this.timer.take().unwrap();
                    return Poll::Ready(Some(event));
                }
            }
        }
        Poll::Ready(None)
    }
}

pub struct MockTimer<E> {
    event: Option<E>,
}
impl<E> MockTimer<E> {
    pub fn new() -> Self {
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
    use std::time::Duration;

    use futures::StreamExt;

    use crate::{
        executor::mock::MockExecutor,
        state::{Codec, Command, Executor, PeerId, RouterCommand, State, TimerCommand},
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
    enum LongAckParseError {}

    impl State for LongAckState {
        type Event = LongAckEvent;

        fn init() -> (Self, Vec<Command<Self::Event>>) {
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
                    to: PeerId,
                    payload: init_self.num_ack.to_ne_bytes().to_vec(),
                    on_ack: LongAckEvent::IncrementNumAck,
                }),
            ];

            (init_self, init_cmds)
        }
        fn update(&mut self, event: Self::Event) -> Vec<Command<Self::Event>> {
            let mut commands = Vec::new();
            match event {
                LongAckEvent::IncrementNumAck => {
                    commands.push(Command::RouterCommand(RouterCommand::Unpublish {
                        to: PeerId,
                        payload: self.num_ack.to_ne_bytes().to_vec(),
                    }));
                    self.num_ack += 1;
                    commands.push(Command::RouterCommand(RouterCommand::Publish {
                        to: PeerId,
                        payload: self.num_ack.to_ne_bytes().to_vec(),
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

    struct LongAckCodec;
    impl Codec for LongAckCodec {
        type Event = LongAckEvent;
        type ParseError = LongAckParseError;

        fn parse_peer_payload(
            _from: PeerId,
            _payload: &[u8],
        ) -> Result<Self::Event, Self::ParseError> {
            unimplemented!()
        }
    }

    fn simulate_peer<C: Codec>(
        executor: &mut MockExecutor<C>,
        message_delays: &mut impl Iterator<Item = Duration>,
    ) {
        while let Some((to, outbound_message)) = executor.receive_message() {
            // ack all messages that executor wants to send out
            executor.send_ack(
                executor.tick() + message_delays.next().unwrap(),
                to,
                outbound_message,
            )
        }
        while executor.receive_ack().is_some() {}
    }

    fn run_simulation<S, C>(
        init_events: Vec<S::Event>,
        mut message_delays: impl Iterator<Item = Duration>,
        terminate: impl Fn(&S) -> bool,
    ) -> (S, Vec<S::Event>)
    where
        S: State,
        C: Codec<Event = S::Event>,
    {
        let mut executor: MockExecutor<C> = MockExecutor::new();

        let (mut state, mut init_commands) = S::init();
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
            while let Some((to, outbound_message)) = executor.receive_message() {
                // ack all messages that executor wants to send out
                executor.send_ack(
                    executor.tick() + message_delays.next().unwrap(),
                    to,
                    outbound_message,
                )
            }
            while executor.receive_ack().is_some() {}
        }

        (state, event_log)
    }

    #[test]
    fn test_100_milli_ack() {
        let (state, events) = run_simulation::<LongAckState, LongAckCodec>(
            Vec::new(),
            std::iter::repeat(Duration::from_millis(100)),
            |state| state.num_ack == 1000,
        );
        assert_eq!(state.num_timeouts, 0);

        let mut replay_state = LongAckState::init().0;
        for event in events {
            replay_state.update(event);
        }
        assert_eq!(state, replay_state);
    }

    #[test]
    fn test_1000_milli_ack() {
        let (state, events) = run_simulation::<LongAckState, LongAckCodec>(
            Vec::new(),
            std::iter::repeat(Duration::from_millis(1_001)),
            |state| state.num_ack == 1000,
        );
        assert_eq!(state.num_timeouts, 1_000);

        let mut replay_state = LongAckState::init().0;
        for event in events {
            replay_state.update(event);
        }
        assert_eq!(state, replay_state);
    }

    #[test]
    fn test_half_long_ack() {
        let (state, events) = run_simulation::<LongAckState, LongAckCodec>(
            Vec::new(),
            (0..).map(|i| (i % 2) * Duration::from_millis(1_001)),
            |state| state.num_ack == 1000,
        );
        assert_eq!(state.num_timeouts, 500);

        let mut replay_state = LongAckState::init().0;
        for event in events {
            replay_state.update(event);
        }
        assert_eq!(state, replay_state);
    }

    #[test]
    fn test_crash() {
        // send 500 messages
        let (_, init_events) = run_simulation::<LongAckState, LongAckCodec>(
            Vec::new(),
            // send back 500 acks MAX
            std::iter::repeat(Duration::from_millis(1_001)).take(500),
            |state| state.num_ack == 500,
        );
        // replay those 500 messages, send another 500 messages
        let (state, events) = run_simulation::<LongAckState, LongAckCodec>(
            init_events,
            // send back 500 acks MAX
            std::iter::repeat(Duration::from_millis(1_001)).take(500),
            |state| state.num_ack == 1_000,
        );
        // total should be 1_000
        assert_eq!(state.num_timeouts, 1_000);

        let mut replay_state = LongAckState::init().0;
        for event in events {
            replay_state.update(event);
        }
        // replaying all 1_000 messages at once should match
        assert_eq!(state, replay_state);
    }
}
