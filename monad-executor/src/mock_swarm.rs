use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    time::Duration,
};

use monad_crypto::secp256k1::PubKey;
use monad_types::{Deserializable, Serializable};
use monad_wal::PersistenceLogger;
use tracing::info_span;

use crate::{
    executor::mock::{MockExecutor, MockExecutorEvent, MockableExecutor, RouterScheduler},
    timed_event::TimedEvent,
    transformer::Pipeline,
    Executor, PeerId, State,
};
#[derive(Clone, PartialEq, Eq)]
pub struct LinkMessage<M> {
    pub from: PeerId,
    pub to: PeerId,
    pub message: M,

    /// absolute time
    pub from_tick: Duration,
}

impl<M: Eq> Ord for LinkMessage<M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.from_tick, self.from, self.to).cmp(&(other.from_tick, other.from, other.to))
    }
}
impl<M> PartialOrd for LinkMessage<M>
where
    Self: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Nodes<S, RS, P, LGR, ME>
where
    S: State,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    ME: MockableExecutor,
{
    states: BTreeMap<PeerId, (MockExecutor<S, RS, ME>, S, LGR)>,
    pipeline: P,
    scheduled_messages: BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>>,
}

enum SwarmEventType {
    ExecutorEvent(PeerId),
    ScheduledMessage,
}

impl<S, RS, P, LGR, ME> Nodes<S, RS, P, LGR, ME>
where
    S: State,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    ME: MockableExecutor<Event = S::Event>,

    MockExecutor<S, RS, ME>: Unpin,
    S::Event: Unpin,
    S::Block: Unpin,
{
    pub fn new(peers: Vec<(PubKey, S::Config, LGR::Config, RS::Config)>, pipeline: P) -> Self {
        assert!(!peers.is_empty());

        let mut states = BTreeMap::new();

        for (pubkey, state_config, logger_config, router_scheduler_config) in peers {
            let mut executor: MockExecutor<S, RS, ME> =
                MockExecutor::new(RS::new(router_scheduler_config));
            let (wal, replay_events) = LGR::new(logger_config).unwrap();
            let (mut state, mut init_commands) = S::init(state_config);

            for event in replay_events {
                init_commands.extend(state.update(event.event));
            }

            executor.exec(init_commands);

            states.insert(PeerId(pubkey), (executor, state, wal));
        }

        Self {
            states,
            pipeline,
            scheduled_messages: Default::default(),
        }
    }

    fn peek_event(&self) -> Option<(Duration, SwarmEventType)> {
        let min_event = self
            .states
            .iter()
            .filter_map(|(id, (executor, state, wal))| {
                let tick = executor.peek_tick()?;
                Some((id, executor, state, wal, tick))
            })
            .min_by_key(|(_, _, _, _, tick)| *tick);
        let maybe_min_event_tick = min_event.as_ref().map(|(id, _, _, _, min_event_tick)| {
            (*min_event_tick, SwarmEventType::ExecutorEvent(**id))
        });
        let maybe_min_scheduled_tick =
            self.scheduled_messages
                .peek()
                .map(|Reverse((min_scheduled_tick, _))| {
                    (*min_scheduled_tick, SwarmEventType::ScheduledMessage)
                });
        maybe_min_event_tick
            .into_iter()
            .chain(maybe_min_scheduled_tick)
            .min_by_key(|(tick, _)| *tick)
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _)| tick)
    }

    pub fn step(&mut self) -> Option<(Duration, PeerId, S::Event)> {
        self.step_until(Duration::MAX)
    }

    pub fn step_until(&mut self, until: Duration) -> Option<(Duration, PeerId, S::Event)> {
        while let Some((tick, event_type)) = self.peek_event() {
            if tick > until {
                break;
            }

            match event_type {
                SwarmEventType::ExecutorEvent(id) => {
                    let (executor, state, wal) = self
                        .states
                        .get_mut(&id)
                        .expect("logic error, should be nonempty");
                    let executor_event = executor.step_until(tick);
                    match executor_event {
                        None => continue,
                        Some(MockExecutorEvent::Event(event)) => {
                            let timed_event = TimedEvent {
                                timestamp: tick,
                                event: event.clone(),
                            };
                            wal.push(&timed_event).unwrap(); // FIXME: propagate the error
                            let node_span = info_span!("node", id = ?id);
                            let _guard = node_span.enter();
                            let commands = state.update(event.clone());

                            executor.exec(commands);

                            return Some((tick, id, event));
                        }
                        Some(MockExecutorEvent::Send(to, serialized)) => {
                            let lm = LinkMessage {
                                from: id,
                                to,
                                message: serialized,

                                from_tick: tick,
                            };
                            let transformed = self.pipeline.process(lm);
                            for (delay, msg) in transformed {
                                self.scheduled_messages.push(Reverse((tick + delay, msg)));
                            }
                        }
                    }
                }
                SwarmEventType::ScheduledMessage => {
                    let Reverse((scheduled_tick, message)) = self
                        .scheduled_messages
                        .pop()
                        .expect("logic error, should be nonempty");
                    assert_eq!(tick, scheduled_tick);
                    let (executor, _, _) = self.states.get_mut(&message.to).unwrap();
                    executor.send_message(scheduled_tick, message.from, message.message);
                }
            }
        }

        None
    }

    pub fn states(&self) -> &BTreeMap<PeerId, (MockExecutor<S, RS, ME>, S, LGR)> {
        &self.states
    }

    pub fn mut_states(&mut self) -> &mut BTreeMap<PeerId, (MockExecutor<S, RS, ME>, S, LGR)> {
        &mut self.states
    }

    pub fn scheduled_messages(
        &self,
    ) -> &BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>> {
        &self.scheduled_messages
    }
}
