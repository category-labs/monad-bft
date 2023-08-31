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

pub struct Node<S, RS, P, LGR, ME>
where
    S: State,
    ME: MockableExecutor,
{
    pub executor: MockExecutor<S, RS, ME>,
    pub state: S,
    pub logger: LGR,
    pub pipeline: P,
}

pub struct Nodes<S, RS, P, LGR, ME>
where
    S: State,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    ME: MockableExecutor,
{
    states: BTreeMap<PeerId, Node<S, RS, P, LGR, ME>>,
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
    pub fn new(peers: Vec<(PubKey, S::Config, LGR::Config, RS::Config, P)>) -> Self {
        assert!(!peers.is_empty());

        let mut states = BTreeMap::new();

        for (pubkey, state_config, logger_config, router_scheduler_config, pipeline) in peers {
            let mut executor: MockExecutor<S, RS, ME> =
                MockExecutor::new(RS::new(router_scheduler_config));
            let (wal, replay_events) = LGR::new(logger_config).unwrap();
            let (mut state, mut init_commands) = S::init(state_config);

            for event in replay_events {
                init_commands.extend(state.update(event.event));
            }

            executor.exec(init_commands);

            states.insert(
                PeerId(pubkey),
                Node {
                    executor,
                    state,
                    logger: wal,
                    pipeline,
                },
            );
        }

        Self {
            states,
            scheduled_messages: Default::default(),
        }
    }

    fn peek_event(&self) -> Option<(Duration, SwarmEventType)> {
        let min_event = self
            .states
            .iter()
            .filter_map(|(id, node)| {
                let tick = node.executor.peek_tick()?;
                Some((tick, SwarmEventType::ExecutorEvent(*id)))
            })
            .min_by_key(|(tick, _id)| *tick);
        let min_scheduled =
            self.scheduled_messages
                .peek()
                .map(|Reverse((min_scheduled_tick, _))| {
                    (*min_scheduled_tick, SwarmEventType::ScheduledMessage)
                });
        min_event
            .into_iter()
            .chain(min_scheduled)
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
                    let Node {
                        executor,
                        state,
                        logger,
                        pipeline,
                    } = self
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
                            logger.push(&timed_event).unwrap(); // FIXME: propagate the error
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
                            let transformed = pipeline.process(lm);
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
                    let node = self.states.get_mut(&message.to).unwrap();
                    node.executor
                        .send_message(scheduled_tick, message.from, message.message);
                }
            }
        }

        None
    }

    pub fn states(&self) -> &BTreeMap<PeerId, Node<S, RS, P, LGR, ME>> {
        &self.states
    }

    pub fn scheduled_messages(
        &self,
    ) -> &BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>> {
        &self.scheduled_messages
    }
}
