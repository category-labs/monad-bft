use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    marker::PhantomData,
    time::Duration,
};

use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_crypto::secp256k1::PubKey;
use monad_executor::{timed_event::TimedEvent, Executor, State};
use monad_executor_glue::{MonadEvent, PeerId};
use monad_types::{Deserializable, Serializable};
use monad_wal::PersistenceLogger;
use rayon::prelude::*;
use tracing::info_span;

use crate::{
    mock::{MockExecutor, MockExecutorEvent, MockableExecutor, RouterScheduler},
    transformer::{LinkMessage, Pipeline},
};

pub struct Node<S, RS, P, LGR, ME, ST, SCT>
where
    S: State,
    RS: RouterScheduler,
    ME: MockableExecutor,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    pub id: PeerId,
    pub executor: MockExecutor<S, RS, ME, ST, SCT>,
    pub state: S,
    pub logger: LGR,
    pub pipeline: P,
    pub pending_inbound_messages: BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>>,
    _marker: PhantomData<(ST, SCT)>,
}

impl<S, RS, P, LGR, ME, ST, SCT> Node<S, RS, P, LGR, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    ME: MockableExecutor<Event = S::Event>,

    MockExecutor<S, RS, ME, ST, SCT>: Unpin,
    S::Block: Unpin,
{
    fn peek_event(&self) -> Option<(Duration, SwarmEventType)> {
        std::iter::empty()
            .chain(
                self.executor
                    .peek_tick()
                    .iter()
                    .map(|tick| (*tick, SwarmEventType::ExecutorEvent)),
            )
            .chain(self.pending_inbound_messages.peek().iter().map(
                |Reverse((min_scheduled_tick, _))| {
                    (*min_scheduled_tick, SwarmEventType::ScheduledMessage)
                },
            ))
            .min()
    }
    fn step_until(
        &mut self,
        until: Duration,
        emitted_messages: &mut Vec<(Duration, LinkMessage<RS::Serialized>)>,
    ) -> Option<(Duration, S::Event)> {
        while let Some((tick, event_type)) = self.peek_event() {
            if tick > until {
                break;
            }

            let event = match event_type {
                SwarmEventType::ExecutorEvent => {
                    let executor_event = self.executor.step_until(tick);
                    match executor_event {
                        None => continue,
                        Some(MockExecutorEvent::Event(event)) => {
                            let timed_event = TimedEvent {
                                timestamp: tick,
                                event: event.clone(),
                            };
                            self.logger.push(&timed_event).unwrap(); // FIXME: propagate the error
                            let node_span = info_span!("node", id = ?self.id);
                            let _guard = node_span.enter();
                            let commands = self.state.update(event.clone());

                            self.executor.exec(commands);

                            (tick, event)
                        }
                        Some(MockExecutorEvent::Send(to, serialized)) => {
                            let lm = LinkMessage {
                                from: self.id,
                                to,
                                message: serialized,

                                from_tick: tick,
                            };
                            let transformed = self.pipeline.process(lm);
                            for (delay, msg) in transformed {
                                let sched_tick = tick + delay;
                                if msg.to == self.id {
                                    self.pending_inbound_messages
                                        .push(Reverse((sched_tick, msg)))
                                } else {
                                    emitted_messages.push((sched_tick, msg))
                                }
                            }
                            continue;
                        }
                    }
                }
                SwarmEventType::ScheduledMessage => {
                    let Reverse((scheduled_tick, message)) = self
                        .pending_inbound_messages
                        .pop()
                        .expect("logic error, should be nonempty");
                    assert_eq!(tick, scheduled_tick);
                    self.executor
                        .send_message(scheduled_tick, message.from, message.message);
                    continue;
                }
            };
            return Some(event);
        }
        None
    }
}

pub struct Nodes<S, RS, P, LGR, ME, ST, SCT>
where
    S: State,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    ME: MockableExecutor,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    states: BTreeMap<PeerId, Node<S, RS, P, LGR, ME, ST, SCT>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum SwarmEventType {
    ExecutorEvent,
    ScheduledMessage,
}

impl<S, RS, P, LGR, ME, ST, SCT> Nodes<S, RS, P, LGR, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    ME: MockableExecutor<Event = S::Event>,

    MockExecutor<S, RS, ME, ST, SCT>: Unpin,
    S::Block: Unpin,
    Node<S, RS, P, LGR, ME, ST, SCT>: Send,
    RS::Serialized: Send,
{
    pub fn new(peers: Vec<(PubKey, S::Config, LGR::Config, RS::Config, P)>) -> Self {
        assert!(!peers.is_empty());

        let mut states = BTreeMap::new();

        for (pubkey, state_config, logger_config, router_scheduler_config, pipeline) in peers {
            let mut executor: MockExecutor<S, RS, ME, ST, SCT> =
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
                    id: PeerId(pubkey),
                    executor,
                    state,
                    logger: wal,
                    pipeline,
                    pending_inbound_messages: Default::default(),
                    _marker: PhantomData,
                },
            );
        }

        Self { states }
    }

    fn peek_event(&self) -> Option<(Duration, SwarmEventType, PeerId)> {
        self.states
            .iter()
            .filter_map(|(id, node)| {
                node.peek_event()
                    .map(|(tick, event_type)| (tick, event_type, *id))
            })
            .min()
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _, _)| tick)
    }

    pub fn step(&mut self) -> Option<(Duration, PeerId, S::Event)> {
        self.step_until(Duration::MAX, usize::MAX)
    }

    pub fn step_until(
        &mut self,
        until: Duration,
        until_block: usize,
    ) -> Option<(Duration, PeerId, S::Event)> {
        while let Some((tick, _event_type, id)) = self.peek_event() {
            if tick > until
                || self
                    .states()
                    .values()
                    .any(|node| node.executor.ledger().get_blocks().len() > until_block)
            {
                break;
            }

            let node = self
                .states
                .get_mut(&id)
                .expect("logic error, should be nonempty");

            let mut emitted_messages = Vec::new();
            let emitted_event = node.step_until(tick, &mut emitted_messages);

            for (sched_tick, message) in emitted_messages {
                self.states
                    .get_mut(&message.to)
                    .expect("logic error, should be nonempty")
                    .pending_inbound_messages
                    .push(Reverse((sched_tick, message)));
            }

            if let Some((tick, event)) = emitted_event {
                return Some((tick, id, event));
            }
        }

        None
    }

    pub fn batch_step_until(&mut self, until: Duration, until_block: usize) -> Duration {
        let mut end_tick = Duration::from_nanos(0);
        while let Some(tick) = {
            self.peek_event().map(|(min_tick, _, id)| {
                let min_unsafe_tick = min_tick
                    + self
                        .states
                        .get(&id)
                        .expect("must exist")
                        .pipeline
                        .min_external_delay();
                // max safe tick is (min_unsafe_tick - EPSILON)
                min_unsafe_tick - Duration::from_nanos(1)
            })
        } {
            if tick > until
                || self
                    .states()
                    .values()
                    .any(|node| node.executor.ledger().get_blocks().len() > until_block)
            {
                end_tick = tick;
                break;
            }

            let mut emitted_messages = Vec::new();

            emitted_messages.par_extend(self.states.par_iter_mut().flat_map_iter(|(_id, node)| {
                let mut emitted = Vec::new();
                while let Some((_tick, _event)) = node.step_until(tick, &mut emitted) {}
                emitted.into_iter()
            }));

            for (sched_tick, message) in emitted_messages {
                self.states
                    .get_mut(&message.to)
                    .expect("logic error, should be nonempty")
                    .pending_inbound_messages
                    .push(Reverse((sched_tick, message)));
            }
        }
        end_tick
    }

    pub fn states(&self) -> &BTreeMap<PeerId, Node<S, RS, P, LGR, ME, ST, SCT>> {
        &self.states
    }
}
