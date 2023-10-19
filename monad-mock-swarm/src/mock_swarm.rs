use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    time::Duration,
};

use itertools::Itertools;
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_crypto::secp256k1::PubKey;
use monad_executor::{timed_event::TimedEvent, Executor, State};
use monad_executor_glue::{MonadEvent, PeerId};
use monad_types::{Deserializable, Serializable};
use monad_wal::PersistenceLogger;
use rand::{Rng, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaChaRng};
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
    ME: MockableExecutor<SignatureCollection = SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    pub id: PeerId,
    pub executor: MockExecutor<S, RS, ME, ST, SCT>,
    pub state: S,
    pub logger: LGR,
    pub pipeline: P,
    pub pending_inbound_messages: BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>>,
    pub rng: ChaCha20Rng,
    pub current_seed: usize,
}

impl<S, RS, P, LGR, ME, ST, SCT> Node<S, RS, P, LGR, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,

    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,
    MockExecutor<S, RS, ME, ST, SCT>: Unpin,
{
    fn peek_event(&self) -> Option<(Duration, SwarmEventType)> {
        // avoid modification of the original rng
        let events = std::iter::empty()
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
            .min_set();
        if !events.is_empty() {
            Some(events[self.current_seed % events.len()])
        } else {
            None
        }
    }

    fn update_rng(&mut self) {
        self.current_seed = self.rng.gen();
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
            // polling event, thus update the rng
            self.update_rng();

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
    ME: MockableExecutor<SignatureCollection = SCT>,
{
    states: BTreeMap<PeerId, Node<S, RS, P, LGR, ME, ST, SCT>>,
    tick: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SwarmEventType {
    ExecutorEvent,
    ScheduledMessage,
}

impl<S, RS, P, LGR, ME, ST, SCT> Nodes<S, RS, P, LGR, ME, ST, SCT>
where
    S: State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,

    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

    MockExecutor<S, RS, ME, ST, SCT>: Unpin,
    Node<S, RS, P, LGR, ME, ST, SCT>: Send,
{
    pub fn new(
        peers: Vec<(
            PubKey,
            S::Config,
            LGR::Config,
            RS::Config,
            ME::Config,
            P,
            u64,
        )>,
    ) -> Self {
        assert!(!peers.is_empty());

        let mut nodes = Self {
            states: BTreeMap::new(),
            tick: Duration::ZERO,
        };

        for peer in peers {
            nodes.add_state(peer);
        }

        nodes
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
            self.tick = tick;

            for (sched_tick, message) in emitted_messages {
                // deliver message if node is still in the swarm
                self.states.get_mut(&message.to).map(|node| {
                    node.pending_inbound_messages
                        .push(Reverse((sched_tick, message)))
                });
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
            self.tick = tick;

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

    pub fn remove_state(&mut self, peer_id: &PeerId) -> Option<Node<S, RS, P, LGR, ME, ST, SCT>> {
        self.states.remove(peer_id)
    }

    pub fn add_state(
        &mut self,
        peer: (
            PubKey,
            S::Config,
            LGR::Config,
            RS::Config,
            ME::Config,
            P,
            u64,
        ),
    ) {
        let (
            pubkey,
            state_config,
            logger_config,
            router_scheduler_config,
            mock_mempool_config,
            pipeline,
            seed,
        ) = peer;
        let mut executor: MockExecutor<S, RS, ME, ST, SCT> = MockExecutor::new(
            RS::new(router_scheduler_config),
            mock_mempool_config,
            self.tick,
        );
        let (wal, replay_events) = LGR::new(logger_config).unwrap();
        let (mut state, mut init_commands) = S::init(state_config);

        for event in replay_events {
            init_commands.extend(state.update(event.event));
        }

        executor.exec(init_commands);
        let mut rng = ChaChaRng::seed_from_u64(seed);

        self.states.insert(
            PeerId(pubkey),
            Node {
                id: PeerId(pubkey),
                executor,
                state,
                logger: wal,
                pipeline,
                pending_inbound_messages: Default::default(),
                rng: ChaCha20Rng::seed_from_u64(rng.gen()),
                current_seed: rng.gen(),
            },
        );
    }
}
