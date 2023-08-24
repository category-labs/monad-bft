use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    time::Duration,
};

use futures::StreamExt;
use monad_crypto::secp256k1::PubKey;
use monad_types::{Deserializable, Serializable};
use monad_wal::PersistenceLogger;
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
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
pub enum NextTickEvent {
    Internal { pid: PeerId, tick: Duration },
    External { tick: Duration },
}

pub struct NodesConfig<S, RS, T, LGR>
where
    S: State,
    RS: RouterScheduler,
    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
{
    pub peers: Vec<(PubKey, S::Config, LGR::Config, RS::Config)>,
    pub pipeline: T,
    pub seed: u64,
    pub bias: f64,
}

impl<S, RS, T, LGR> Default for NodesConfig<S, RS, T, LGR>
where
    S: State,
    RS: RouterScheduler,
    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
{
    fn default() -> Self {
        Self {
            peers: vec![],
            pipeline: T::default(),
            seed: 1,
            bias: 1.0,
        }
    }
}
pub struct Nodes<S, RS, T, LGR, ME>
where
    S: State,
    RS: RouterScheduler,
    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    ME: MockableExecutor,
{
    states: BTreeMap<PeerId, (MockExecutor<S, RS, ME>, S, LGR)>,
    pipeline: T,
    scheduled_messages: BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>>,
    rng_gen: ChaChaRng,
    ie_bias: f64,
    pref_ie: bool,
}

impl<S, RS, T, LGR, ME> Nodes<S, RS, T, LGR, ME>
where
    S: State,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    ME: MockableExecutor<Event = S::Event>,

    MockExecutor<S, RS, ME>: Unpin,
    S::Event: Unpin,
    S::Block: Unpin,
{
    pub fn new(config: NodesConfig<S, RS, T, LGR>) -> Self {
        assert!(!config.peers.is_empty());

        let mut states = BTreeMap::new();

        for (pubkey, state_config, logger_config, router_scheduler_config) in config.peers {
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
        let mut gen = ChaChaRng::seed_from_u64(config.seed);
        let ie_bias = config.bias;
        let pref_ie = gen.gen_bool(config.bias);
        Self {
            states,
            pipeline: config.pipeline,
            scheduled_messages: Default::default(),
            rng_gen: gen,
            ie_bias,
            pref_ie,
        }
    }

    pub fn peek_next_tick(&mut self) -> Option<NextTickEvent> {
        let min_event = self
            .states
            .iter_mut()
            .filter_map(|(id, (executor, state, wal))| {
                let tick = executor.peek_event_tick()?;
                Some((id, executor, state, wal, tick))
            })
            .min_by_key(|(_, _, _, _, tick)| *tick);

        let maybe_min_event_tick = min_event.as_ref().map(|(_, _, _, _, tick)| *tick);

        let maybe_min_scheduled_tick = self
            .scheduled_messages
            .peek()
            .map(|Reverse((min_scheduled_tick, _))| *min_scheduled_tick);

        match (maybe_min_event_tick, maybe_min_scheduled_tick) {
            (None, None) => None,
            (None, Some(min_scheduled_tick)) => Some(NextTickEvent::External {
                tick: min_scheduled_tick,
            }),
            (Some(min_event_tick), None) => Some(NextTickEvent::Internal {
                pid: *min_event.unwrap().0,
                tick: min_event_tick,
            }),
            (Some(min_event_tick), Some(min_scheduled_tick)) => {
                if min_event_tick < min_scheduled_tick
                    || (min_event_tick == min_scheduled_tick && self.pref_ie)
                {
                    Some(NextTickEvent::Internal {
                        pid: *min_event.unwrap().0,
                        tick: min_event_tick,
                    })
                } else {
                    Some(NextTickEvent::External {
                        tick: min_scheduled_tick,
                    })
                }
            }
        }
    }

    fn generate_next_preference(&mut self) {
        self.pref_ie = self.rng_gen.gen_bool(self.ie_bias);
    }

    pub fn next_tick(&mut self) -> Option<Duration> {
        match self.peek_next_tick() {
            Some(NextTickEvent::Internal { pid: _, tick }) => Some(tick),
            Some(NextTickEvent::External { tick }) => Some(tick),
            None => None,
        }
    }

    pub fn step(&mut self) -> Option<(Duration, PeerId, S::Event)> {
        loop {
            match self.peek_next_tick() {
                Some(NextTickEvent::Internal { pid, tick }) => {
                    self.generate_next_preference();
                    let (executor, state, wal) = self
                        .states
                        .get_mut(&pid)
                        .expect("logic error, must be nonempty");
                    let executor_event: MockExecutorEvent<
                        <S as State>::Event,
                        <RS as RouterScheduler>::Serialized,
                    > = futures::executor::block_on(executor.next()).unwrap();
                    match executor_event {
                        MockExecutorEvent::Event(event) => {
                            let timed_event = TimedEvent {
                                timestamp: tick,
                                event: event.clone(),
                            };
                            wal.push(&timed_event).unwrap(); // FIXME: propagate the error
                            let node_span = info_span!("node", id = ?pid);
                            let _guard = node_span.enter();
                            let commands = state.update(event.clone());

                            executor.exec(commands);

                            return Some((tick, pid, event));
                        }
                        MockExecutorEvent::Send(to, serialized) => {
                            let lm = LinkMessage {
                                from: pid,
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
                Some(NextTickEvent::External { tick: _ }) => {
                    self.generate_next_preference();
                    let Reverse((scheduled_tick, message)) = self
                        .scheduled_messages
                        .pop()
                        .expect("logic error, must be nonempty");
                    let (executor, _, _) = self.states.get_mut(&message.to).unwrap();
                    executor.send_message(scheduled_tick, message.from, message.message);
                }
                None => break,
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
