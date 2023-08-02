use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    time::Duration,
};

use futures::StreamExt;
use monad_crypto::secp256k1::PubKey;
use monad_wal::PersistenceLogger;
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use tracing::info_span;

use crate::{
    executor::mock::{MockExecutor, MockExecutorEvent, RouterScheduler},
    timed_event::TimedEvent,
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

pub trait Transformer<M> {
    #[must_use]
    /// note that the output Duration should be a delay, not an absolute time
    // TODO smallvec? resulting Vec will almost always be len 1
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)>;

    fn boxed(self) -> Box<dyn Transformer<M>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// adds constant latency
#[derive(Clone, Debug)]
pub struct LatencyTransformer(pub Duration);
impl<M> Transformer<M> for LatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        vec![(self.0, message)]
    }
}

/// adds constant latency (parametrizable cap) to each link determined by xor(peer_id_1, peer_id_2)
#[derive(Clone, Debug)]
pub struct XorLatencyTransformer(pub Duration);
impl<M> Transformer<M> for XorLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        let mut ck: u8 = 0;
        for b in message.from.0.bytes() {
            ck ^= b;
        }
        for b in message.to.0.bytes() {
            ck ^= b;
        }
        vec![(self.0.mul_f32(ck as f32 / u8::MAX as f32), message)]
    }
}

/// adds random latency to each message up to a cap
pub struct RandLatencyTransformer {
    gen: ChaChaRng,
    max_latency: u64,
}

impl RandLatencyTransformer {
    pub fn new(seed: u64, max_latency: u64) -> Self {
        RandLatencyTransformer {
            gen: ChaChaRng::seed_from_u64(seed),
            max_latency,
        }
    }

    pub fn next_latency(&mut self) -> Duration {
        let s = self.gen.gen_range(1..self.max_latency);

        Duration::from_millis(s)
    }
}

impl<M> Transformer<M> for RandLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        vec![(self.next_latency(), message)]
    }
}

/// blacklists given nodes
#[derive(Clone)]
pub struct BlacklistTransformer<M> {
    peers: HashSet<PeerId>,

    // TODO once this transformer supports replay, can remove this
    _pd: PhantomData<M>,
}
impl<M> Transformer<M> for BlacklistTransformer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        let mut output = Vec::new();
        if !self.peers.contains(&message.from) && !self.peers.contains(&message.to) {
            output.push((Duration::ZERO, message))
        }
        output
    }
}

impl<M> Debug for BlacklistTransformer<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlacklistTransformer")
            .field("peers", &self.peers)
            .finish()
    }
}

impl<M> Transformer<M> for Vec<Box<dyn Transformer<M>>> {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        self.iter_mut().fold(
            // accumulator is transformed set of messages before/after each layer
            vec![(Duration::ZERO, message)],
            |messages, layer| {
                messages
                    .into_iter()
                    .flat_map(|(base_duration, message)| {
                        // transform each message by applying the layer to each
                        layer
                            .transform(message)
                            .into_iter()
                            .map(move |(duration, message)| (base_duration + duration, message))
                    })
                    .collect()
            },
        )
    }
}

#[derive(Debug, Clone)]
pub enum Layer<M> {
    Latency(LatencyTransformer),
    XorLatency(XorLatencyTransformer),
    Blacklist(BlacklistTransformer<M>),
}

impl<M> Transformer<M> for Layer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        match self {
            Layer::Latency(t) => t.transform(message),
            Layer::XorLatency(t) => t.transform(message),
            Layer::Blacklist(t) => t.transform(message),
        }
    }
}

pub type LayerTransformer<M> = Vec<Layer<M>>;
impl<M> Transformer<M> for Vec<Layer<M>> {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        self.iter_mut().fold(
            // accumulator is transformed set of messages before/after each layer
            vec![(Duration::ZERO, message)],
            |messages, layer| {
                messages
                    .into_iter()
                    .flat_map(|(base_duration, message)| {
                        // transform each message by applying the layer to each
                        layer
                            .transform(message)
                            .into_iter()
                            .map(move |(duration, message)| (base_duration + duration, message))
                    })
                    .collect()
            },
        )
    }
}

pub struct Nodes<S, RS, T, LGR>
where
    S: State,
    RS: RouterScheduler,
    T: Transformer<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
{
    states: BTreeMap<PeerId, (MockExecutor<S, RS>, S, LGR)>,
    transformer: T,
    scheduled_messages: BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>>,
}

impl<S, RS, T, LGR> Nodes<S, RS, T, LGR>
where
    S: State,

    RS: RouterScheduler<M = S::Message>,
    RS::Serialized: Eq,

    T: Transformer<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    MockExecutor<S, RS>: Unpin,
    S::Event: Unpin,
{
    pub fn new(peers: Vec<(PubKey, S::Config, LGR::Config)>, transformer: T) -> Self {
        assert!(!peers.is_empty());

        let mut states = BTreeMap::new();

        let all_peers: BTreeSet<_> = peers.iter().map(|(pubkey, _, _)| PeerId(*pubkey)).collect();
        for (pubkey, state_config, logger_config) in peers {
            let mut executor: MockExecutor<S, RS> =
                MockExecutor::new(RS::new(all_peers.clone(), PeerId(pubkey)));
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
            transformer,
            scheduled_messages: Default::default(),
        }
    }

    pub fn next_tick(&self) -> Option<Duration> {
        let min_event = self
            .states
            .iter()
            .filter_map(|(id, (executor, state, wal))| {
                let tick = executor.peek_event_tick()?;
                Some((id, executor, state, wal, tick))
            })
            .min_by_key(|(_, _, _, _, tick)| *tick);
        let maybe_min_event_tick = min_event
            .as_ref()
            .map(|(_, _, _, _, min_event_tick)| min_event_tick);
        let maybe_min_scheduled_tick = self
            .scheduled_messages
            .peek()
            .map(|Reverse((min_scheduled_tick, _))| min_scheduled_tick);
        maybe_min_event_tick
            .into_iter()
            .chain(maybe_min_scheduled_tick.into_iter())
            .min()
            .copied()
    }

    pub fn step(&mut self) -> Option<(Duration, PeerId, S::Event)> {
        loop {
            let min_event = self
                .states
                .iter_mut()
                .filter_map(|(id, (executor, state, wal))| {
                    let tick = executor.peek_event_tick()?;
                    Some((id, executor, state, wal, tick))
                })
                .min_by_key(|(_, _, _, _, tick)| *tick);
            let maybe_min_event_tick = min_event
                .as_ref()
                .map(|(_, _, _, _, min_event_tick)| min_event_tick);
            let maybe_min_scheduled_tick = self
                .scheduled_messages
                .peek()
                .map(|Reverse((min_scheduled_tick, _))| min_scheduled_tick);
            let poll_event = match (maybe_min_event_tick, maybe_min_scheduled_tick) {
                (None, None) => break,
                (None, Some(_)) => false,
                (Some(_), None) => true,
                (Some(min_event_tick), Some(min_scheduled_tick)) => {
                    min_event_tick <= min_scheduled_tick
                }
            };

            if poll_event {
                let (id, executor, state, wal, tick) =
                    min_event.expect("logic error, must be nonempty");
                let id = *id;
                let executor_event = futures::executor::block_on(executor.next()).unwrap();
                match executor_event {
                    MockExecutorEvent::Event(event) => {
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
                    MockExecutorEvent::Send(to, serialized) => {
                        let lm = LinkMessage {
                            from: id,
                            to,
                            message: serialized,

                            from_tick: tick,
                        };
                        let transformed = self.transformer.transform(lm);

                        for (delay, msg) in transformed {
                            self.scheduled_messages.push(Reverse((tick + delay, msg)));
                        }
                    }
                }
            } else {
                let Reverse((scheduled_tick, message)) = self
                    .scheduled_messages
                    .pop()
                    .expect("logic error, must be nonempty");
                let (executor, _, _) = self.states.get_mut(&message.to).unwrap();
                executor.send_message(scheduled_tick, message.from, message.message);
            }
        }
        None
    }

    pub fn states(&self) -> &BTreeMap<PeerId, (MockExecutor<S, RS>, S, LGR)> {
        &self.states
    }

    pub fn mut_states(&mut self) -> &mut BTreeMap<PeerId, (MockExecutor<S, RS>, S, LGR)> {
        &mut self.states
    }

    pub fn scheduled_messages(
        &self,
    ) -> &BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>> {
        &self.scheduled_messages
    }
}
