use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    time::Duration,
};

use futures::StreamExt;
use monad_crypto::secp256k1::PubKey;
use monad_wal::PersistenceLogger;
use tracing::info_span;

use crate::{
    executor::mock::{MockExecutor, MockExecutorEvent, RouterScheduler},
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

pub struct Nodes<S, RS, T, LGR>
where
    S: State,
    RS: RouterScheduler,
    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
{
    states: BTreeMap<PeerId, (MockExecutor<S, RS>, S, LGR)>,
    pipeline: T,
    scheduled_messages: BinaryHeap<Reverse<(Duration, LinkMessage<RS::Serialized>)>>,
}

impl<S, RS, T, LGR> Nodes<S, RS, T, LGR>
where
    S: State,

    RS: RouterScheduler<M = S::Message>,
    RS::Serialized: Eq,

    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

    MockExecutor<S, RS>: Unpin,
    S::Event: Unpin,
{
    pub fn new(peers: Vec<(PubKey, S::Config, LGR::Config)>, pipeline: T) -> Self {
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
            pipeline,
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
            .chain(maybe_min_scheduled_tick)
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
                        let transformed = self.pipeline.process(lm);
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
