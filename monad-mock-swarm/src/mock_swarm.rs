use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    time::Duration,
    usize,
};

use itertools::Itertools;
use monad_executor::{timed_event::TimedEvent, Executor, State};
use monad_wal::PersistenceLogger;
use rand::{Rng, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaChaRng};
use rayon::prelude::*;
use tracing::info_span;

use crate::{
    mock::{MockExecutor, MockExecutorEvent, RouterScheduler},
    swarm_relation::SwarmRelation,
    transformer::{LinkMessage, Pipeline, ID},
};

pub struct Node<S>
where
    S: SwarmRelation,
{
    pub id: ID,
    pub executor: MockExecutor<S::STATE, S::RS, S::ME, S::ST, S::SCT>,
    pub state: S::STATE,
    pub logger: S::LGR,
    pub pipeline: S::P,
    pub pending_inbound_messages: BinaryHeap<Reverse<(Duration, LinkMessage<S::Message>)>>,
    pub rng: ChaCha20Rng,
    pub current_seed: usize,
}

impl<S> Node<S>
where
    S: SwarmRelation,
    MockExecutor<S::STATE, S::RS, S::ME, S::ST, S::SCT>: Unpin,
{
    fn update_rng(&mut self) {
        self.current_seed = self.rng.gen();
    }

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

    fn step_until(
        &mut self,
        until: Duration,
        emitted_messages: &mut Vec<(Duration, LinkMessage<S::Message>)>,
    ) -> Option<(Duration, <S::STATE as State>::Event)> {
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
                                to: ID::new(to),
                                message: serialized,

                                from_tick: tick,
                            };
                            let transformed = self.pipeline.process(lm);
                            for (delay, msg) in transformed {
                                let sched_tick = tick + delay;
                                // FIXME: do we need to transform msg to self?
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
                    self.executor.send_message(
                        scheduled_tick,
                        *message.from.get_peer_id(),
                        message.message,
                    );
                    continue;
                }
            };
            return Some(event);
        }
        None
    }
}

pub trait NodesTerminator<S>
where
    S: SwarmRelation,
{
    fn should_terminate(&self, nodes: &Nodes<S>) -> bool;
}

#[derive(Clone, Copy)]
pub struct UntilTerminator {
    until_tick: Duration,
    until_block: usize,
}

impl Default for UntilTerminator {
    fn default() -> Self {
        Self::new()
    }
}

impl UntilTerminator {
    pub fn new() -> Self {
        UntilTerminator {
            until_tick: Duration::MAX,
            until_block: usize::MAX,
        }
    }

    pub fn until_tick(mut self, tick: Duration) -> Self {
        self.until_tick = tick;
        self
    }

    pub fn until_block(mut self, b_cnt: usize) -> Self {
        self.until_block = b_cnt;
        self
    }
}

impl<S> NodesTerminator<S> for UntilTerminator
where
    S: SwarmRelation,
{
    fn should_terminate(&self, nodes: &Nodes<S>) -> bool {
        nodes.tick > self.until_tick
            || nodes
                .states
                .values()
                .any(|node| node.executor.ledger().get_blocks().len() > self.until_block)
    }
}
// observe and monitor progress of certain nodes until commit progress is achieved for all
pub struct ProgressTerminator {
    // PeerId -> Ledger len
    nodes_monitor: BTreeMap<ID, usize>,
    timeout: Duration,
}

impl ProgressTerminator {
    pub fn new(nodes_monitor: BTreeMap<ID, usize>, timeout: Duration) -> Self {
        ProgressTerminator {
            nodes_monitor,
            timeout,
        }
    }
}

impl<S> NodesTerminator<S> for ProgressTerminator
where
    S: SwarmRelation,
{
    fn should_terminate(&self, nodes: &Nodes<S>) -> bool {
        if nodes.tick > self.timeout {
            panic!(
                "ProgressTerminator timed-out, expecting nodes 
                to reach following progress before timeout: {:?},
                but the actual progress is: {:?}",
                self.nodes_monitor,
                nodes
                    .states
                    .iter()
                    .map(|(id, nodes)| (id, nodes.executor.ledger().get_blocks().len()))
                    .collect::<BTreeMap<_, _>>()
            );
        }

        nodes.states.iter().all(|(peer_id, node)| {
            if let Some(expected_len) = self.nodes_monitor.get(peer_id) {
                node.executor.ledger().get_blocks().len() > *expected_len
            } else {
                true
            }
        })
    }
}

pub struct Nodes<S>
where
    S: SwarmRelation,
{
    states: BTreeMap<ID, Node<S>>,
    tick: Duration,
    must_deliver: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SwarmEventType {
    ExecutorEvent,
    ScheduledMessage,
}

impl<S> Nodes<S>
where
    S: SwarmRelation,
    MockExecutor<S::STATE, S::RS, S::ME, S::ST, S::SCT>: Unpin,
    Node<S>: Send,
{
    pub fn new(
        peers: Vec<(
            ID,
            <S::STATE as State>::Config,
            S::LGRCFG,
            S::RSCFG,
            S::MPCFG,
            S::P,
            u64,
        )>,
    ) -> Self {
        assert!(!peers.is_empty());

        let mut nodes = Self {
            states: BTreeMap::new(),
            tick: Duration::ZERO,
            must_deliver: true,
        };

        for peer in peers {
            nodes.add_state(peer);
        }

        nodes
    }

    pub fn can_fail_deliver(mut self) -> Self {
        self.must_deliver = false;
        self
    }

    fn peek_event(&self) -> Option<(Duration, SwarmEventType, ID)> {
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

    // step until exactly the next event, either internal or external event out of all the nodes.
    pub fn step_until<Terminator: NodesTerminator<S>>(
        &mut self,
        terminator: &Terminator,
    ) -> Option<Duration> {
        while let Some((tick, _event_type, id)) = self.peek_event() {
            if terminator.should_terminate(self) {
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
                let node = self.states.get_mut(&message.to);
                // if message must be delivered, then node must exists
                assert!(!self.must_deliver || node.is_some());
                node.map(|node| {
                    node.pending_inbound_messages
                        .push(Reverse((sched_tick, message)))
                });
            }
            if let Some((tick, _)) = emitted_event {
                return Some(tick);
            }
        }
        None
    }

    pub fn batch_step_until<Terminator: NodesTerminator<S>>(
        &mut self,
        terminator: &Terminator,
    ) -> Option<Duration> {
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
            if terminator.should_terminate(self) {
                return Some(self.tick);
            }

            let mut emitted_messages: Vec<(Duration, LinkMessage<S::Message>)> = Vec::new();

            emitted_messages.par_extend(self.states.par_iter_mut().flat_map_iter(|(_id, node)| {
                let mut emitted = Vec::new();
                while let Some((_tick, _event)) = node.step_until(tick, &mut emitted) {}
                emitted.into_iter()
            }));
            self.tick = tick;

            for (sched_tick, message) in emitted_messages {
                let node = self.states.get_mut(&message.to);
                // if message must be delivered, then node must exists
                assert!(!self.must_deliver || node.is_some());
                node.map(|node| {
                    node.pending_inbound_messages
                        .push(Reverse((sched_tick, message)))
                });
            }
        }
        None
    }

    pub fn states(&self) -> &BTreeMap<ID, Node<S>> {
        &self.states
    }

    pub fn remove_state(&mut self, peer_id: &ID) -> Option<Node<S>> {
        self.states.remove(peer_id)
    }

    pub fn add_state(
        &mut self,
        peer: (
            ID,
            <S::STATE as State>::Config,
            S::LGRCFG,
            S::RSCFG,
            S::MPCFG,
            S::P,
            u64,
        ),
    ) {
        let (
            id,
            state_config,
            logger_config,
            router_scheduler_config,
            mock_mempool_config,
            pipeline,
            seed,
        ) = peer;

        // No duplicate ID insertion should be allowed
        assert!(!self.states.contains_key(&id));

        let mut executor: MockExecutor<S::STATE, S::RS, S::ME, S::ST, S::SCT> = MockExecutor::new(
            S::RS::new(router_scheduler_config),
            mock_mempool_config,
            self.tick,
        );
        let (wal, replay_events) = S::LGR::new(logger_config).unwrap();
        let (mut state, mut init_commands) = S::STATE::init(state_config);

        for event in replay_events {
            init_commands.extend(state.update(event.event));
        }

        executor.exec(init_commands);
        let mut rng = ChaChaRng::seed_from_u64(seed);

        self.states.insert(
            id,
            Node {
                id,
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
