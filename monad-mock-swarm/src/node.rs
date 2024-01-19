use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
    usize,
};

use itertools::Itertools;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_executor::Executor;
use monad_executor_glue::MonadEvent;
use monad_state::MonadStateBuilder;
use monad_transformer::{LinkMessage, Pipeline, ID};
use monad_validator::validator_set::BoxedValidatorSetTypeFactory;
use monad_wal::{PersistenceLogger, PersistenceLoggerBuilder};
use rand::{Rng, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaChaRng};
use tracing::info_span;

use crate::{
    mock::{MockExecutor, MockExecutorEvent},
    mock_swarm::SwarmEventType,
    swarm_relation::{DebugSwarmRelation, SwarmRelation, SwarmRelationStateType},
};

pub struct NodeBuilder<S: SwarmRelation> {
    pub id: ID<CertificateSignaturePubKey<S::SignatureType>>,
    pub state_builder: MonadStateBuilder<
        S::SignatureType,
        S::SignatureCollectionType,
        S::ValidatorSetTypeFactory,
        S::LeaderElection,
        S::TxPool,
        S::BlockValidator,
        S::StateRootValidator,
    >,
    pub logger: S::Logger,
    pub replay_events: Vec<MonadEvent<S::SignatureType, S::SignatureCollectionType>>,
    pub router_scheduler: S::RouterScheduler,
    pub state_root_executor: S::StateRootHashExecutor,
    pub pipeline: S::Pipeline,
    pub seed: u64,
}
impl<S: SwarmRelation> NodeBuilder<S> {
    pub fn new(
        id: ID<CertificateSignaturePubKey<S::SignatureType>>,
        state_builder: MonadStateBuilder<
            S::SignatureType,
            S::SignatureCollectionType,
            S::ValidatorSetTypeFactory,
            S::LeaderElection,
            S::TxPool,
            S::BlockValidator,
            S::StateRootValidator,
        >,
        logger_builder: impl PersistenceLoggerBuilder<PersistenceLogger = S::Logger>,
        router_scheduler: S::RouterScheduler,
        state_root_executor: S::StateRootHashExecutor,
        pipeline: S::Pipeline,
        seed: u64,
    ) -> Self {
        let (logger, replay_events) = logger_builder.build().unwrap();
        Self {
            id,
            state_builder,
            logger,
            replay_events,
            router_scheduler,
            state_root_executor,
            pipeline,
            seed,
        }
    }

    pub fn debug(self) -> NodeBuilder<DebugSwarmRelation>
    where
        S: SwarmRelation<
            SignatureType = <DebugSwarmRelation as SwarmRelation>::SignatureType,
            SignatureCollectionType = <DebugSwarmRelation as SwarmRelation>::SignatureCollectionType,
                TransportMessage = <DebugSwarmRelation as SwarmRelation>::TransportMessage,
        >,

    // FIXME can this be deleted?
        S::RouterScheduler: Sync,
    {
        NodeBuilder {
            id: self.id,
            state_builder: MonadStateBuilder {
                validator_set_factory: BoxedValidatorSetTypeFactory::new(
                    self.state_builder.validator_set_factory,
                ),
                leader_election: Box::new(self.state_builder.leader_election),
                transaction_pool: Box::new(self.state_builder.transaction_pool),
                block_validator: Box::new(self.state_builder.block_validator),
                state_root_validator: Box::new(self.state_builder.state_root_validator),
                validators: self.state_builder.validators,
                key: self.state_builder.key,
                certkey: self.state_builder.certkey,
                val_set_update_interval: self.state_builder.val_set_update_interval,
                epoch_start_delay: self.state_builder.epoch_start_delay,
                beneficiary: self.state_builder.beneficiary,
                consensus_config: self.state_builder.consensus_config,
            },
            logger: Box::new(self.logger),
            replay_events: self.replay_events,
            router_scheduler: Box::new(self.router_scheduler),
            state_root_executor: Box::new(self.state_root_executor),
            pipeline: Box::new(self.pipeline),
            seed: self.seed,
        }
    }
    pub fn build(self, tick: Duration) -> Node<S> {
        let mut executor: MockExecutor<S> =
            MockExecutor::new(self.router_scheduler, self.state_root_executor, tick);
        let (mut state, init_commands) = self.state_builder.build();

        executor.exec(init_commands);

        for event in self.replay_events {
            executor.replay(state.update(event));
        }

        let mut rng = ChaChaRng::seed_from_u64(self.seed);

        Node {
            id: self.id,
            executor,
            state,
            logger: self.logger,
            pipeline: self.pipeline,
            pending_inbound_messages: Default::default(),
            rng: ChaCha20Rng::seed_from_u64(rng.gen()),
            current_seed: rng.gen(),
        }
    }
}

pub struct Node<S>
where
    S: SwarmRelation,
{
    pub id: ID<CertificateSignaturePubKey<S::SignatureType>>,
    pub executor: MockExecutor<S>,
    pub state: SwarmRelationStateType<S>,
    pub logger: S::Logger,
    pub pipeline: S::Pipeline,
    pub pending_inbound_messages: BTreeMap<
        Duration,
        VecDeque<LinkMessage<CertificateSignaturePubKey<S::SignatureType>, S::TransportMessage>>,
    >,
    rng: ChaCha20Rng,
    current_seed: usize,
}

impl<S: SwarmRelation> Node<S> {
    fn update_rng(&mut self) {
        self.current_seed = self.rng.gen();
    }

    pub fn peek_event(&self) -> Option<(Duration, SwarmEventType)> {
        // avoid modification of the original rng
        let events = std::iter::empty()
            .chain(
                self.executor
                    .peek_tick()
                    .iter()
                    .map(|tick| (*tick, SwarmEventType::ExecutorEvent)),
            )
            .chain(self.pending_inbound_messages.first_key_value().map(
                |(min_scheduled_tick, _)| (*min_scheduled_tick, SwarmEventType::ScheduledMessage),
            ))
            .min_set();
        if !events.is_empty() {
            Some(events[self.current_seed % events.len()])
        } else {
            None
        }
    }

    pub fn step_until(
        &mut self,
        until: Duration,
        emitted_messages: &mut Vec<(
            Duration,
            LinkMessage<CertificateSignaturePubKey<S::SignatureType>, S::TransportMessage>,
        )>,
    ) -> Option<(
        Duration,
        MonadEvent<S::SignatureType, S::SignatureCollectionType>,
    )> {
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
                            self.logger.push(&event).unwrap(); // FIXME-4: propagate the error
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

                                // FIXME-3: do we need to transform msg to self?
                                if msg.to == self.id {
                                    self.pending_inbound_messages
                                        .entry(sched_tick)
                                        .or_default()
                                        .push_back(msg)
                                } else {
                                    emitted_messages.push((sched_tick, msg))
                                }
                            }
                            continue;
                        }
                    }
                }
                SwarmEventType::ScheduledMessage => {
                    let mut entry = self
                        .pending_inbound_messages
                        .first_entry()
                        .expect("logic error, should be nonempty");

                    let scheduled_tick = *entry.key();
                    let msgs = entry.get_mut();

                    assert_eq!(tick, scheduled_tick);

                    let message = msgs.pop_front().expect("logic error, should be nonempty");

                    if msgs.is_empty() {
                        entry.remove_entry();
                    }

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
