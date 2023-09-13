mod config;
mod graph;
mod replay_graph;

use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    f32::consts::PI,
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use config::{ConfigEditor, SimConfig};
use graph::{Graph, NodeEvent, NodeState, NodesSimulation, ReplayConfig};
use iced::{
    executor, keyboard, subscription,
    widget::{
        canvas::{Frame, Path, Program, Text},
        Canvas, Row, VerticalSlider,
    },
    Application, Color, Command, Event, Length, Settings, Theme, Vector,
};
use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::{BlockType, FullBlock},
    multi_sig::MultiSig,
    payload::NopStateRoot,
    transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::{MonadEvent, PeerId};
use monad_mock_swarm::{
    mock::{MockMempool, NoSerRouterScheduler, RouterScheduler},
    transformer::{
        GenericTransformer, GenericTransformerPipeline, LatencyTransformer, XorLatencyTransformer,
    },
};
use monad_state::{MonadMessage, MonadState};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::{
    mock::MockWALogger,
    wal::{WALogger, WALoggerConfig},
    PersistenceLogger,
};
use replay_graph::{RepConfig, ReplayNodesSimulation};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type TransactionValidatorType = MockValidator;
type StateRootValidatorType = NopStateRoot;
type NS<'a> = NodeState<
    'a,
    PeerId,
    MS,
    monad_state::MonadMessage<SignatureType, SignatureCollectionType>,
    MonadEvent<SignatureType, SignatureCollectionType>,
>;
type MS = MonadState<
    ConsensusState<SignatureCollectionType, TransactionValidatorType, StateRootValidatorType>,
    SignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;
type MM = <MS as State>::Message;
type ME = MonadEvent<SignatureType, SignatureCollectionType>;
type PersistenceLoggerType =
    MockWALogger<TimedEvent<MonadEvent<SignatureType, SignatureCollectionType>>>;
type Rsc = <NoSerRouterScheduler<MM> as RouterScheduler>::Config;
type Sim = NodesSimulation<
    MS,
    NoSerRouterScheduler<MM>,
    GenericTransformerPipeline<MM>,
    PersistenceLoggerType,
    SimConfig,
    MockMempool<ME>,
    SignatureType,
    SignatureCollectionType,
>;
type ReplaySim = ReplayNodesSimulation<MS, RepConfig, SignatureType, SignatureCollectionType>;

#[derive(Parser, Default)]
struct Arg {
    logdir: Option<PathBuf>,
}

pub fn main() -> iced::Result {
    let cli = Arg::parse();

    Viz::run(Settings::<Arg> {
        flags: cli,
        ..Default::default()
    })
}

pub enum SimType {
    S(Sim),
    RS(ReplaySim),
}

struct Viz {
    simulation: SimType,
}

#[derive(Debug, Clone)]
enum Message {
    SetTick(f32),
    AddTick(f32),
    SetConfig(SimConfig),
}

impl Application for Viz {
    type Executor = executor::Default;
    type Flags = Arg;
    type Message = Message;
    type Theme = Theme;

    fn new(flags: Arg) -> (Self, Command<Self::Message>) {
        if let Some(logdir) = flags.logdir {
            let mut config = RepConfig {
                num_nodes: 4,
                delta: Duration::from_millis(101),
                max_tick: Duration::from_secs_f32(0.0),
            };
            let pubkeys = config.nodes();
            let mut replay_events = BTreeMap::new();
            for (pk, _) in pubkeys.iter() {
                let log_config = WALoggerConfig {
                    file_path: logdir.join(format!("{:?}.log", pk)),
                    sync: false,
                };
                let (_, event_vec) = WALogger::<
                    TimedEvent<MonadEvent<SignatureType, SignatureCollectionType>>,
                >::new(log_config)
                .unwrap();
                replay_events.insert(PeerId(*pk), event_vec.clone());
                config.max_tick = max(
                    config.max_tick,
                    event_vec
                        .iter()
                        .max_by_key(|x| x.timestamp)
                        .unwrap()
                        .timestamp,
                );
            }
            let simulation = {
                ReplayNodesSimulation::<
                    MonadState<
                        ConsensusState<
                            SignatureCollectionType,
                            TransactionValidatorType,
                            StateRootValidatorType,
                        >,
                        SignatureType,
                        SignatureCollectionType,
                        ValidatorSet,
                        SimpleRoundRobin,
                        BlockSyncState,
                    >,
                    _,
                    SignatureType,
                    SignatureCollectionType,
                >::new(config, replay_events)
            };

            (
                Self {
                    simulation: SimType::RS(simulation),
                },
                Command::none(),
            )
        } else {
            let config = SimConfig {
                num_nodes: 4,
                delta: Duration::from_millis(101),
                max_tick: Duration::from_secs_f32(4.0),
                pipeline: vec![
                    GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(100))),
                    GenericTransformer::XorLatency(XorLatencyTransformer(Duration::from_millis(
                        20,
                    ))),
                ],
            };
            let simulation = {
                NodesSimulation::<
                    MonadState<
                        ConsensusState<
                            SignatureCollectionType,
                            TransactionValidatorType,
                            StateRootValidatorType,
                        >,
                        SignatureType,
                        SignatureCollectionType,
                        ValidatorSet,
                        SimpleRoundRobin,
                        BlockSyncState,
                    >,
                    NoSerRouterScheduler<MonadMessage<SignatureType, SignatureCollectionType>>,
                    _,
                    _,
                    _,
                    _,
                    SignatureType,
                    SignatureCollectionType,
                >::new(config)
            };

            (
                Self {
                    simulation: SimType::S(simulation),
                },
                Command::none(),
            )
        }
    }

    fn title(&self) -> String {
        "monviz <3".to_owned()
    }

    fn update(&mut self, message: Self::Message) -> iced::Command<Self::Message> {
        match self.simulation {
            SimType::S(ref mut sim) => match message {
                Message::SetTick(tick) => {
                    sim.set_tick(Duration::from_secs_f32(tick));
                }
                Message::AddTick(delta) => {
                    sim.set_tick(Duration::from_secs_f32(
                        (sim.tick().as_secs_f32() + delta)
                            .clamp(sim.min_tick().as_secs_f32(), sim.max_tick().as_secs_f32()),
                    ));
                }
                Message::SetConfig(config) => {
                    sim.update_config(config);
                }
            },
            SimType::RS(ref mut sim) => match message {
                Message::SetTick(tick) => {
                    sim.set_tick(Duration::from_secs_f32(tick));
                }
                Message::AddTick(delta) => {
                    sim.set_tick(Duration::from_secs_f32(
                        (sim.tick().as_secs_f32() + delta)
                            .clamp(sim.min_tick().as_secs_f32(), sim.max_tick().as_secs_f32()),
                    ));
                }
                _ => {}
            },
        }
        Command::none()
    }

    fn view(&self) -> iced::Element<'_, Self::Message> {
        match self.simulation {
            SimType::S(ref sim) => Row::new()
                .push(
                    VerticalSlider::new(
                        sim.min_tick().as_secs_f32()..=sim.max_tick().as_secs_f32(),
                        sim.tick().as_secs_f32(),
                        Message::SetTick,
                    )
                    .step(0.001),
                )
                .push(Canvas::new(sim).width(Length::Fill).height(Length::Fill))
                .push(ConfigEditor::new(sim.config(), Message::SetConfig))
                .into(),
            SimType::RS(ref sim) => Row::new()
                .push(
                    VerticalSlider::new(
                        sim.min_tick().as_secs_f32()..=sim.max_tick().as_secs_f32(),
                        sim.tick().as_secs_f32(),
                        Message::SetTick,
                    )
                    .step(0.001),
                )
                .push(Canvas::new(sim).width(Length::Fill).height(Length::Fill))
                .into(),
        }
    }

    fn subscription(&self) -> iced::Subscription<Self::Message> {
        subscription::events_with(|event, _status| match event {
            Event::Keyboard(keyboard::Event::KeyPressed {
                key_code:
                    keyboard::KeyCode::Right | keyboard::KeyCode::Space | keyboard::KeyCode::Enter,
                modifiers,
            }) => Some(Message::AddTick(
                0.001 * if modifiers.alt() { 10.0 } else { 1.0 },
            )),
            Event::Keyboard(keyboard::Event::KeyPressed {
                key_code: keyboard::KeyCode::Left,
                modifiers,
            }) => Some(Message::AddTick(
                -0.001 * if modifiers.alt() { 10.0 } else { 1.0 },
            )),
            _ => None,
        })
    }
}

fn draw_ledger(
    frame: &mut Frame,
    ledger: &Vec<FullBlock<MultiSig<NopSignature>>>,
    idx: usize,
    x: &f32,
    y: &f32,
) {
    frame.fill_text(Text {
        content: format!(
            "node-{} ledger_len={}, last_id={:?}",
            idx,
            ledger.len(),
            ledger.last().map(|block| block.get_id())
        ),
        position: frame.center() + Vector::new(x + 5.0, y - 5.0),
        size: 20.0,
        color: Color::BLACK,
        ..Default::default()
    })
}

fn draw_circle(frame: &mut Frame, x: &f32, y: &f32) {
    let circle = Path::circle(frame.center() + Vector::new(*x, *y), 5.0);
    frame.fill(&circle, Color::from_rgb(255.0, 0.0, 0.0));
}

fn draw_msg(frame: &mut Frame, state: &Vec<NS>, tick: f32, points: &[(f32, f32)]) {
    let node_indices = state
        .iter()
        .enumerate()
        .map(|(idx, peer)| (*peer.id, idx))
        .collect::<HashMap<_, _>>();

    for node in state {
        let rx_peer = node.id;
        for pending_event in &node.pending_events {
            match pending_event {
                NodeEvent::Message {
                    tx_time,
                    rx_time,
                    tx_peer,
                    message,
                } => {
                    if *tx_time == Duration::from_secs_f32(tick) || *rx_peer == **tx_peer {
                        continue;
                    }
                    let (x1, y1) = points[node_indices[tx_peer]];
                    let (x2, y2) = points[node_indices[rx_peer]];
                    let ratio =
                        (tick - tx_time.as_secs_f32()) / (*rx_time - *tx_time).as_secs_f32();
                    let (x, y) = (x1 + (x2 - x1) * ratio, y1 + (y2 - y1) * ratio);

                    let circle = Path::circle(frame.center() + Vector::new(x, y), 4.0);
                    frame.fill(&circle, Color::from_rgb(0.0, 0.0, 255.0));

                    frame.fill_text(Text {
                        content: format!("{:?}", message),
                        position: frame.center() + Vector::new(x + 5.0, y - 5.0),
                        size: 20.0,
                        color: Color::BLACK,
                        ..Default::default()
                    })
                }
                NodeEvent::Timer {
                    scheduled_time,
                    trip_time,
                    event,
                } => {
                    // TODO
                }
            };
        }
    }
}

impl Program<Message> for &ReplaySim {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        _theme: &iced::Theme,
        bounds: iced::Rectangle,
        _cursor: iced::widget::canvas::Cursor,
    ) -> Vec<iced::widget::canvas::Geometry> {
        let tick = self.tick().as_secs_f32();
        let state = self.state();

        let vw = bounds.width;
        let vh = bounds.height;
        let points = get_circle_points(state.len(), f32::min(vw, vh) / 3.0);

        let mut frame = Frame::new(bounds.size());
        for (idx, (node, (x, y))) in state.iter().zip(&points).enumerate() {
            let ledger = self
                .nodes
                .replay_nodes_info
                .get(node.id)
                .unwrap()
                .blockchain();
            draw_circle(&mut frame, x, y);
            draw_ledger(&mut frame, ledger, idx, x, y);
        }

        draw_msg(&mut frame, &state, tick, &points);

        vec![frame.into_geometry()]
    }
}

impl Program<Message> for &Sim {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        _theme: &iced::Theme,
        bounds: iced::Rectangle,
        _cursor: iced::widget::canvas::Cursor,
    ) -> Vec<iced::widget::canvas::Geometry> {
        let tick = self.tick().as_secs_f32();
        let state = self.state();

        let vw = bounds.width;
        let vh = bounds.height;
        let points = get_circle_points(state.len(), f32::min(vw, vh) / 3.0);

        let mut frame = Frame::new(bounds.size());
        for (idx, (node, (x, y))) in state.iter().zip(&points).enumerate() {
            let ledger = self
                .nodes
                .states()
                .get(node.id)
                .unwrap()
                .executor
                .ledger()
                .get_blocks();
            draw_circle(&mut frame, x, y);
            draw_ledger(&mut frame, ledger, idx, x, y);
        }

        draw_msg(&mut frame, &state, tick, &points);

        vec![frame.into_geometry()]
    }
}

fn get_circle_points(num: usize, radius: f32) -> Vec<(f32, f32)> {
    (0..num)
        .map(|i| {
            let theta = i as f32 * 2.0 * PI / num as f32;

            (radius * theta.sin(), radius * theta.cos())
        })
        .collect()
}
