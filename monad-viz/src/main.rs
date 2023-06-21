use std::collections::HashMap;
use std::{f32::consts::PI, time::Duration};

mod config;
mod graph;

use config::{ConfigEditor, SimConfig};

use graph::NodesSimulation;
use graph::{Graph, NodeEvent};

use iced::widget::canvas::{Frame, Path, Program, Text};
use iced::widget::{Canvas, Row, VerticalSlider};
use iced::{
    executor, keyboard, subscription, Application, Color, Command, Event, Length, Settings, Theme,
    Vector,
};

use monad_consensus::signatures::aggregate_signature::AggregateSignatures;
use monad_crypto::nop::NopSignature;
use monad_executor::mock_swarm::{
    LatencyTransformer, Layer, LayerTransformer, XorLatencyTransformer,
};
use monad_executor::State;
use monad_state::{MonadEvent, MonadState};
use monad_wal::mock::MockWALogger;

type SignatureType = NopSignature;
type SignatureCollectionType = AggregateSignatures<SignatureType>;
type MS = MonadState<SignatureType, SignatureCollectionType>;
type MM = <MS as State>::Message;
type PersistenceLoggerType = MockWALogger<MonadEvent<SignatureType, SignatureCollectionType>>;
type Sim = NodesSimulation<MS, LayerTransformer<MM>, PersistenceLoggerType, SimConfig>;

pub fn main() -> iced::Result {
    Viz::run(Settings::default())
}

struct Viz {
    simulation: Sim,
}

#[derive(Debug, Clone)]
enum Message {
    SetTick(f32),
    AddTick(f32),
    SetConfig(SimConfig),
}

impl Application for Viz {
    type Executor = executor::Default;
    type Flags = ();
    type Message = Message;
    type Theme = Theme;

    fn new(_flags: ()) -> (Self, Command<Self::Message>) {
        let config = SimConfig {
            num_nodes: 7,
            delta: Duration::from_millis(101),
            max_tick: Duration::from_secs_f32(4.0),
            transformer: vec![
                Layer::Latency(LatencyTransformer(Duration::from_millis(100))),
                Layer::XorLatency(XorLatencyTransformer(Duration::from_millis(20))),
            ],
        };
        let simulation = {
            NodesSimulation::<MonadState<SignatureType, SignatureCollectionType>, _, _, _>::new(
                config,
            )
        };

        (Self { simulation }, Command::none())
    }

    fn title(&self) -> String {
        "monviz <3".to_owned()
    }

    fn update(&mut self, message: Self::Message) -> iced::Command<Self::Message> {
        match message {
            Message::SetTick(tick) => {
                self.simulation.set_tick(Duration::from_secs_f32(tick));
            }
            Message::AddTick(delta) => {
                self.simulation.set_tick(Duration::from_secs_f32(
                    (self.simulation.tick().as_secs_f32() + delta).clamp(
                        self.simulation.min_tick().as_secs_f32(),
                        self.simulation.max_tick().as_secs_f32(),
                    ),
                ));
            }
            Message::SetConfig(config) => {
                self.simulation.update_config(config);
            }
        }

        Command::none()
    }

    fn view(&self) -> iced::Element<'_, Self::Message> {
        Row::new()
            .push(
                VerticalSlider::new(
                    self.simulation.min_tick().as_secs_f32()
                        ..=self.simulation.max_tick().as_secs_f32(),
                    self.simulation.tick().as_secs_f32(),
                    Message::SetTick,
                )
                .step(0.001),
            )
            .push(
                Canvas::new(&self.simulation)
                    .width(Length::Fill)
                    .height(Length::Fill),
            )
            .push(ConfigEditor::new(
                self.simulation.config(),
                Message::SetConfig,
            ))
            .into()
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

impl Program<Message> for &Sim {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        theme: &iced::Theme,
        bounds: iced::Rectangle,
        cursor: iced::widget::canvas::Cursor,
    ) -> Vec<iced::widget::canvas::Geometry> {
        let tick = self.tick().as_secs_f32();
        let state = self.state();

        let vw = bounds.width;
        let vh = bounds.height;
        let points = get_circle_points(state.len(), f32::min(vw, vh) / 3.0);

        let mut frame = Frame::new(bounds.size());
        for (idx, (node, (x, y))) in state.iter().zip(&points).enumerate() {
            let circle = Path::circle(frame.center() + Vector::new(*x, *y), 5.0);
            frame.fill(&circle, Color::from_rgb(255.0, 0.0, 0.0));

            let ledger = self.nodes.states().get(node.id).unwrap().0.ledger();
            frame.fill_text(Text {
                content: format!(
                    "node-{} ledger_len={}, last_id={:?}",
                    idx,
                    ledger.get_blocks().len(),
                    ledger.get_blocks().last().map(|block| block.get_id())
                ),
                position: frame.center() + Vector::new(x + 5.0, y - 5.0),
                size: 20.0,
                color: Color::BLACK,
                ..Default::default()
            })
        }

        // for (x1, y1) in &points {
        //     for (x2, y2) in &points {
        //         let line = Path::line(
        //             frame.center() + Vector::new(*x1, *y1),
        //             frame.center() + Vector::new(*x2, *y2),
        //         );
        //         frame.stroke(
        //             &line,
        //             Stroke::default()
        //                 .with_color(Color::from_rgb(0.0, 255.0, 0.0))
        //                 .with_width(1.0),
        //         );
        //     }
        // }

        let node_indices = state
            .iter()
            .enumerate()
            .map(|(idx, peer)| (*peer.id, idx))
            .collect::<HashMap<_, _>>();

        for node in state {
            let rx_peer = node.id;
            for pending_event in node.pending_events {
                match pending_event {
                    NodeEvent::Message {
                        tx_time,
                        rx_time,
                        tx_peer,
                        message,
                    } => {
                        if tx_time == Duration::from_secs_f32(tick) || rx_peer == tx_peer {
                            continue;
                        }
                        let (x1, y1) = points[node_indices[tx_peer]];
                        let (x2, y2) = points[node_indices[rx_peer]];
                        let ratio =
                            (tick - tx_time.as_secs_f32()) / (rx_time - tx_time).as_secs_f32();
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
                    NodeEvent::Ack {
                        tx_time,
                        rx_time,
                        tx_peer,
                        message_id,
                    } => {
                        if tx_time == Duration::from_secs_f32(tick) || rx_peer == tx_peer {
                            continue;
                        }
                        let (x1, y1) = points[node_indices[tx_peer]];
                        let (x2, y2) = points[node_indices[rx_peer]];
                        let ratio =
                            (tick - tx_time.as_secs_f32()) / (rx_time - tx_time).as_secs_f32();
                        let (x, y) = (x1 + (x2 - x1) * ratio, y1 + (y2 - y1) * ratio);

                        let circle = Path::circle(frame.center() + Vector::new(x, y), 4.0);
                        frame.fill(&circle, Color::from_rgb(255.0, 0.0, 255.0));

                        frame.fill_text(Text {
                            content: "ACK".to_owned(),
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
