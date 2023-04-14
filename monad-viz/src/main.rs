use std::collections::HashMap;
use std::{f32::consts::PI, time::Duration};

mod graph;

use graph::Graph;
use graph::NodeEvent;
use graph::NodesSimulation;

use macroquad::prelude::*;
use monad_crypto::secp256k1::KeyPair;
use monad_state::{MonadConfig, MonadState};
use monad_testutil::signing::create_keys;

fn window_conf() -> Conf {
    Conf {
        fullscreen: true,
        ..Default::default()
    }
}

#[macroquad::main(window_conf)]
async fn main() {
    let mut simulation = {
        const NUM_NODES: u16 = 5;
        let config_gen = || {
            let keys = create_keys(NUM_NODES as u32);
            let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();

            let state_configs = keys
                .into_iter()
                .zip(std::iter::repeat(pubkeys.clone()))
                .map(|(key, pubkeys)| MonadConfig {
                    key,
                    validators: pubkeys,

                    delta: Duration::from_millis(101),
                })
                .collect::<Vec<_>>();

            pubkeys.into_iter().zip(state_configs).collect()
        };
        NodesSimulation::<MonadState, _, _>::new(
            config_gen,
            |node_1, node_2| {
                let mut ck = 0;
                for b in node_1.0.into_bytes() {
                    ck ^= b;
                }
                for b in node_2.0.into_bytes() {
                    ck ^= b;
                }
                Duration::from_millis(ck as u64 % 100)
            },
            Duration::from_secs(4),
        )
    };
    let mut tick = simulation.min_tick().as_secs_f32();

    loop {
        clear_background(WHITE);

        // Process keys, mouse etc.
        let vw = screen_width();
        let vh = screen_height();

        egui_macroquad::ui(|egui_ctx| {
            egui::Window::new("SimViz").show(egui_ctx, |ui| {
                ui.style_mut().spacing.slider_width = vw * 0.9;
                ui.add(egui::Slider::new(
                    &mut tick,
                    simulation.min_tick().as_secs_f32()..=simulation.max_tick().as_secs_f32(),
                ))
            });
        });

        simulation.set_tick(Duration::from_secs_f32(tick));

        let state = simulation.state();
        let points = get_circle_points(state.len(), f32::min(vw, vh) / 3.0);

        // Draw things before egui
        set_camera(&Camera2D::from_display_rect(Rect {
            x: -vw / 2.0,
            y: -vh / 2.0,

            w: vw,
            h: vh,
        }));

        for (idx, (node, (x, y))) in state.iter().zip(&points).enumerate() {
            draw_circle(*x, *y, 5.0, RED);

            draw_text(
                // format!("{:?}", node.id).as_ref(),
                format!(
                    "node-{} ledger_len={}, last_id={:?}",
                    idx,
                    node.state.ledger().len(),
                    node.state.ledger().last().map(|block| block.get_id())
                )
                .as_ref(),
                x + 5.0,
                y - 5.0,
                20.0,
                BLACK,
            );
        }

        for (x1, y1) in &points {
            for (x2, y2) in &points {
                draw_line(*x1, *y1, *x2, *y2, 1.0, GREEN);
            }
        }

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
                        draw_circle(x, y, 4.0, BLUE);

                        draw_text(
                            format!("{:?}", message).as_ref(),
                            x + 5.0,
                            y - 5.0,
                            20.0,
                            BLACK,
                        );
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
                        draw_circle(x, y, 4.0, PURPLE);

                        draw_text(
                            // format!("{:?}", message_id).as_ref(),
                            "ACK",
                            x + 5.0,
                            y - 5.0,
                            20.0,
                            BLACK,
                        );
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

        egui_macroquad::draw();

        // Draw things after egui

        next_frame().await;
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
