use std::collections::{HashMap, VecDeque};

use monad_executor::{PeerId, RouterTarget};

pub enum GossipEvent {
    /// Send gossip_message to peer
    Send(PeerId, Vec<u8>), // send gossip_message

    /// Emit app_message to executor (NOTE: not gossip_message)
    Emit(PeerId, Vec<u8>),
}

/// Gossip converts:
/// - outbound application messages to outbound gossip messages (tag whatever necessary metadata)
/// - inbound gossip messages to inbound application messages + outbound gossip messages
///
/// NOTE that this must gracefully handle outbound application to self (should immediately Emit, not Send)
///
/// `message` and `gossip_message` are both typed as bytes intentionally, because that's the atomic
/// unit of transfer.
pub trait Gossip {
    type Config;

    fn new(config: Self::Config) -> Self;

    fn send(&mut self, to: RouterTarget, message: &[u8]);
    fn handle_gossip_message(&mut self, from: PeerId, gossip_message: &[u8]);
    fn poll(&mut self) -> Option<GossipEvent>;
}

pub struct MockGossipConfig {
    pub all_peers: Vec<PeerId>,
}

pub struct MockGossip {
    config: MockGossipConfig,

    read_buffers: HashMap<PeerId, VecDeque<u8>>,
    events: VecDeque<GossipEvent>,
}

type MessageLenType = u32;
const MESSAGE_HEADER_LEN: usize = std::mem::size_of::<MessageLenType>();

impl Gossip for MockGossip {
    type Config = MockGossipConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,

            read_buffers: Default::default(),
            events: VecDeque::default(),
        }
    }

    fn send(&mut self, to: RouterTarget, message: &[u8]) {
        let mut gossip_message = Vec::from((message.len() as MessageLenType).to_le_bytes());
        gossip_message.extend_from_slice(message);
        match to {
            RouterTarget::Broadcast => {
                for peer in &self.config.all_peers {
                    self.events
                        .push_back(GossipEvent::Send(*peer, gossip_message.clone()))
                }
            }
            RouterTarget::PointToPoint(to) => {
                self.events.push_back(GossipEvent::Send(to, gossip_message))
            }
        }
    }

    fn handle_gossip_message(&mut self, from: PeerId, gossip_message: &[u8]) {
        let read_buffer = self.read_buffers.entry(from).or_default();
        read_buffer.extend(gossip_message.iter());
        while {
            let buffer_len = read_buffer.len();
            if buffer_len < MESSAGE_HEADER_LEN {
                false
            } else {
                let message_len = MessageLenType::from_le_bytes(
                    read_buffer
                        .iter()
                        .copied()
                        .take(MESSAGE_HEADER_LEN)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                );
                buffer_len >= MESSAGE_HEADER_LEN + message_len as usize
            }
        } {
            let message_len = MessageLenType::from_le_bytes(
                read_buffer
                    .drain(..MESSAGE_HEADER_LEN)
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            ) as usize;
            let emit: Vec<u8> = read_buffer.drain(..message_len).collect();
            self.events.push_back(GossipEvent::Emit(from, emit))
        }
    }

    fn poll(&mut self) -> Option<GossipEvent> {
        self.events.pop_front()
    }
}
