use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use monad_executor_glue::{PeerId, RouterTarget};
use monad_proto::{error::ProtoError, proto::gossip::ProtoParentMessage};
use monad_types::{Deserializable, Serializable};
use prost::Message;

use super::{Gossip, GossipError, GossipEvent};

#[cfg(test)]
mod mock;

pub struct Parent<P, B> {
    primary: P,
    backup: B,

    read_buffers: HashMap<PeerId, VecDeque<u8>>,
}

pub trait Primary {
    type Config;
    type GossipMessage: Serializable<Vec<u8>> + Deserializable<[u8], ReadError = ProtoError>;
    fn new(config: Self::Config) -> Self;

    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]);
    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: PeerId,
        gossip_message: Self::GossipMessage,
    );

    fn peek_tick(&self) -> Option<Duration>;
    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::GossipMessage>>;
}

pub trait Backup {
    type Config;
    type MessageId;
    type GossipMessage: Serializable<Vec<u8>> + Deserializable<[u8], ReadError = ProtoError>;

    fn new(config: Self::Config) -> Self;
    fn message_id(message: &[u8]) -> Self::MessageId;

    /// used to tell backup that primary has attempted to publish a message
    fn sent(&mut self, time: Duration, to: RouterTarget, message: &[u8]) -> Self::MessageId;
    fn forget(&mut self, time: Duration, to: RouterTarget, message_id: Self::MessageId);

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: PeerId,
        gossip_message: Self::GossipMessage,
    );
    fn ack_message(&mut self, time: Duration, source: PeerId, message_id: Self::MessageId);

    fn peek_tick(&self) -> Option<Duration>;
    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::GossipMessage>>;
}

enum GossipEventType {
    Primary,
    Backup,
}

pub enum GossipMessage {
    Primary(Vec<u8>),
    Backup(Vec<u8>),
}

type MessageLenType = u32;
const MESSAGE_HEADER_LEN: usize = std::mem::size_of::<MessageLenType>();

impl<P, B> Gossip for Parent<P, B>
where
    P: Primary,
    B: Backup,
{
    type Config = (P::Config, B::Config);
    type MessageId = B::MessageId;

    fn new(config: Self::Config) -> Self {
        let (primary_config, backup_config) = config;
        Self {
            primary: P::new(primary_config),
            backup: B::new(backup_config),
            read_buffers: Default::default(),
        }
    }

    fn message_id(message: &[u8]) -> Self::MessageId {
        B::message_id(message)
    }

    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) -> Self::MessageId {
        self.primary.send(time, to, message);
        self.backup.sent(time, to, message)
    }

    fn forget(&mut self, time: Duration, to: RouterTarget, message_id: Self::MessageId) {
        self.backup.forget(time, to, message_id);
    }

    fn handle_gossip_message(&mut self, time: Duration, from: PeerId, gossip_message: &[u8]) {
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
            let gossip_message: Vec<u8> = read_buffer.drain(..message_len).collect();

            let mut handle_message = || -> Result<(), GossipError> {
                let parent_message = ProtoParentMessage::decode(gossip_message.as_slice())?;
                let message = GossipMessage::try_from(parent_message)?;
                match message {
                    GossipMessage::Primary(primary_message) => {
                        let primary_message = P::GossipMessage::deserialize(&primary_message)?;
                        self.primary
                            .handle_gossip_message(time, from, primary_message);
                    }
                    GossipMessage::Backup(backup_message) => {
                        let backup_message = B::GossipMessage::deserialize(&backup_message)?;
                        self.backup
                            .handle_gossip_message(time, from, backup_message);
                    }
                }
                Ok(())
            };
            if handle_message().is_err() {
                // TODO handle invalid message - should be counted and exposed to validator runners
            }
        }
    }

    fn ack_message(&mut self, time: Duration, source: PeerId, message_id: B::MessageId) {
        self.backup.ack_message(time, source, message_id)
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _)| tick)
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>> {
        self.peek_event().and_then(|(event_tick, event_type)| {
            if event_tick < time {
                None
            } else {
                let event = match event_type {
                    GossipEventType::Primary => {
                        let primary_event = self.primary.poll(time)?;
                        primary_event.map(|event| {
                            let raw_message =
                                ProtoParentMessage::from(GossipMessage::Primary(event.serialize()))
                                    .encode_to_vec();
                            let mut full_message =
                                Vec::from((raw_message.len() as MessageLenType).to_le_bytes());
                            full_message.extend_from_slice(&raw_message);
                            full_message
                        })
                    }
                    GossipEventType::Backup => {
                        let backup_event = self.backup.poll(time)?;
                        backup_event.map(|event| {
                            let raw_message =
                                ProtoParentMessage::from(GossipMessage::Backup(event.serialize()))
                                    .encode_to_vec();
                            let mut full_message =
                                Vec::from((raw_message.len() as MessageLenType).to_le_bytes());
                            full_message.extend_from_slice(&raw_message);
                            full_message
                        })
                    }
                };
                Some(event)
            }
        })
    }
}

impl<P, B> Parent<P, B>
where
    P: Primary,
    B: Backup,
{
    fn peek_event(&self) -> Option<(Duration, GossipEventType)> {
        std::iter::empty()
            .chain(
                self.primary
                    .peek_tick()
                    .map(|tick| (tick, GossipEventType::Primary)),
            )
            .chain(
                self.backup
                    .peek_tick()
                    .map(|tick| (tick, GossipEventType::Backup)),
            )
            .min_by_key(|(tick, _)| *tick)
    }
}
