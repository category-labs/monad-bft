// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::{HashMap, VecDeque};

use alloy_rlp::{Decodable, Encodable, Header, encode_list, list_length};
use bytes::Bytes;

use super::{
    conductor::Conductor,
    slot::SlotConsensus,
    types::{NodeId, Slot, Timestamp, TimestampDelta, Validated},
};
use crate::spec::{Deserializable, Serializable};

// A driver translates consensus effects into concrete node effects
// that are agnostic of the consensus protocol.
pub trait Driver<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    type WireMsg;

    // translate CadenceEvent into NodeEvent
    fn schedule_alarm(&mut self, at: Timestamp, alarm: C::Alarm);
    fn schedule_slot_deadline(&mut self, slot: Slot, deadline: Timestamp);
    fn schedule_slot_timer(&mut self, delta: TimestampDelta, slot: Slot, timer: S::Timer);
    fn broadcast_slot(&mut self, slot: Slot, message: S::Message);
    fn unicast_slot(&mut self, slot: Slot, to: NodeId, message: S::Message);
    fn broadcast_conductor(&mut self, message: C::Message);
    fn poll_node_event(&mut self) -> Option<NodeEvent<Self::WireMsg>>;

    // translate NodeEvent into CadenceEvent
    fn handle_wake(&mut self, wake: WakeId);
    fn handle_message(&mut self, message: Validated<Self::WireMsg>);
    fn poll_cadence_event(
        &mut self,
    ) -> Option<CadenceEvent<S::Timer, C::Alarm, S::Message, C::Message>>;
}

pub enum CadenceEvent<Timer, Alarm, SMessage, CMessage> {
    SlotTimer(Slot, Timer),
    Alarm(Alarm),
    ConductorMessage(Validated<CMessage>),
    SlotMessage(Validated<(Slot, SMessage)>),
    SlotDeadline(Slot),
}

// An opaque token for a pending timer/alarm
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WakeId(u64);

impl WakeId {
    pub(crate) const FIRST: Self = Self(0);

    pub(crate) fn post_increment(&mut self) -> Self {
        let id = *self;
        self.0 = self.0.wrapping_add(1);
        id
    }
}

pub enum NodeEvent<M> {
    Wake(Timestamp, WakeId),
    WakeAfter(TimestampDelta, WakeId),
    Broadcast(M),
    Unicast { to: NodeId, message: M },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CadenceMessage<SM, CM> {
    Slot(Slot, SM),
    Conductor(CM),
}

// A type alias for quickly get the wire message type for a given
// SlotConsensus and Conductor pair.
pub type CadenceDriverMsg<S, C> = <CadenceDriver<S, C> as Driver<S, C>>::WireMsg;

// A canonical driver implementation for Cadence.
pub struct CadenceDriver<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    inbox: VecDeque<CadenceEvent<S::Timer, C::Alarm, S::Message, C::Message>>,
    outbox: VecDeque<NodeEvent<CadenceMessage<S::Message, C::Message>>>,

    wakes: HashMap<WakeId, PendingWake<S::Timer, C::Alarm>>,
    next_wake: WakeId,
}

enum PendingWake<Timer, Alarm> {
    Deadline(Slot),
    SlotTimer(Slot, Timer),
    Alarm(Alarm),
}

impl<S, C> Default for CadenceDriver<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    fn default() -> Self {
        Self {
            inbox: VecDeque::new(),
            outbox: VecDeque::new(),
            wakes: HashMap::new(),
            next_wake: WakeId::FIRST,
        }
    }
}

impl<S, C> CadenceDriver<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    fn fresh_wake(&mut self) -> WakeId {
        self.next_wake.post_increment()
    }
}

impl<S, C> Driver<S, C> for CadenceDriver<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    type WireMsg = CadenceMessage<S::Message, C::Message>;

    fn schedule_alarm(&mut self, at: Timestamp, alarm: C::Alarm) {
        let id = self.fresh_wake();
        self.wakes.insert(id, PendingWake::Alarm(alarm));
        self.outbox.push_back(NodeEvent::Wake(at, id));
    }

    fn schedule_slot_timer(&mut self, delta: TimestampDelta, slot: Slot, timer: S::Timer) {
        let id = self.fresh_wake();
        self.wakes.insert(id, PendingWake::SlotTimer(slot, timer));
        self.outbox.push_back(NodeEvent::WakeAfter(delta, id));
    }

    fn schedule_slot_deadline(&mut self, slot: Slot, deadline: Timestamp) {
        let id = self.fresh_wake();
        self.wakes.insert(id, PendingWake::Deadline(slot));
        self.outbox.push_back(NodeEvent::Wake(deadline, id));
    }

    fn broadcast_slot(&mut self, slot: Slot, message: S::Message) {
        self.outbox
            .push_back(NodeEvent::Broadcast(CadenceMessage::Slot(slot, message)));
    }

    fn unicast_slot(&mut self, slot: Slot, to: NodeId, message: S::Message) {
        self.outbox.push_back(NodeEvent::Unicast {
            to,
            message: CadenceMessage::Slot(slot, message),
        });
    }

    fn broadcast_conductor(&mut self, message: C::Message) {
        self.outbox
            .push_back(NodeEvent::Broadcast(CadenceMessage::Conductor(message)))
    }

    fn poll_node_event(&mut self) -> Option<NodeEvent<Self::WireMsg>> {
        self.outbox.pop_front()
    }

    fn handle_wake(&mut self, wake: WakeId) {
        if let Some(pending) = self.wakes.remove(&wake) {
            match pending {
                PendingWake::Deadline(slot) => {
                    self.inbox.push_back(CadenceEvent::SlotDeadline(slot));
                }
                PendingWake::Alarm(alarm) => {
                    self.inbox.push_back(CadenceEvent::Alarm(alarm));
                }
                PendingWake::SlotTimer(slot, timer) => {
                    self.inbox.push_back(CadenceEvent::SlotTimer(slot, timer));
                }
            }
        }
    }

    fn handle_message(&mut self, message: Validated<Self::WireMsg>) {
        let (message, author) = message.destructure();
        match message {
            CadenceMessage::Slot(slot, msg) => {
                // safety: message is already validated.
                let validated = Validated::new_unchecked((slot, msg), author);
                self.inbox.push_back(CadenceEvent::SlotMessage(validated));
            }
            CadenceMessage::Conductor(msg) => {
                // safety: message is already validated.
                let validated = Validated::new_unchecked(msg, author);
                self.inbox
                    .push_back(CadenceEvent::ConductorMessage(validated));
            }
        }
    }

    fn poll_cadence_event(
        &mut self,
    ) -> Option<CadenceEvent<S::Timer, C::Alarm, S::Message, C::Message>> {
        self.inbox.pop_front()
    }
}

impl<SM, CM> Encodable for CadenceMessage<SM, CM>
where
    SM: Encodable,
    CM: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::Slot(slot, message) => {
                let fields: [&dyn Encodable; 3] = [&1u8, slot, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Conductor(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Slot(slot, message) => {
                let fields: [&dyn Encodable; 3] = [&1u8, slot, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Conductor(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl<SM, CM> Decodable for CadenceMessage<SM, CM>
where
    SM: Decodable,
    CM: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::Slot(
                <Slot as Decodable>::decode(&mut payload)?,
                <SM as Decodable>::decode(&mut payload)?,
            ),
            2 => Self::Conductor(<CM as Decodable>::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown CadenceMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

impl<SM: Encodable, CM: Encodable> Serializable<Bytes> for CadenceMessage<SM, CM> {
    fn serialize(&self) -> Bytes {
        alloy_rlp::encode(self).into()
    }
}

impl<SM: Decodable, CM: Decodable> Deserializable<Bytes> for CadenceMessage<SM, CM> {
    type ReadError = alloy_rlp::Error;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        alloy_rlp::decode_exact(message)
    }
}

#[cfg(test)]
mod rlp_tests {
    use alloy_rlp::{Decodable, Encodable};

    use super::{
        super::{
            conductor::deadline_agreement::DeadlineAgreementMessage,
            test_utils::assert_roundtrip,
            types::{Timestamp, WindowId},
        },
        *,
    };

    #[test]
    fn cadence_wire_layout_and_byte_traits() {
        type M = CadenceMessage<u64, DeadlineAgreementMessage<Timestamp>>;
        let slot = M::Slot(Slot(7), 42);
        assert_eq!(alloy_rlp::encode(&slot), [0xc3, 1, 7, 42]);
        let conductor = M::Conductor(DeadlineAgreementMessage {
            window: WindowId(3),
            acs_message: Timestamp::from_nanos(42),
        });
        assert_eq!(alloy_rlp::encode(&conductor), [0xc4, 2, 0xc2, 3, 42]);
        for message in [slot, conductor] {
            assert_roundtrip(&message);
            let bytes: bytes::Bytes = message.serialize();
            assert_eq!(M::deserialize(&bytes).unwrap(), message);
            let mut extra = bytes.to_vec();
            extra.push(0);
            assert!(M::deserialize(&bytes::Bytes::from(extra)).is_err());
        }
    }

    fn chorus_message(slot: Slot) -> super::super::slot::chorus::ChorusMessage {
        use super::super::{
            slot::fallback::EnterFallbackVote,
            types::{ValidatorData, VoteMsg, VotePool},
        };
        use crate::spec::KeyPair as _;

        let nodes: Vec<_> = (0..4).map(NodeId::dummy).collect();
        let validators = ValidatorData::new(
            nodes.iter().map(|node| (*node, 1u64.into())).collect(),
            nodes
                .iter()
                .map(|node| (*node, node.keypair().pubkey()))
                .collect(),
        );
        let mut votes = VotePool::new(slot);
        for node in &nodes[..3] {
            votes.add_vote(
                *node,
                VoteMsg::new_signed(slot, EnterFallbackVote, &node.keypair()),
            );
        }
        votes.try_form_strong_qc(&validators).unwrap().into()
    }

    #[test]
    fn concrete_cadence_slot_and_conductor_byte_roundtrips() {
        use super::super::{
            conductor::{MonadConductor, acs::median::MedianAcs},
            slot::chorus::Chorus,
            test_utils::{assert_roundtrip, assert_serialization_roundtrip},
            types::SlotDeadline,
        };
        type Wire = CadenceDriverMsg<Chorus, MonadConductor<MedianAcs<SlotDeadline>>>;

        let slot = Slot(7);
        let message = chorus_message(slot);
        assert_roundtrip(&message);
        assert_serialization_roundtrip(&Wire::Slot(slot, message));

        for nanos in [0, 127, 128, u128::MAX] {
            let message = DeadlineAgreementMessage {
                window: WindowId(3),
                acs_message: Timestamp::from_nanos(nanos),
            };
            assert_roundtrip(&message);
            assert_serialization_roundtrip(&Wire::Conductor(message));
        }
    }

    #[test]
    fn nop_conductor_still_allows_slot_message_serialization() {
        use super::super::{
            conductor::{MonadConductor, acs::nop::NopAcs},
            slot::chorus::Chorus,
            types::SlotDeadline,
        };
        type Wire = CadenceDriverMsg<Chorus, MonadConductor<NopAcs<SlotDeadline>>>;

        let slot = Slot(7);
        let message = chorus_message(slot);
        let wire = Wire::Slot(slot, message.clone());
        let bytes = <Wire as Serializable<bytes::Bytes>>::serialize(&wire);
        assert_eq!(bytes.len(), wire.length());
        let Wire::Slot(decoded_slot, decoded_message) = Wire::deserialize(&bytes).unwrap();
        assert_eq!(decoded_slot, slot);
        assert_eq!(decoded_message, message);

        for end in 0..bytes.len() {
            assert!(Wire::deserialize(&bytes.slice(..end)).is_err());
        }
        let mut extra = bytes.to_vec();
        extra.push(0x42);
        assert!(Wire::deserialize(&bytes::Bytes::from(extra)).is_err());
        // [Conductor, [window]] cannot supply a NoMessage value.
        assert!(Wire::deserialize(&bytes::Bytes::from_static(&[0xc3, 2, 0xc1, 3])).is_err());
    }

    #[test]
    fn dummy_conductor_still_allows_slot_message_serialization() {
        use super::super::{
            conductor::dummy::DummyConductor,
            slot::dummy::{DummySlotConsensus, DummyVote},
            test_utils::assert_roundtrip,
            types::VoteMsg,
        };
        type Wire = CadenceDriverMsg<DummySlotConsensus, DummyConductor>;

        let slot = Slot(7);
        let message = VoteMsg::new_signed(slot, DummyVote, &NodeId::dummy(1).keypair());
        assert_roundtrip(&message);
        let wire = Wire::Slot(slot, message.clone());
        let bytes = <Wire as Serializable<bytes::Bytes>>::serialize(&wire);
        assert_eq!(bytes.len(), wire.length());
        let Wire::Slot(decoded_slot, decoded_message) = Wire::deserialize(&bytes).unwrap();
        assert_eq!(decoded_slot, slot);
        assert_eq!(decoded_message, message);

        for end in 0..bytes.len() {
            assert!(Wire::deserialize(&bytes.slice(..end)).is_err());
        }
        let mut extra = bytes.to_vec();
        extra.push(0x42);
        assert!(Wire::deserialize(&bytes::Bytes::from(extra)).is_err());
        // The conductor tag cannot be followed by a Never value.
        assert!(Wire::deserialize(&bytes::Bytes::from_static(&[0xc2, 2, 0xc0])).is_err());
    }

    #[test]
    fn rejects_bad_tags_and_list_framing() {
        type M = CadenceMessage<u64, u64>;
        for bad in [
            vec![0xc0],
            vec![0xc1, 3],
            vec![0xc1, 0x80],
            vec![0x80],
            vec![0xc4, 1, 7, 42, 0],
            vec![0xc2, 1, 7],
            vec![0xc3, 2, 42, 0],
            vec![0xc3, 0x81, 2, 42],
        ] {
            assert!(M::decode(&mut bad.as_slice()).is_err(), "accepted {bad:x?}");
        }
    }

    #[test]
    fn generic_directions_do_not_require_the_other_codec() {
        struct Out;
        impl Encodable for Out {
            fn encode(&self, out: &mut dyn bytes::BufMut) {
                1u8.encode(out);
            }
        }
        struct In;
        impl Decodable for In {
            fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
                u8::decode(buf)?;
                Ok(Self)
            }
        }
        let bytes = CadenceMessage::<Out, Out>::Slot(Slot(1), Out).serialize();
        assert!(CadenceMessage::<In, In>::deserialize(&bytes).is_ok());
    }
}
