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

//! The cadence wire message and its byte layout.

use alloy_rlp::{Decodable, Encodable};
pub use monad_mcp_common::{Outbound, Recipients, SlotRecipients};

use super::{conductor::Conductor, slot::SlotConsensus, types::NodeId};

pub type Inbound<M> = monad_mcp_common::Inbound<M, NodeId>;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CadenceMessage<SM, CM> {
    Slot(SM),
    Conductor(CM),
}

// the wire message of a cadence over the given slot consensus and conductor
pub type CadenceWireMsg<S, C> =
    CadenceMessage<<S as SlotConsensus>::Message, <C as Conductor>::Message>;

impl<SM, CM> Encodable for CadenceMessage<SM, CM>
where
    SM: Encodable,
    CM: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::Slot(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Conductor(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Slot(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::Conductor(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
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
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::Slot(<SM as Decodable>::decode(&mut payload)?),
            2 => Self::Conductor(<CM as Decodable>::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown CadenceMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod rlp_tests {
    use alloy_rlp::{Decodable, Encodable};

    use super::{
        super::{
            conductor::{
                CapAdvance, ConductorMessage, deadline_agreement::DeadlineAgreementMessage,
            },
            test_utils::{assert_roundtrip, dummy_index},
            types::{Slot, SlotScoped, Timestamp, WindowId},
        },
        *,
    };

    #[test]
    fn cadence_wire_layout_and_byte_traits() {
        type M = CadenceMessage<u64, DeadlineAgreementMessage<Timestamp>>;
        let slot = M::Slot(42);
        assert_eq!(alloy_rlp::encode(&slot), [0xc2, 1, 42]);
        let conductor = M::Conductor(DeadlineAgreementMessage {
            window: WindowId(3),
            acs_message: Timestamp::from_nanos(42),
        });
        assert_eq!(alloy_rlp::encode(&conductor), [0xc4, 2, 0xc2, 3, 42]);
        let cap_advance = ConductorMessage::<Timestamp>::CapAdvance(CapAdvance { cap: Slot(9) });
        assert_eq!(alloy_rlp::encode(&cap_advance), [0xc3, 2, 0xc1, 9]);
        assert_roundtrip(&cap_advance);
        for message in [slot, conductor] {
            assert_roundtrip(&message);
            let bytes = bytes::Bytes::from(alloy_rlp::encode(&message));
            assert_eq!(alloy_rlp::decode_exact::<M>(&bytes).unwrap(), message);
            let mut extra = bytes.to_vec();
            extra.push(0);
            assert!(alloy_rlp::decode_exact::<M>(&bytes::Bytes::from(extra)).is_err());
        }
    }

    fn chorus_message(slot: Slot) -> super::super::slot::chorus::ChorusMessage {
        use super::super::{
            slot::fallback::EnterFallbackVote,
            types::{ValidatorData, VoteMsg, VotePool},
        };

        let nodes: Vec<_> = (0..4).map(NodeId::dummy).collect();
        let validators = ValidatorData::dummy(4);
        let mut votes = VotePool::new(slot, std::sync::Arc::new(validators));
        for node in &nodes[..3] {
            votes.add_vote(
                dummy_index(node),
                VoteMsg::new_signed(slot, EnterFallbackVote, &node.keypair()),
            );
        }
        votes.tally().strong_qc().unwrap().into()
    }

    #[test]
    fn concrete_cadence_slot_and_conductor_byte_roundtrips() {
        use monad_mcp_common::SlotDeadline;

        use super::super::{
            conductor::{CadenceConductor, acs::median::MedianAcs},
            slot::chorus::Chorus,
            test_utils::assert_roundtrip,
        };
        type Wire = CadenceWireMsg<Chorus, CadenceConductor<MedianAcs<SlotDeadline>>>;

        let slot = Slot(7);
        let message = chorus_message(slot);
        assert_roundtrip(&message);
        assert_roundtrip(&Wire::Slot(message));

        for nanos in [0, 127, 128, u128::MAX] {
            let message = ConductorMessage::DeadlineAgreement(DeadlineAgreementMessage {
                window: WindowId(3),
                acs_message: Timestamp::from_nanos(nanos),
            });
            assert_roundtrip(&message);
            assert_roundtrip(&Wire::Conductor(message));
        }

        for cap in [0, 127, 128, u64::MAX - 1] {
            let message = ConductorMessage::<Timestamp>::CapAdvance(CapAdvance { cap: Slot(cap) });
            assert_roundtrip(&message);
            assert_roundtrip(&Wire::Conductor(message));
        }
    }

    #[test]
    fn nop_conductor_still_allows_slot_message_serialization() {
        use monad_mcp_common::SlotDeadline;

        use super::super::{
            conductor::{CadenceConductor, acs::nop::NopAcs},
            slot::chorus::Chorus,
        };
        type Wire = CadenceWireMsg<Chorus, CadenceConductor<NopAcs<SlotDeadline>>>;

        let slot = Slot(7);
        let message = chorus_message(slot);
        let wire = Wire::Slot(message.clone());
        let bytes = bytes::Bytes::from(alloy_rlp::encode(&wire));
        assert_eq!(bytes.len(), wire.length());
        let Wire::Slot(decoded_message) = alloy_rlp::decode_exact::<Wire>(&bytes).unwrap() else {
            panic!("expected a slot message");
        };
        assert_eq!(decoded_message.slot(), slot);
        assert_eq!(decoded_message, message);

        for end in 0..bytes.len() {
            assert!(alloy_rlp::decode_exact::<Wire>(&bytes.slice(..end)).is_err());
        }
        let mut extra = bytes.to_vec();
        extra.push(0x42);
        assert!(alloy_rlp::decode_exact::<Wire>(&bytes::Bytes::from(extra)).is_err());
        // [Conductor, [DeadlineAgreement, [window]]] cannot supply a NoMessage value.
        assert!(
            alloy_rlp::decode_exact::<Wire>(&bytes::Bytes::from_static(&[
                0xc5, 2, 0xc3, 1, 0xc1, 3
            ]))
            .is_err()
        );
        // The cap-advance variant carries no ACS message, so it still decodes.
        assert!(matches!(
            alloy_rlp::decode_exact::<Wire>(&bytes::Bytes::from_static(&[0xc5, 2, 0xc3, 2, 0xc1, 9])).unwrap(),
            Wire::Conductor(ConductorMessage::CapAdvance(CapAdvance { cap })) if cap == Slot(9)
        ));
    }

    #[test]
    fn dummy_conductor_still_allows_slot_message_serialization() {
        use super::super::{
            conductor::dummy::DummyConductor,
            slot::dummy::{DummySlotConsensus, DummyVote},
            test_utils::assert_roundtrip,
            types::VoteMsg,
        };
        type Wire = CadenceWireMsg<DummySlotConsensus, DummyConductor>;

        let slot = Slot(7);
        let message = VoteMsg::new_signed(slot, DummyVote, &NodeId::dummy(1).keypair());
        assert_roundtrip(&message);
        let wire = Wire::Slot(message.clone());
        let bytes = bytes::Bytes::from(alloy_rlp::encode(&wire));
        assert_eq!(bytes.len(), wire.length());
        let Wire::Slot(decoded_message) = alloy_rlp::decode_exact::<Wire>(&bytes).unwrap();
        assert_eq!(decoded_message.slot(), slot);
        assert_eq!(decoded_message, message);

        for end in 0..bytes.len() {
            assert!(alloy_rlp::decode_exact::<Wire>(&bytes.slice(..end)).is_err());
        }
        let mut extra = bytes.to_vec();
        extra.push(0x42);
        assert!(alloy_rlp::decode_exact::<Wire>(&bytes::Bytes::from(extra)).is_err());
        // The conductor tag cannot be followed by a Never value.
        assert!(
            alloy_rlp::decode_exact::<Wire>(&bytes::Bytes::from_static(&[0xc2, 2, 0xc0])).is_err()
        );
    }

    #[test]
    fn rejects_bad_tags_and_list_framing() {
        type M = CadenceMessage<u64, u64>;
        for bad in [
            vec![0xc0],
            vec![0xc1, 3],
            vec![0xc1, 0x80],
            vec![0x80],
            vec![0xc3, 1, 42, 0],
            vec![0xc1, 1],
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
        let bytes = alloy_rlp::encode(CadenceMessage::<Out, Out>::Slot(Out));
        assert!(alloy_rlp::decode_exact::<CadenceMessage<In, In>>(&bytes).is_ok());
    }
}
