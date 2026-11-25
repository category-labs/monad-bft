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

use std::sync::Arc;

use alloy_rlp::{Decodable, Encodable};
use monad_mcp_common::{
    spec::{ProposalKeyPair as _, ProposalSignature as _},
    stub::{ProposalKeyPair, ProposalSignature},
};

use super::{
    env::epoch::election::FixedProposerSchedule,
    slot::context::{ChorusConfig, ChorusContext},
    types::{EpochHandle, HeaderAuth, NodeCredential, NodeId, Slot, TimestampDelta, ValidatorData},
};

pub(crate) fn assert_roundtrip<T>(value: &T) -> T
where
    T: Encodable + Decodable + std::fmt::Debug + PartialEq,
{
    let encoded = alloy_rlp::encode(value);
    assert_eq!(value.length(), encoded.len());
    let decoded: T = alloy_rlp::decode_exact(&encoded).unwrap();
    assert_eq!(&decoded, value);
    for end in 0..encoded.len() {
        assert!(
            alloy_rlp::decode_exact::<T>(&encoded[..end]).is_err(),
            "accepted truncated input at {end}"
        );
    }
    let mut stream = encoded;
    stream.push(0x42);
    let mut remaining = stream.as_slice();
    assert_eq!(&T::decode(&mut remaining).unwrap(), value);
    assert_eq!(remaining, &[0x42]);
    assert!(alloy_rlp::decode_exact::<T>(&stream).is_err());
    decoded
}

pub(crate) fn dummy_index(
    node: impl std::borrow::Borrow<super::types::NodeId>,
) -> super::types::ValidatorIndex {
    super::types::ValidatorData::dummy_index(u64::from(*node.borrow()))
}

// the epoch of the dummy validators 0..num_validators, where validator j
// proposes at lane j in every slot
pub(crate) fn epoch_handle(num_validators: u64, num_lanes: u64) -> EpochHandle {
    let proposers = (0..num_lanes).map(NodeId::dummy).collect();
    let election = FixedProposerSchedule::new(proposers);
    EpochHandle::new(ValidatorData::dummy(num_validators), election)
}

pub(crate) fn reject_each_header() -> Arc<HeaderAuth> {
    Arc::new(|_, _, _| false)
}

// the signature of `signer` over no data, for a header whose bytes no test signs
pub(crate) fn dummy_signature(signer: NodeId) -> ProposalSignature {
    ProposalKeyPair::dummy(signer).sign(&[])
}

// a header authenticates when the proposer of its lane made its dummy_signature
pub(crate) fn accept_dummy_signature() -> Arc<HeaderAuth> {
    Arc::new(|header, _, proposer| header.sig.recover_author(&[]).as_ref() == Some(proposer))
}

// what the dummy validator `id` among 4 knows about the slot
pub(crate) fn chorus_context(id: u64, slot: Slot, num_lanes: u64) -> ChorusContext {
    chorus_context_with(id, slot, num_lanes, reject_each_header())
}

pub(crate) fn chorus_context_with(
    id: u64,
    slot: Slot,
    num_lanes: u64,
    header_auth: Arc<HeaderAuth>,
) -> ChorusContext {
    let config = ChorusConfig {
        delta: TimestampDelta::from_millis(100),
        credential: NodeCredential::dummy(id),
        header_auth,
    };
    let epoch_handle = epoch_handle(4, num_lanes);
    config.at_slot(slot, &epoch_handle).expect("in bound")
}
