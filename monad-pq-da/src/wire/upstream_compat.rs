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

// The wire format is monad-mcp-da's, byte for byte, in both directions.

use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use monad_mcp_chorus::spec::{Deserializable, ProposalHeader as _, Serializable};
use monad_mcp_da::stub as upstream;

use super::{
    super::{
        chunk::{ChunkRequest, ChunkRequestType, ChunksSubset},
        runtime::{ChunkRecoveryRequest, EpochHandle},
        test_util::{MESSAGE_LEN, SLOT, author, epoch_handle_for},
    },
    proposal::AssembledProposal,
    read_envelope, write_envelope,
};

fn upstream_handle(handle: &EpochHandle) -> upstream::EpochHandle {
    upstream::EpochHandle {
        self_id: handle.self_id,
        key_pair: Arc::new(upstream::ProposalKeyPair::dummy(handle.self_id)),
        header_auth: handle.header_auth.clone(),
        validator_data: handle.validator_data.clone(),
    }
}

#[test]
fn proposal_packets_are_byte_identical_to_upstream() {
    let handle = epoch_handle_for(author(), 4, vec![author()]);
    let upstream_handle = upstream_handle(&handle);
    let message = vec![7u8; MESSAGE_LEN];

    let ours = [
        AssembledProposal::build_d25(&handle, SLOT, &message, 0),
        AssembledProposal::build_s11(&handle, SLOT, 0, &message, 0),
    ];
    let theirs = [
        upstream::AssembledProposal::build_d25(&upstream_handle, SLOT, &message, 0),
        upstream::AssembledProposal::build_s11(&upstream_handle, SLOT, 0, &message, 0),
    ];
    for (ours, theirs) in ours.into_iter().zip(theirs) {
        let (ours, theirs) = (ours.expect("fits"), theirs.expect("fits"));
        let theirs = theirs.disseminate().into_packets();
        assert_eq!(ours.disseminate().into_packets(), theirs);

        // upstream packets read here and written back unchanged, and the reverse
        for (_, packet) in theirs {
            let envelope = read_envelope(packet.clone()).expect("well-formed");
            let rewritten = write_envelope(&envelope);
            assert_eq!(rewritten, vec![packet.clone()]);

            let theirs = upstream::read_envelope(packet.clone()).expect("well-formed");
            assert_eq!(upstream::write_envelope(&theirs), rewritten);
        }
    }
}

#[test]
fn chunk_requests_are_byte_identical_to_upstream() {
    let handle = epoch_handle_for(author(), 4, vec![author()]);
    let proposal = AssembledProposal::build_d25(&handle, SLOT, &[1u8; MESSAGE_LEN], 0);
    let pid = proposal.expect("the message fits").header().pid();

    let subsets = [
        ChunksSubset::All,
        ChunksSubset::HeaderOnly,
        ChunksSubset::Narrowed(BTreeSet::from([0, 4, 300])),
    ];
    for subset in subsets {
        for kind in [ChunkRequestType::MyChunks, ChunkRequestType::YourChunks] {
            let ours = ChunkRecoveryRequest {
                slot: SLOT,
                proposal_index: 2,
                request: ChunkRequest {
                    pid,
                    kind,
                    subset: subset.clone(),
                },
            };
            let bytes: Bytes = ours.serialize();
            let theirs = upstream::ChunkRecoveryRequest::deserialize(&bytes).expect("valid");
            let rewritten: Bytes = theirs.serialize();
            assert_eq!(rewritten, bytes);
            assert_eq!(ChunkRecoveryRequest::deserialize(&rewritten), Ok(ours));
        }
    }
}
