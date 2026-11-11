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

//! Encodings shared by the nodes of one process, for simulations.
//!
//! In a simulation every node runs in one process, and every receiver of
//! a proposal repeats work its author already did: it checks each chunk's
//! proof against the root, decodes, and re-encodes to confirm the root.
//! When a node opts in (`EpochHandle::shared_encodings`), the author
//! publishes its encoding and message here, and a receiver of the same
//! process starts from them, as the author's own instance does (see
//! `Chunks::Authored`):
//!
//! - a chunk equal to the published one is held without a check; any
//!   other chunk is checked as before;
//! - the proposal resolves when the receiver's decoder says the chunks it
//!   holds decode, to the published message and encoding, with no decode
//!   and no re-encode.
//!
//! So every receiver's events and outputs are those it would have without
//! the store: a published encoding is one an author built from its
//! message, which every chunk equal to it binds to, and a lookup that
//! finds nothing (the encoding lives in another process, its last holder
//! dropped it, or no author built it, as for a Byzantine proposer's own
//! chunks) leaves the receiver on the real path. Only the work differs,
//! and the receivers share one copy of each encoding.
//!
//! Each process holds one node outside a simulation, so the store then
//! holds that node's own proposals only.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex, MutexGuard, Weak},
};

use bytes::Bytes;

use super::{
    super::types::{EncodingScheme, MerkleRoot, ProposalHeader},
    Chunks, DAEncodingScheme as _, Encoded as _, ProposalChunks,
    d33::D33Encoded,
};

// a published encoding: alive while a node holds it
struct Published {
    encoded: Weak<D33Encoded>,
    message: Bytes,
}

static SHARED: LazyLock<Mutex<HashMap<MerkleRoot, Published>>> = LazyLock::new(Default::default);

fn shared() -> MutexGuard<'static, HashMap<MerkleRoot, Published>> {
    SHARED
        .lock()
        .expect("no thread panics holding the shared encodings")
}

// the author's encoding of `message`, for the receivers of this process.
// The caller must have built `encoded` from `message`.
pub(crate) fn publish(encoded: &Arc<D33Encoded>, message: &Bytes) {
    let mut shared = shared();
    shared.retain(|_, published| published.encoded.strong_count() > 0);
    let published = Published {
        encoded: Arc::downgrade(encoded),
        message: message.clone(),
    };
    shared.insert(encoded.root(), published);
}

fn lookup(root: &MerkleRoot) -> Option<(Arc<D33Encoded>, Bytes)> {
    let shared = shared();
    let published = shared.get(root)?;
    let encoded = published.encoded.upgrade()?;
    Some((encoded, published.message.clone()))
}

// a receiver's chunks of the proposal under `header`, starting from its
// published encoding; None unless one is published
pub(crate) fn known_chunks(
    header: &ProposalHeader,
    num_chunks: usize,
) -> Option<Box<dyn ProposalChunks>> {
    let d33 = match &header.scheme {
        EncodingScheme::D33(d33) => d33,
        // a hollow proposal carries no data to share
        #[cfg(feature = "enable_hollow")]
        EncodingScheme::D33Hollow(_) => return None,
    };
    let (encoded, message) = lookup(&header.root)?;
    let decoder = d33.decoder(header.root, num_chunks);
    Some(Box::new(Chunks::authored(decoder, message, encoded)))
}

#[cfg(test)]
pub(crate) fn is_published(root: &MerkleRoot) -> bool {
    lookup(root).is_some()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        super::super::{
            runtime::{EpochHandle, ProposalScheme},
            test_util::{MESSAGE_LEN, SLOT, author, epoch_handle_for},
            types::{Lane, Proposal},
            wire::proposal::AuthoredProposal,
        },
        *,
    };

    // a proposal of a payload no other test publishes, by an author that
    // shares its encodings or not
    fn authored(payload: u8, shared_encodings: bool) -> AuthoredProposal {
        let handle = EpochHandle {
            shared_encodings,
            ..epoch_handle_for(author(), 4, vec![author()])
        };
        let proposal = Proposal {
            slot: SLOT,
            lane: Lane::new_unchecked(0),
            payload: Bytes::from(vec![payload; MESSAGE_LEN]),
        };
        AuthoredProposal::build(ProposalScheme::D33, &handle, &proposal, 0).expect("fits")
    }

    fn num_chunks(authored: &AuthoredProposal) -> usize {
        authored.envelope().num_chunks()
    }

    #[test]
    fn an_encoding_is_shared_while_a_node_holds_it() {
        let authored = authored(0x51, true);
        let header = authored.header().header.clone();
        let num_chunks = num_chunks(&authored);
        let receiving = known_chunks(&header, num_chunks).expect("published");
        assert!(!receiving.is_resolved());

        // a receiver holds the encoding once the author let go of it
        drop(authored);
        assert!(is_published(&header.root));
        drop(receiving);

        // no node holds it: a receiver now starts from nothing
        assert!(!is_published(&header.root));
        assert!(known_chunks(&header, num_chunks).is_none());
    }

    #[test]
    fn an_author_that_does_not_share_publishes_nothing() {
        let authored = authored(0x52, false);
        let header = &authored.header().header;
        assert!(!is_published(&header.root));
        assert!(known_chunks(header, num_chunks(&authored)).is_none());
    }
}
