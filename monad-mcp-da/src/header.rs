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

use monad_mcp_chorus::spec::validator::ValidatorData as _;

use super::{
    election::ProposerElection,
    encoding_scheme::DAEncodingScheme as _,
    types::{HeaderAuth, ProposalHeader, ValidatorData},
    wire,
};
use crate::spec::DAProposalSignature as _;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidProposalHeader {
    SlotOutOfRange,
    // not a canonical scheme signed by a proposer of the slot
    Unauthenticated,
}

// the header check shared by consensus and DA: the scheme is the one
// the proposer must have chosen for the validator set, and a proposer
// of the slot signed the header.
pub fn header_auth<E>(election: Arc<E>, validator_data: Arc<ValidatorData>) -> HeaderAuth
where
    E: ProposerElection + Send + Sync + 'static,
{
    HeaderAuth::new(move |header: &ProposalHeader, _slot: u64| {
        if !header.scheme.is_canonical(validator_data.len()) {
            return None;
        }
        let signed = wire::signed_bytes(header.slot, &header.scheme, &header.root);
        let author = header.sig.recover_author(&signed)?;
        election.get_index(header.slot, &author)
    })
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::proposal::HeaderAuth as _;

    use super::super::{
        test_util::{SLOT, epoch_handle, proposal_chunks, proposal_chunks_from, signed_header},
        types::{EncodingScheme, MerkleRoot, Slot},
    };
    use crate::spec::DAMerkleRoot as _;

    #[test]
    fn authentication_needs_a_canonical_scheme_signed_by_a_proposer() {
        let epoch_handle = epoch_handle();
        let auth = &epoch_handle.header_auth;
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        assert_eq!(auth.authenticate(&header, SLOT.get()), Some(0));

        // another slot's scope
        assert_eq!(auth.authenticate(&header, Slot(2).get()), None);

        // signed by a non-proposer
        let (other, _) = proposal_chunks_from(&epoch_handle, 2, SLOT, 1);
        assert_eq!(auth.authenticate(&other, SLOT.get()), None);

        // the signature no longer covers the header
        let mut tampered = header.clone();
        tampered.root = MerkleRoot::from_bytes(&[9; 20]).expect("20 bytes");
        assert_eq!(auth.authenticate(&tampered, SLOT.get()), None);

        // validly signed, but one level deeper than the message needs
        let EncodingScheme::D25(mut d25) = header.scheme;
        d25.depth += 1;
        let deeper = signed_header(SLOT, EncodingScheme::D25(d25), header.root, 0);
        assert_eq!(auth.authenticate(&deeper, SLOT.get()), None);
    }
}
