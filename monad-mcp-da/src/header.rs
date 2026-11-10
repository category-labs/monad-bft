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

use super::{
    types::{HeaderAuth, ProposerSchedule, SignedProposalHeader, ValidatorData},
    wire,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidProposalHeader {
    SlotOutOfRange,
    // not a canonical scheme signed by a proposer of the slot
    Unauthenticated,
}

// the header check shared by consensus and DA: the scheme is the one
// the proposer must have chosen for the validator set, and a proposer
// of the slot signed the header. The schedule is the same one consensus
// holds, so the two cannot disagree on who proposes where.
pub fn header_auth<S>(schedule: Arc<S>, validator_data: Arc<ValidatorData>) -> HeaderAuth
where
    S: ProposerSchedule + Send + Sync + 'static,
{
    use monad_mcp_cadence::spec::{
        ProposalHeader as _, SignedProposalHeader as _, validator::ValidatorData as _,
    };

    use super::encoding_scheme::is_canonical;
    use crate::spec::{DAProposalHeader as _, DAProposalSignature as _};

    HeaderAuth::new(move |signed: &SignedProposalHeader| {
        // todo: cache the auth result by (ProposalHeader, slot).

        if !is_canonical(signed.scheme(), validator_data.len()) {
            return false;
        }
        let preimage = wire::signed_bytes(&signed.header);
        let Some(author) = signed.sig().recover_author(&preimage) else {
            return false;
        };
        // an unavailable schedule authenticates nothing
        let Ok(proposers) = schedule.proposers_at(signed.slot()) else {
            return false;
        };
        proposers.proposer(signed.lane()) == Some(author)
    })
}

#[cfg(test)]
mod tests {
    use monad_mcp_cadence::spec::proposal::HeaderAuth as _;

    use super::super::{
        runtime::EpochHandle,
        test_util::{SLOT, d33_scheme_at, epoch_handle, proposal_chunks_under, signed_header},
        types::{EncodingScheme, Lane, MerkleRoot, SignedProposalHeader},
    };
    use crate::spec::DAMerkleRoot as _;

    // a d33 proposal of the fixture's message at lane 0, signed by `author_id`
    fn d33_header(epoch_handle: &EpochHandle, author_id: u64) -> SignedProposalHeader {
        let scheme = d33_scheme_at(epoch_handle, SLOT, Lane::new_unchecked(0));
        let (header, _) = proposal_chunks_under(epoch_handle, scheme, author_id, 1);
        header
    }

    #[test]
    fn authentication_needs_a_canonical_scheme_signed_by_a_proposer() {
        let epoch_handle = epoch_handle();
        let auth = &epoch_handle.header_auth;
        let header = d33_header(&epoch_handle, 0);
        assert!(auth.authenticate(&header));

        // signed by a non-proposer
        let other = d33_header(&epoch_handle, 2);
        assert!(!auth.authenticate(&other));

        // the signature no longer covers the header
        let SignedProposalHeader {
            header: mut tampered,
            sig,
        } = header.clone();
        tampered.root = MerkleRoot::from_bytes(&[9; 20]).expect("20 bytes");
        let tampered = SignedProposalHeader {
            header: tampered,
            sig,
        };
        assert!(!auth.authenticate(&tampered));

        // validly signed, but one level deeper than the message needs
        let SignedProposalHeader {
            header: mut deeper, ..
        } = header;
        let EncodingScheme::D33(d33) = &mut deeper.scheme else {
            panic!("the fixture's scheme is d33");
        };
        d33.depth += 1;
        let deeper = signed_header(deeper, 0);
        assert!(!auth.authenticate(&deeper));
    }
}
