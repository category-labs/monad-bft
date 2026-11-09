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

// whether a node takes proposals under a hollow scheme. Hollow chunks
// carry no data and no proof: their message exists only in the process
// of the node that encoded it. Only nodes of an in-process simulation
// may accept them; anywhere else a hollow proposal would count as
// available without anyone holding its data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HollowProposals {
    Rejected,
    Accepted,
}

// the header check shared by consensus and DA: the scheme is the one
// the proposer must have chosen for the validator set, the node takes
// proposals under it, and a proposer of the slot signed the header. The
// schedule is the same one consensus holds, so the two cannot disagree
// on who proposes where.
pub fn header_auth<S>(
    schedule: Arc<S>,
    validator_data: Arc<ValidatorData>,
    hollow: HollowProposals,
) -> HeaderAuth
where
    S: ProposerSchedule + Send + Sync + 'static,
{
    use monad_mcp_cadence::spec::{
        ProposalHeader as _, SignedProposalHeader as _, validator::ValidatorData as _,
    };

    use super::{encoding_scheme::DAEncodingScheme as _, types::EncodingScheme};
    use crate::spec::{DAProposalHeader as _, DAProposalSignature as _};

    HeaderAuth::new(move |signed: &SignedProposalHeader| {
        if hollow == HollowProposals::Rejected
            && matches!(signed.scheme(), EncodingScheme::D33Hollow(_))
        {
            return false;
        }
        // todo: cache the auth result by (ProposalHeader, slot).

        if !signed.scheme().is_canonical(validator_data.len()) {
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

    use super::{
        super::{
            runtime::EpochHandle,
            test_util::{
                SLOT, author, d33_scheme_at, epoch_handle, hollow_scheme, proposal_chunks_under,
                proposer_schedule, signed_header,
            },
            types::{EncodingScheme, Lane, MerkleRoot, SignedProposalHeader},
        },
        HollowProposals, header_auth,
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

    #[test]
    fn hollow_proposals_authenticate_only_where_accepted() {
        let epoch_handle = epoch_handle();
        let scheme = hollow_scheme(&epoch_handle);
        let (hollow, _) = proposal_chunks_under(&epoch_handle, scheme, 0, 1);
        let d33 = d33_header(&epoch_handle, 0);

        // the fixture's node, like any node by default, rejects them
        let rejecting = &epoch_handle.header_auth;
        assert!(!rejecting.authenticate(&hollow));
        assert!(rejecting.authenticate(&d33));

        let schedule = proposer_schedule(vec![author()]);
        let validator_data = epoch_handle.validator_data.clone();
        let accepting = header_auth(schedule, validator_data, HollowProposals::Accepted);
        assert!(accepting.authenticate(&hollow));
        assert!(accepting.authenticate(&d33));
    }
}
