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

use super::{
    types::{NodeId, SignedProposalHeader, ValidatorData},
    wire,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidProposalHeader {
    SlotOutOfRange,
    // not a canonical scheme signed by a proposer of the slot
    Unauthenticated,
}

// The header check that consensus and DA share: the scheme is the one the
// proposer must choose for the validators, and the proposer signed the header.
pub fn validate_header(
    signed: &SignedProposalHeader,
    validators: &ValidatorData,
    proposer: &NodeId,
) -> bool {
    use monad_mcp_cadence::spec::SignedProposalHeader as _;
    use monad_mcp_common::spec::{ProposalSignature as _, ValidatorData as _};

    use super::encoding_scheme::is_canonical;
    use crate::spec::DAProposalHeader as _;

    if !is_canonical(signed.scheme(), validators.len()) {
        return false;
    }
    let preimage = wire::signed_bytes(&signed.header);
    signed.sig().recover_author(&preimage).as_ref() == Some(proposer)
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            runtime::DAContext,
            test_util::{
                SLOT, author, context, d33_scheme_at, proposal_chunks_under, signed_header,
            },
            types::{EncodingScheme, Lane, MerkleRoot, SignedProposalHeader},
        },
        validate_header,
    };
    use crate::spec::DAMerkleRoot as _;

    // a d33 proposal of the fixture's message at lane 0, signed by `author_id`
    fn d33_header(context: &DAContext, author_id: u64) -> SignedProposalHeader {
        let scheme = d33_scheme_at(context, SLOT, Lane::new_unchecked(0));
        let (header, _) = proposal_chunks_under(context, scheme, author_id, 1);
        header
    }

    #[test]
    fn authentication_needs_a_canonical_scheme_signed_by_a_proposer() {
        let context = context();
        let auth = |header: &SignedProposalHeader| {
            validate_header(header, &context.validator_data, &author())
        };
        let header = d33_header(&context, 0);
        assert!(auth(&header));

        // signed by a non-proposer
        let other = d33_header(&context, 2);
        assert!(!auth(&other));

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
        assert!(!auth(&tampered));

        // validly signed, but one level deeper than the message needs
        let SignedProposalHeader {
            header: mut deeper, ..
        } = header;
        let EncodingScheme::D33(d33) = &mut deeper.scheme else {
            panic!("the fixture's scheme is d33");
        };
        d33.depth += 1;
        let deeper = signed_header(deeper, 0);
        assert!(!auth(&deeper));
    }
}
