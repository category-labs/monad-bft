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

//! The value the MVBA agrees on: a *metablock*, a vector of certified
//! per-proposer entries.
//!
//! Agreement is over the metablock's entries; the per-entry certificates are
//! carried only so that a proposed metablock can be checked valid before it is
//! decided, and play no part in the agreement beyond that check. The fallback
//! certificate is not part of the value at all -- it travels beside it, and is
//! checked here only where a proposal claims one.

use super::super::{
    super::{
        fast::{CertifiedEntry, EnterFallbackCert, Entry},
        types::{ProposalIndex, ProposalMap, Slot, ValidatorData},
    },
    Metablock,
};

/// `Metablock.is_valid()` minus anything a fallback certificate could say: one
/// entry per proposer, and every certified entry bound to this slot and its
/// own proposer index with valid signatures.
///
/// One deviation from the paper's definition, forced by the types: the "one
/// entry per proposer, none missing, extra or duplicated" requirement is
/// structural here -- a [`Metablock`] is a `TotalProposalMap` indexed by
/// proposer, so only its size is left to check.
///
/// Availability of the proposal behind a positive root follows from a `FastQc`
/// or `FallbackQc` and is deliberately not checked.
///
/// This is also what a block retrieved by block sync is checked against, with
/// no fallback certificate involved: the entries are the identity a
/// certificate over them already fixed, and the requester holds a fallback
/// certificate of its own -- an instance only exists once its input carried
/// one -- so a sender forwarding another would add nothing.
pub(crate) fn metablock_is_valid(
    block: &Metablock,
    slot: Slot,
    num_proposals: usize,
    validator_data: &ValidatorData,
) -> bool {
    if block.size() != num_proposals {
        return false;
    }

    block
        .as_ref()
        .into_iter()
        .enumerate()
        .all(|(j, cert)| certified_entry_is_valid(cert, slot, j, validator_data))
}

/// Whether a *proposed* metablock is acceptable, given the fallback
/// certificate carried beside it -- the whole of the paper's
/// `Metablock.is_valid()`, including the `fbcert` arm.
///
/// The certificate is `Option` because `fbcert` can be `⊥`, and this is the
/// only place that decides what that means. A carried certificate must be for
/// this slot and verify; an absent one is the paper's *fast metablock*, whose
/// admission rests on its entries instead of on a certificate.
///
/// FIXME: the fast metablock arm is under-constrained: the paper additionally
/// requires every entry of a `fbcert = ⊥` metablock to be a `FastQC`. Nothing
/// claims a fast metablock yet -- `Chorus` always enters the fallback path
/// with a certificate in hand -- and the check belongs here, on this arm, the
/// day something does.
pub(crate) fn proposed_metablock_is_valid(
    block: &Metablock,
    cert: Option<&EnterFallbackCert>,
    slot: Slot,
    num_proposals: usize,
    validator_data: &ValidatorData,
) -> bool {
    if let Some(cert) = cert
        && (cert.scope != slot || !cert.verify(validator_data))
    {
        return false;
    }

    metablock_is_valid(block, slot, num_proposals, validator_data)
}

/// `entries(B)`: the embedded entry of each certified entry, in increasing
/// order of proposer index. The fallback certificate is not part of it.
///
/// Fixing the order makes this a single value that all validators sign
/// identically; prepare and commit votes range over it rather than over the
/// metablock, so a fallback decision is comparable with a fast-path commitment
/// carrying the same entries.
pub(crate) fn entries_of(block: &Metablock) -> ProposalMap<Entry> {
    block.as_ref().map(CertifiedEntry::entry)
}

/// `CE.verify(slot, j)`: the certificate is bound to this slot and proposer
/// index, and its own signatures verify.
fn certified_entry_is_valid(
    cert: &CertifiedEntry,
    slot: Slot,
    j: ProposalIndex,
    validator_data: &ValidatorData,
) -> bool {
    let bound_to_proposer = match cert {
        CertifiedEntry::FastQc(qc) => qc.scope == (slot, j),
        CertifiedEntry::FallbackQc(qc) => qc.scope == (slot, j),
        // An EquivCert carries no validator votes: the proposer's own two
        // signatures over conflicting roots are the proof. Their binding to
        // (slot, j) sits inside the opaque chunk header, which only
        // `CertifiedEntry::verify` can check.
        CertifiedEntry::EquivCert(_) => true,
    };

    bound_to_proposer && cert.verify(validator_data)
}
