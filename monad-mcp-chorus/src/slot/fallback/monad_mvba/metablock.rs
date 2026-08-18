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
//! per-proposer entries plus the fallback certificate that admits it.
//!
//! Agreement is over the metablock's entries; the certificates are carried
//! only so that a proposed metablock can be checked valid before it is
//! decided, and play no part in the agreement beyond that check.

use super::super::{
    super::{
        fast::{CertifiedEntry, Entry},
        types::{ProposalIndex, ProposalMap, Slot, ValidatorData},
    },
    MVBAInputs,
};

impl MVBAInputs {
    /// `entries(B)`: the embedded entry of each certified entry, in increasing
    /// order of proposer index. The fallback certificate is not part of it.
    ///
    /// Fixing the order makes this a single value that all validators sign
    /// identically; prepare and commit votes range over it rather than over
    /// the metablock, so a fallback decision is comparable with a fast-path
    /// commitment carrying the same entries.
    pub(crate) fn entries(&self) -> ProposalMap<Entry> {
        self.block.as_ref().map(CertifiedEntry::entry)
    }

    /// `Metablock.is_valid()`: the metablock has one entry per proposer, its
    /// fallback certificate verifies for `slot`, and every certified entry
    /// verifies under `slot` and its own proposer index.
    ///
    /// Two deviations from the paper's definition, both forced by the types:
    /// the "one entry per proposer, none missing, extra or duplicated"
    /// requirement is structural here -- `block` is a `TotalProposalMap`
    /// indexed by proposer, so only its size is left to check -- and the fast
    /// metablock case (`fbcert = ⊥`, all entries `FastQC`) is unrepresentable,
    /// since [`MVBAInputs`] always carries a fallback certificate.
    ///
    /// Availability of the proposal behind a positive root follows from a
    /// `FastQc` or `FallbackQc` and is deliberately not checked.
    pub(crate) fn is_valid(
        &self,
        slot: Slot,
        num_proposals: usize,
        validator_data: &ValidatorData,
    ) -> bool {
        if self.block.size() != num_proposals {
            return false;
        }

        if self.enter_fallback_cert.scope != slot
            || !self.enter_fallback_cert.verify(validator_data)
        {
            return false;
        }

        self.block
            .as_ref()
            .into_iter()
            .enumerate()
            .all(|(j, cert)| certified_entry_is_valid(cert, slot, j, validator_data))
    }
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
