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

//! The certificates the MVBA forms: prepare, commit, and timeout.

use std::collections::HashSet;

use super::{
    super::{
        super::{
            fast::Entry,
            types::{IsVote, ProposalMap, SignatureCollection, Slot, StrongQc, ValidatorData},
        },
        FallbackView,
    },
    messages::{FallbackCommitVote, PrepareVote, TimeoutVote},
};
use crate::spec::{Stake as _, validator::ValidatorData as _, vote::SignatureCollection as _};

/// `prepareQC_{slot, v}`: 2f+1 prepare votes on the same entries.
pub(crate) type PrepareQc = StrongQc<PrepareVote>;

/// `CommitQC`: 2f+1 commit votes on the same entries. This is the transferable
/// commitment proof the fallback path finalizes on.
pub(crate) type FallbackCommitQc = StrongQc<FallbackCommitVote>;

/// `TC_{slot, v}`: 2f+1 timeouts for view `v` from distinct senders.
///
/// Timeouts do not all sign the same bytes -- each signs the view of the
/// prepare certificate it carries -- so the aggregate is one signature
/// collection per distinct claimed view rather than a single one. This is the
/// price of letting timeouts aggregate at all, and it stays cheap because
/// validators converge on few distinct locks.
///
/// The highest prepare certificate among the aggregated timeouts rides along
/// unsigned by the certificate; it is authenticated by its own signatures.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutCertificate {
    pub slot: Slot,
    pub view: FallbackView,
    pub groups: Vec<(TimeoutVote, SignatureCollection)>,

    /// `highPrepQC(J)`: the highest prepare certificate the aggregated
    /// timeouts carried, `None` if none carried one. It pins the lock the next
    /// leader is bound to; the block behind it is not here and is fetched by
    /// block sync when the leader needs it.
    pub high_prep_qc: Option<PrepareQc>,
}

impl TimeoutCertificate {
    /// Whether the certificate is what it claims to be: every group's
    /// signatures verify over the digest that group signed, the distinct
    /// signers hold a supermajority of stake, and the carried prepare
    /// certificate is exactly the highest one the aggregated timeouts claim.
    ///
    /// Pinning the carried certificate to the highest *claim* is what makes
    /// `lock` trustworthy: a leader cannot widen its choice of proposal by
    /// dropping an inconvenient certificate while keeping the timeout that
    /// announced it.
    pub(crate) fn verify(&self, validator_data: &ValidatorData) -> bool {
        let scope = (self.slot, self.view);

        let mut signers = HashSet::new();
        for (vote, sigcol) in &self.groups {
            let data = vote.serialize(&scope);
            let Some(group_signers) = sigcol.verify(&data, validator_data) else {
                return false;
            };
            signers.extend(group_signers);
        }

        let stake = validator_data.sum_stake(signers.iter().copied());
        if stake <= validator_data.total_stake().supermajority_threshold() {
            return false;
        }

        let highest_claim = self
            .groups
            .iter()
            .map(|(vote, _)| vote.high_prep_view)
            .max()
            // view 0 is the "no lock" claim.
            .filter(|view| *view != FallbackView::GENESIS);

        match (highest_claim, &self.high_prep_qc) {
            (None, None) => true,
            (Some(claimed_view), Some(qc)) => {
                qc.scope == (self.slot, claimed_view)
                    // no validator can hold a prepare certificate from a view
                    // later than the one it is abandoning.
                    && claimed_view <= self.view
                    && qc.verify(validator_data)
            }
            // a claimed certificate that is not carried, or one carried
            // without any timeout claiming it.
            _ => false,
        }
    }

    /// `lock(J) = entries(highPrepQC(J))`: the entries view `self.view` may
    /// have locked, which the next leader is bound to extend. `None` when
    /// nothing is locked and the leader may propose any valid metablock.
    pub(crate) fn lock(&self) -> Option<&ProposalMap<Entry>> {
        self.high_prep_qc.as_ref().map(|qc| &qc.verdict.0)
    }
}
