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

use std::collections::HashSet;

use super::{
    super::{
        super::types::{IsVote, SignatureCollection, Slot, StrongQc, ValidatorData},
        FallbackView, Votable,
    },
    messages::{FallbackCommitVote, PrepareVote, TimeoutVote},
};
use crate::spec::{Stake as _, validator::ValidatorData as _, vote::SignatureCollection as _};

/// `prepareQC_{slot, v}`: 2f+1 prepare votes on the same entries
pub(crate) type PrepareQc<V> = StrongQc<PrepareVote<V>>;

/// `CommitQC`: 2f+1 commit votes on the same entries
pub type FallbackCommitQc<V> = StrongQc<FallbackCommitVote<V>>;

/// `TC_{slot, v}`: 2f+1 timeouts for view `v` from distinct senders
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutCertificate<V: Votable> {
    pub slot: Slot,
    pub view: FallbackView,
    // exposing raw signature collection for BLS multisig optimization
    pub groups: Vec<(TimeoutVote, SignatureCollection)>,
    pub high_prep_qc: Option<PrepareQc<V>>,
}

impl<V: Votable> TimeoutCertificate<V> {
    pub(crate) fn verify(&self, validator_data: &ValidatorData) -> bool {
        let scope = (self.slot, self.view);

        let mut signers = HashSet::new();
        for (vote, sigcol) in &self.groups {
            let data = vote.serialize(&scope);
            let Some(group_signers) = sigcol.verify(&data, validator_data) else {
                return false;
            };
            for signer in group_signers {
                // equivocating signers invalidates the certificate
                if !signers.insert(signer) {
                    return false;
                }
            }
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
            // view 0 is the "no lock" claim
            .filter(|view| *view != FallbackView::GENESIS);

        match (highest_claim, &self.high_prep_qc) {
            (None, None) => true,
            (Some(claimed_view), Some(qc)) => {
                qc.scope == (self.slot, claimed_view)
                    // no validator can hold a prepare certificate from a view
                    // later than the one it is abandoning
                    && claimed_view <= self.view
                    && qc.verify(validator_data)
            }
            _ => false,
        }
    }

    /// `lock(J) = entries(highPrepQC(J))`: what the next leader must extend
    pub(crate) fn lock(&self) -> Option<&V::Entries> {
        self.high_prep_qc.as_ref().map(|qc| &qc.verdict.0)
    }
}
