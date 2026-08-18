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
        FallbackView, PartialBlock,
    },
    messages::{CommitVote, PrepareVote, TimeoutVote},
    metablock::entries_of,
};
use crate::spec::{Stake as _, validator::ValidatorData as _, vote::SignatureCollection as _};

/// `prepareQC_{slot, v}`: 2f+1 prepare votes on the same entries.
pub(crate) type PrepareQc = StrongQc<PrepareVote>;

/// `CommitQC`: 2f+1 commit votes on the same entries. This is the transferable
/// commitment proof the fallback path finalizes on.
pub(crate) type CommitQc = StrongQc<CommitVote>;

/// `TC_{slot, v}`: 2f+1 timeouts for view `v` from distinct senders.
///
/// Timeouts do not all sign the same bytes -- each signs the view of the
/// prepare certificate it carries -- so the aggregate is one signature
/// collection per distinct claimed view rather than a single one. This is the
/// price of letting timeouts aggregate at all, and it stays cheap because
/// validators converge on few distinct locks.
///
/// The highest prepare certificate among the aggregated timeouts, and the
/// metablock it locks, ride along unsigned by the certificate; both are
/// authenticated by the prepare certificate's own signatures.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutCertificate {
    pub slot: Slot,
    pub view: FallbackView,
    pub groups: Vec<(TimeoutVote, SignatureCollection)>,

    pub high_prepare: Option<HighPrepare>,
}

/// The highest prepare certificate the aggregated timeouts carried, with the
/// metablock it locks if one of them carried that too.
///
/// The block hangs off the certificate rather than sitting beside it, so a
/// block without the certificate that authenticates it cannot be represented.
/// The other way round is normal: a certificate with no block still pins the
/// lock, it just leaves the next leader with nothing it is allowed to propose.
///
/// FIXME: Q: does high_block need to carry full MVBAInputs or only
/// partial_block
///
/// Response: only the partial block is needed. `entries(x)` is read off the
/// certified entries alone -- the fallback certificate is explicitly not part
/// of it -- so the lock, the lock check and this certificate's own consistency
/// check would all be unchanged. The one thing the full metablock buys is that
/// a leader bound by the lock can broadcast it verbatim; with a partial block
/// it would pair the entries with its own `enter_fallback_cert`, which it
/// always holds, since an instance only exists once its input carried one, and
/// which certifies the same statement `⟨fallback, slot⟩`. So this could carry
/// `PartialBlock` and save a supermajority aggregate per timeout and per
/// timeout certificate; it is a wire-format change across `TimeoutMsg`,
/// `TimeoutCertificate` and the leader path, so it is left for you to call.
///
/// // FIXME: only hold partial block. in higher views, proof to enter fallback
/// is not used
///
/// Response: done -- `block` is a [`PartialBlock`] now. The fallback
/// certificate is dropped from what travels, and a locked leader pairs the
/// certified entries with its own, which it necessarily holds since an
/// instance only exists once its input carried one, and which certifies the
/// same statement `⟨fallback, slot⟩`. Nothing in agreement notices: `entries(x)`
/// never included the fallback certificate, so the lock, the lock check and
/// this certificate's own consistency check are unchanged.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct HighPrepare {
    pub qc: PrepareQc,
    // FIXME: Q: when would we have prepareQC but not block?
    //
    // Response: never among honest validators -- only when a sender announces
    // a certificate whose block it does not forward. A prepare certificate is
    // formed in exactly one place, `prepare_qc_formed`, and that runs only at
    // a validator already in `Preparing`, i.e. one that accepted the view's
    // pre-prepare, so it always pairs the certificate with the block it just
    // voted on. The only other way to hold one is to adopt it out of a timeout
    // certificate, and there the block travels alongside. So `None` means the
    // sender signed `high_prep_view = v` in its timeout digest while omitting
    // the block riding with it -- either a Byzantine validator, or a leader
    // stripping the block out of the timeouts it aggregates. Neither is
    // preventable by the types, since the block is unsigned by the timeout.
    //
    // It stays an `Option` rather than being rejected because dropping the
    // certificate along with the absent block would understate `lock(J)`,
    // which is a safety bug. Keeping it costs liveness only: a leader bound by
    // that lock has nothing it is allowed to propose, so the view times out
    // until some honest timeout carries the block.
    pub block: Option<PartialBlock>,
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

        match (highest_claim, &self.high_prepare) {
            (None, None) => true,
            (Some(claimed_view), Some(high)) => {
                high.qc.scope == (self.slot, claimed_view)
                    // no validator can hold a prepare certificate from a view
                    // later than the one it is abandoning.
                    && claimed_view <= self.view
                    && high.qc.verify(validator_data)
                    && high
                        .block
                        .as_ref()
                        .is_none_or(|block| entries_of(block) == high.qc.verdict.0)
            }
            // a claimed certificate that is not carried, or one carried
            // without any timeout claiming it.
            _ => false,
        }
    }

    /// `highPrepQC(J)`: the prepare certificate of highest view among those
    /// carried by the timeouts in this certificate, `None` if none carried one.
    pub(crate) fn high_prep_qc(&self) -> Option<&PrepareQc> {
        self.high_prepare.as_ref().map(|high| &high.qc)
    }

    /// `lock(J) = entries(highPrepQC(J))`: the entries view `self.view` may
    /// have locked, which the next leader is bound to extend. `None` when
    /// nothing is locked and the leader may propose any valid metablock.
    pub(crate) fn lock(&self) -> Option<&ProposalMap<Entry>> {
        self.high_prep_qc().map(|qc| &qc.verdict.0)
    }

    /// The certified entries matching `lock`, when a timeout carried them.
    /// Without them a leader bound by the lock has nothing valid to propose,
    /// since this implementation has no way to fetch the block from a signer.
    pub(crate) fn high_block(&self) -> Option<&PartialBlock> {
        self.high_prepare
            .as_ref()
            .and_then(|high| high.block.as_ref())
    }
}
