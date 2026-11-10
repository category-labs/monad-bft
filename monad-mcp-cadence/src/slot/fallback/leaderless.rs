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

//! The dummy vote, run beside view 1 of the MVBA rather than before it: a
//! validator whose input is a fast metablock votes for its entries, any other
//! declares `NotFull`. Nothing here depends on the view the MVBA is in

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};

use super::{
    super::types::{IsVote, Lanes, Slot, StrongQc, ValidatorIndex, VoteMsg},
    Entry, Metablock, ValidateCert,
    monad_mvba::{MvbaContext, ValidationContext},
};
use crate::spec::vote::{SigningDomain, assert_signing_prefix};

/// `⟨DummyVote, slot, entries(x)⟩` over the entries of a fast metablock
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct DummyVote(pub Lanes<Entry>);

pub struct DummyVoteDomain;
const _: () = assert_signing_prefix::<DummyVoteDomain>();

impl SigningDomain for DummyVoteDomain {
    const PREFIX: &'static [u8] = b"\x20monad/cadence/mvba-dummy-vote/1\n";
}

impl IsVote for DummyVote {
    type Scope = Slot;
    type SigningDomain = DummyVoteDomain;
}

/// `DummyQC`: 2f+1 dummy votes on the same entries, which decides the slot
pub type DummyQc = StrongQc<DummyVote>;

/// `⟨NotFull, slot⟩`: the signer's registered input is not a fast metablock
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub struct NotFull;

pub struct NotFullDomain;
const _: () = assert_signing_prefix::<NotFullDomain>();

impl SigningDomain for NotFullDomain {
    const PREFIX: &'static [u8] = b"\x1Emonad/cadence/mvba-not-full/1\n";
}

impl IsVote for NotFull {
    type Scope = Slot;
    type SigningDomain = NotFullDomain;
}

/// `NFCert`: 2f+1 declarations, which admits an unlocked non-fast proposal
pub type NfCert = StrongQc<NotFull>;

impl ValidateCert for NfCert {
    type Context = ValidationContext;

    fn validate(&self, context: &Self::Context) -> bool {
        todo!("B1: slot match, then verify")
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum LeaderlessMessage {
    /// The sender's registered input, a fast metablock
    FastInput(Metablock),
    NotFull(VoteMsg<NotFull>),
    DummyVote(VoteMsg<DummyVote>),
    /// So a validator that missed the votes or the block can still decide
    DummyDecision {
        qc: DummyQc,
        block: Metablock,
    },
}

/// What the round hands the composition that owns it
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum LeaderlessEffect {
    Broadcast(LeaderlessMessage),
    /// The first valid fast metablock seen, which any leader may propose
    FastBlock(Metablock),
    NfCert(NfCert),
    Decided {
        qc: DummyQc,
        block: Metablock,
    },
}

pub(crate) struct LeaderlessRound {}

impl LeaderlessRound {
    /// Messages are retained from construction, before [`Self::enter`]
    pub(crate) fn new(ctx: &MvbaContext) -> Self {
        todo!("B1")
    }

    /// Registers this validator's input and sends its evidence, once
    pub(crate) fn enter(&mut self, input: Metablock) {
        todo!("B1")
    }

    pub(crate) fn handle_message(&mut self, sender: ValidatorIndex, msg: LeaderlessMessage) {
        todo!("B1")
    }

    pub(crate) fn abandon(&mut self) {
        todo!("B1")
    }

    pub(crate) fn poll(&mut self) -> Option<LeaderlessEffect> {
        todo!("B1")
    }
}
