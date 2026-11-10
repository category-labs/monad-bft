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

//! The fallback path as Chorus runs it: the leaderless dummy vote and the
//! leader-based MVBA, started together and deciding by whichever certificate
//! forms first

use super::{
    super::types::{Lanes, ValidatorData, ValidatorIndex},
    EnterFallbackCert, Entry, FallbackCommitQc, MVBAOutput, Metablock, Mvba, ValidateInput,
    Votable,
    leaderless::{DummyQc, LeaderlessMessage, LeaderlessRound, NfCert},
    monad_mvba::{
        MakesValidationContext, MonadMvba, MvbaContext, MvbaMessage, TimerEvent, ValidationContext,
    },
};

/// The value the core MVBA runs over: a metablock with the `fbcert` the paper
/// makes a field of it, so the certificate travels wherever the block does
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CertifiedMetablock {
    pub block: Metablock,
    pub fbcert: Option<EnterFallbackCert>,
}

impl ValidateInput for CertifiedMetablock {
    type Context = ValidationContext;

    fn validate(&self, context: &Self::Context) -> bool {
        todo!("D1: a fast block carries no fbcert, any other a valid one")
    }

    fn cert_optional(&self) -> bool {
        todo!("D1")
    }
}

impl Votable for CertifiedMetablock {
    type Entries = Lanes<Entry>;

    fn entries(&self) -> Self::Entries {
        todo!("D1")
    }
}

impl MakesValidationContext<CertifiedMetablock> for MvbaContext {
    fn make_validation_context(&self) -> ValidationContext {
        MakesValidationContext::<Metablock>::make_validation_context(self)
    }
}

/// What proves the slot's fallback decision
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotCommitProof {
    Dummy(DummyQc),
    Mvba(FallbackCommitQc<Lanes<Entry>>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotFallbackMessage {
    Leaderless(LeaderlessMessage),
    Mvba(MvbaMessage<CertifiedMetablock, NfCert>),
}

pub struct SlotFallback {
    round: LeaderlessRound,
    core: MonadMvba<CertifiedMetablock, NfCert>,
}

impl Mvba<Metablock> for SlotFallback {
    type Message = SlotFallbackMessage;
    type Context = MvbaContext;
    // the core's timers only: the round has none
    type TimerEvent = TimerEvent<CertifiedMetablock>;
    type FallbackCert = EnterFallbackCert;
    type DecisionProof = SlotCommitProof;

    fn new(ctx: MvbaContext) -> Self {
        todo!("D1")
    }

    fn propose(&mut self, data: Metablock, cert: Option<EnterFallbackCert>) {
        todo!("D1")
    }

    fn validator_data(&self) -> &ValidatorData {
        todo!("D1")
    }

    fn handle_message(&mut self, sender: ValidatorIndex, message: SlotFallbackMessage) {
        todo!("D1")
    }

    fn handle_timer(&mut self, timer_event: TimerEvent<CertifiedMetablock>) {
        todo!("D1")
    }

    fn abandon(&mut self) {
        todo!("D1")
    }

    fn decision(&self) -> Option<&Metablock> {
        todo!("D1")
    }

    fn decision_proof(&self) -> Option<&SlotCommitProof> {
        todo!("D1")
    }

    fn poll(&mut self) -> Option<MVBAOutput<SlotFallbackMessage, TimerEvent<CertifiedMetablock>>> {
        todo!("D1")
    }
}
