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

use crate::message::PqMessage;

/// The slot's alive broadcasts and its single retransmission timer chain
/// (§7). The chain is keyed by a generation, so a duplicate timer firing
/// (the runtime does not dedupe) never forks it.
#[derive(Default)]
pub(crate) struct Retransmit {
    alive: Vec<PqMessage>,
    generation: u32,
    armed: bool,
}

impl Retransmit {
    /// Keeps `message` alive; returns the generation to arm a timer for
    /// when the chain is not running yet.
    pub fn add(&mut self, message: PqMessage) -> Option<u32> {
        self.alive.push(message);
        if self.armed {
            return None;
        }
        self.armed = true;
        Some(self.generation)
    }

    /// A timer fired: the messages to re-broadcast and the generation to
    /// re-arm, unless the firing is stale or the chain stopped.
    pub fn fire(&mut self, generation: u32) -> Option<(u32, &[PqMessage])> {
        if !self.armed || generation != self.generation {
            return None;
        }
        self.generation = self.generation.wrapping_add(1);
        Some((self.generation, &self.alive))
    }

    /// `ln:rule_abandon` for Coda messages below `view`.
    pub fn abandon_coda_below(&mut self, view: u64) {
        self.alive
            .retain(|message| !matches!(message, PqMessage::Coda(coda) if coda.view() < view));
    }

    /// Halt: stop everything (D6; the node repeats Finalized).
    pub fn stop(&mut self) {
        self.alive.clear();
        self.armed = false;
    }

    #[cfg(test)]
    pub fn alive(&self) -> &[PqMessage] {
        &self.alive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        message::CodaMessage,
        types::{CodaValue, Entry, ProposalMap},
    };

    fn echo(view: u64) -> PqMessage {
        PqMessage::Coda(CodaMessage::Echo {
            view,
            j: 0,
            v: CodaValue::Empty,
        })
    }

    #[test]
    fn one_chain_despite_duplicate_firings() {
        let mut retransmit = Retransmit::default();
        assert_eq!(retransmit.add(echo(1)), Some(0));
        assert_eq!(retransmit.add(echo(2)), None);

        let (next, alive) = retransmit.fire(0).expect("current generation");
        assert_eq!((next, alive.len()), (1, 2));
        // a duplicate of the same firing is stale
        assert!(retransmit.fire(0).is_none());
        assert!(retransmit.fire(1).is_some());
    }

    #[test]
    fn abandon_and_stop() {
        let mut retransmit = Retransmit::default();
        let finalized = PqMessage::Finalized(ProposalMap::new(1, |_| Entry::Bot));
        retransmit.add(finalized.clone());
        for view in 1..=4 {
            retransmit.add(echo(view));
        }
        retransmit.abandon_coda_below(3);
        assert_eq!(retransmit.alive(), &[finalized, echo(3), echo(4)]);

        retransmit.stop();
        assert!(retransmit.alive().is_empty());
        assert!(retransmit.fire(0).is_none());
    }
}
