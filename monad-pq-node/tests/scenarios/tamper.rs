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

//! A pq node runtime whose effects pass through a filter, and whose
//! proposals a scenario may replace, before they reach the components or
//! the network.

use monad_pq_chorus::PqMessage;
use monad_pq_node::{
    PqEffect, PqRuntime,
    chorus::types::Timestamp,
    component::{Dispatch, PqCadenceOutput, ProposingOutput, RepeaterOutput},
    da::DAOutput,
    network::Inbound,
};

pub type EffectFilter = Box<dyn FnMut(PqEffect) -> Vec<PqEffect>>;
pub type ProposalHandler<R> = Box<dyn FnMut(&mut R, Timestamp, ProposingOutput) -> Vec<PqEffect>>;

pub struct Tampered<R> {
    inner: R,
    pub filter: Option<EffectFilter>,
    pub proposal_handler: Option<ProposalHandler<R>>,
}

impl<R> From<R> for Tampered<R> {
    fn from(inner: R) -> Self {
        Self {
            inner,
            filter: None,
            proposal_handler: None,
        }
    }
}

impl<R> Tampered<R> {
    pub fn filtered<F>(mut self, filter: F) -> Self
    where
        F: FnMut(PqEffect) -> Vec<PqEffect> + 'static,
    {
        self.filter = Some(Box::new(filter));
        self
    }

    pub fn proposing<F>(mut self, handler: F) -> Self
    where
        F: FnMut(&mut R, Timestamp, ProposingOutput) -> Vec<PqEffect> + 'static,
    {
        self.proposal_handler = Some(Box::new(handler));
        self
    }
}

impl<R: PqRuntime> PqRuntime for Tampered<R> {
    fn handle_inbound(
        &mut self,
        now: Timestamp,
        inbound: Inbound,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        self.inner.handle_inbound(now, inbound, &mut effects);
    }

    fn handle_cadence(&mut self, output: PqCadenceOutput, effects: &mut impl Dispatch<PqEffect>) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        self.inner.handle_cadence(output, &mut effects);
    }

    fn handle_da(
        &mut self,
        now: Timestamp,
        output: DAOutput,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        self.inner.handle_da(now, output, &mut effects);
    }

    fn handle_proposal(
        &mut self,
        now: Timestamp,
        output: ProposingOutput,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        let Some(handler) = &mut self.proposal_handler else {
            self.inner.handle_proposal(now, output, &mut effects);
            return;
        };
        for effect in handler(&mut self.inner, now, output) {
            effects.dispatch(effect);
        }
    }

    fn handle_repeat(
        &mut self,
        output: RepeaterOutput<PqMessage>,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        self.inner.handle_repeat(output, &mut effects);
    }
}

struct Filtered<'a, D> {
    filter: &'a mut Option<EffectFilter>,
    sink: &'a mut D,
}

impl<'a, D> Filtered<'a, D> {
    fn new(filter: &'a mut Option<EffectFilter>, sink: &'a mut D) -> Self {
        Self { filter, sink }
    }
}

impl<D: Dispatch<PqEffect>> Dispatch<PqEffect> for Filtered<'_, D> {
    fn dispatch(&mut self, effect: PqEffect) {
        let Some(filter) = &mut self.filter else {
            self.sink.dispatch(effect);
            return;
        };
        for effect in filter(effect) {
            self.sink.dispatch(effect);
        }
    }
}
