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

//! A node runtime whose effects pass through a filter before they
//! reach the components or the network.

use monad_mcp_node::{
    Dispatch, Effect, Runtime,
    chorus::{slot::chorus::ChorusMessage, types::Timestamp},
    component::{CadenceOutput, ProposingOutput, RepeaterOutput},
    da::DAOutput,
    network::Inbound,
};

pub type EffectFilter = Box<dyn FnMut(Effect) -> Vec<Effect>>;
pub type Handler<R, T> = Box<dyn FnMut(&mut R, T) -> Vec<Effect>>;
pub type ProposalHandler<R> = Box<dyn FnMut(&mut R, Timestamp, ProposingOutput) -> Vec<Effect>>;

pub struct Tampered<R> {
    inner: R,

    pub filter: Option<EffectFilter>,
    pub inbound_handler: Option<Handler<R, Inbound>>,
    pub cadence_handler: Option<Handler<R, CadenceOutput>>,
    pub da_handler: Option<Handler<R, DAOutput>>,
    pub proposal_handler: Option<ProposalHandler<R>>,
    pub repeat_handler: Option<Handler<R, RepeaterOutput<ChorusMessage>>>,
}

impl<R> From<R> for Tampered<R> {
    fn from(inner: R) -> Self {
        Self {
            inner,
            filter: None,
            inbound_handler: None,
            cadence_handler: None,
            da_handler: None,
            proposal_handler: None,
            repeat_handler: None,
        }
    }
}

impl<R> Tampered<R> {
    pub fn filtered<F>(mut self, filter: F) -> Self
    where
        F: FnMut(Effect) -> Vec<Effect> + 'static,
    {
        self.filter = Some(Box::new(filter));
        self
    }
}

impl<R: Runtime> Runtime for Tampered<R> {
    fn handle_inbound(&mut self, inbound: Inbound, effects: &mut impl Dispatch<Effect>) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        let Some(handler) = &mut self.inbound_handler else {
            self.inner.handle_inbound(inbound, &mut effects);
            return;
        };

        let new_effects = handler(&mut self.inner, inbound);
        for effect in new_effects {
            effects.dispatch(effect);
        }
    }

    fn handle_cadence(&mut self, output: CadenceOutput, effects: &mut impl Dispatch<Effect>) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        let Some(handler) = &mut self.cadence_handler else {
            self.inner.handle_cadence(output, &mut effects);
            return;
        };

        let new_effects = handler(&mut self.inner, output);
        for effect in new_effects {
            effects.dispatch(effect);
        }
    }

    fn handle_da(&mut self, output: DAOutput, effects: &mut impl Dispatch<Effect>) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        let Some(handler) = &mut self.da_handler else {
            self.inner.handle_da(output, &mut effects);
            return;
        };

        let new_effects = handler(&mut self.inner, output);
        for effect in new_effects {
            effects.dispatch(effect);
        }
    }

    fn handle_proposal(
        &mut self,
        now: Timestamp,
        output: ProposingOutput,
        effects: &mut impl Dispatch<Effect>,
    ) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        let Some(handler) = &mut self.proposal_handler else {
            self.inner.handle_proposal(now, output, &mut effects);
            return;
        };

        let new_effects = handler(&mut self.inner, now, output);
        for effect in new_effects {
            effects.dispatch(effect);
        }
    }

    fn handle_repeat(
        &mut self,
        output: RepeaterOutput<ChorusMessage>,
        effects: &mut impl Dispatch<Effect>,
    ) {
        let mut effects = Filtered::new(&mut self.filter, effects);
        let Some(handler) = &mut self.repeat_handler else {
            self.inner.handle_repeat(output, &mut effects);
            return;
        };

        let new_effects = handler(&mut self.inner, output);
        for effect in new_effects {
            effects.dispatch(effect);
        }
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

impl<D: Dispatch<Effect>> Dispatch<Effect> for Filtered<'_, D> {
    fn dispatch(&mut self, effect: Effect) {
        let Some(filter) = &mut self.filter else {
            self.sink.dispatch(effect);
            return;
        };

        for effect in filter(effect) {
            self.sink.dispatch(effect);
        }
    }
}
