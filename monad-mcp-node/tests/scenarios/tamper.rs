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

//! A node router whose effects pass through a filter before routing,
//! and whose routed values pass through a filter before they reach the
//! components or the network.

use monad_mcp_common::Timestamp;
use monad_mcp_node::{Effect, Routed, Router, Sink};

pub type PreRouteFilter = Box<dyn FnMut(Effect) -> Vec<Effect>>;
pub type PostRouteFilter = Box<dyn FnMut(Routed) -> Vec<Routed>>;
pub type CustomRouter<R> = Box<dyn FnMut(&mut R, Timestamp, Effect) -> Vec<Routed>>;

pub struct Tampered<R> {
    inner: R,

    pub pre_filter: Option<PreRouteFilter>,
    pub tamper: Option<CustomRouter<R>>,
    pub post_filter: Option<PostRouteFilter>,
}

impl<R> From<R> for Tampered<R> {
    fn from(inner: R) -> Self {
        Self {
            inner,
            pre_filter: None,
            tamper: None,
            post_filter: None,
        }
    }
}

impl<R> Tampered<R> {
    #[allow(unused)]
    pub fn tamper_pre<F>(mut self, pre_filter: F) -> Self
    where
        F: FnMut(Effect) -> Vec<Effect> + 'static,
    {
        self.pre_filter = Some(Box::new(pre_filter));
        self
    }

    #[allow(unused)]
    pub fn tamper_post<F>(mut self, post_filter: F) -> Self
    where
        F: FnMut(Routed) -> Vec<Routed> + 'static,
    {
        self.post_filter = Some(Box::new(post_filter));
        self
    }
}

impl<R: Router> Router for Tampered<R> {
    fn route(&mut self, now: Timestamp, effect: Effect, sink: &mut impl Sink<Routed>) {
        let effects = match &mut self.pre_filter {
            Some(pre_filter) => pre_filter(effect),
            None => vec![effect],
        };
        let mut sink = FilteredSink::new(&mut self.post_filter, sink);
        for effect in effects {
            let Some(tamper) = &mut self.tamper else {
                self.inner.route(now, effect, &mut sink);
                continue;
            };

            let new_routed = tamper(&mut self.inner, now, effect);
            for next in new_routed {
                sink.send(next);
            }
        }
    }
}

struct FilteredSink<'a, D> {
    filter: &'a mut Option<PostRouteFilter>,
    sink: &'a mut D,
}

impl<'a, D> FilteredSink<'a, D> {
    fn new(filter: &'a mut Option<PostRouteFilter>, sink: &'a mut D) -> Self {
        Self { filter, sink }
    }
}

impl<D: Sink<Routed>> Sink<Routed> for FilteredSink<'_, D> {
    fn send(&mut self, routed: Routed) {
        let Some(filter) = &mut self.filter else {
            self.sink.send(routed);
            return;
        };

        for next in filter(routed) {
            self.sink.send(next);
        }
    }
}
