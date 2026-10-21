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

//! Coda (Alg. 2): Byzantine agreement with per-proposer strong unanimity,
//! sans-IO. The host broadcasts [`CodaOutput::Broadcast`] messages and
//! retransmits them until [`CodaOutput::Abandon`], and calls
//! [`Coda::on_timeout`] when a [`CodaOutput::ScheduleTimeout`] elapses.

mod count;
#[cfg(test)]
mod sim;
mod view;

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

use view::{OwnView, ReceivedView};

use crate::{
    message::CodaMessage,
    quorum::Quorum,
    types::{CodaMetablock, CodaValue, NodeId, Slot},
};

pub trait CodaVal: Clone + Eq + Hash + Ord + Debug {}
impl<T: Clone + Eq + Hash + Ord + Debug> CodaVal for T {}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CodaOutput<V> {
    Broadcast(CodaMessage<V>),
    /// Call `on_timeout(view)` once the view timeout (5Δ) elapses.
    ScheduleTimeout(u64),
    /// `ln:rule_abandon`: stop retransmitting messages of views below this.
    Abandon {
        below: u64,
    },
    /// Emitted once; `view` is the view whose Vote2 quorum decided.
    Decide {
        metablock: CodaMetablock<V>,
        view: u64,
    },
}

pub struct Coda<V: CodaVal> {
    me: NodeId,
    slot: Slot,
    k: usize,
    q: Quorum,
    validators: Arc<[NodeId]>,

    // ln:core:ps_view; 0 before start
    view: u64,
    // whether 5Δ has elapsed since entering `view`
    timeout_elapsed: bool,
    decided: Option<CodaMetablock<V>>,
    // line:persistant_memory: views view−1 and view only
    own: BTreeMap<u64, OwnView<V>>,
    // views ≥ view−1, each sender's messages for its two highest views (D7)
    rx: BTreeMap<u64, ReceivedView<V>>,
    // per sender: its highest and second-highest views seen, 0 for none
    sender_views: BTreeMap<NodeId, [u64; 2]>,
    out: VecDeque<CodaOutput<V>>,
}

impl<V: CodaVal> Coda<V> {
    /// `validators` must be sorted and contain `me`; `k ≤ u16::MAX`.
    pub fn new(slot: Slot, me: NodeId, validators: Arc<[NodeId]>, q: Quorum, k: usize) -> Self {
        debug_assert!(validators.is_sorted());
        debug_assert_eq!(validators.len(), q.n);
        debug_assert!(k <= u16::MAX as usize);
        Self {
            me,
            slot,
            k,
            q,
            validators,
            view: 0,
            timeout_elapsed: false,
            decided: None,
            own: BTreeMap::new(),
            rx: BTreeMap::new(),
            sender_views: BTreeMap::new(),
            out: VecDeque::new(),
        }
    }

    pub fn view(&self) -> u64 {
        self.view
    }

    pub fn is_started(&self) -> bool {
        self.view > 0
    }

    pub fn decided(&self) -> Option<&CodaMetablock<V>> {
        self.decided.as_ref()
    }

    pub fn poll(&mut self) -> Option<CodaOutput<V>> {
        self.out.pop_front()
    }

    /// `ln:core:init_header`: input a metablock and enter view 1. Ignored
    /// once started or for a metablock of the wrong size.
    pub fn start(&mut self, input: CodaMetablock<V>) {
        if self.is_started() || input.size() != self.k {
            return;
        }
        // ln:core:init_set_proposal, ln:core:init_enter_view1
        self.enter(1, input);
        self.settle();
    }

    pub fn handle_message(&mut self, sender: NodeId, message: CodaMessage<V>) {
        if self.validators.binary_search(&sender).is_err() || !self.well_formed(&message) {
            return;
        }
        let view = message.view();
        if view < self.view.saturating_sub(1) || !self.admit(sender, view) {
            return;
        }
        let leader = self.leader(view);
        let echo_cap = self.validators.len() + 1;
        let rx = self.rx.entry(view).or_default();
        match message {
            CodaMessage::Prepare { x, .. } => {
                if view >= 2 && sender == leader && rx.prepare.is_none() {
                    rx.prepare = Some(x);
                }
            }
            CodaMessage::Vote1 { x, .. } => rx.add_vote1(sender, x),
            CodaMessage::Vote2 { lock, guard, .. } => rx.add_vote2(sender, lock, guard),
            CodaMessage::Echo { j, v, .. } => rx.add_echo(sender, j, v, echo_cap),
        }
        self.settle();
    }

    /// `ln:core:on_timer_header`, for the timeout scheduled on entering
    /// `view`; stale or repeated timeouts are ignored.
    pub fn on_timeout(&mut self, view: u64) {
        if !self.is_started() || view != self.view || self.timeout_elapsed {
            return;
        }
        self.timeout_elapsed = true;
        let own = self.own.get_mut(&view).expect("current view state");
        // ln:core:on_timer_if_no_vote2, ln:core:on_timer_set_vote2
        if own.vote2.is_none() {
            let guard = own.guard.clone().expect("current view has a guard");
            own.vote2 = Some((None, guard.clone()));
            self.broadcast(CodaMessage::Vote2 {
                view,
                lock: None,
                guard,
            });
        }
        self.settle();
    }

    fn well_formed(&self, message: &CodaMessage<V>) -> bool {
        let k = self.k;
        message.view() > 0
            && match message {
                CodaMessage::Prepare { x, .. } | CodaMessage::Vote1 { x, .. } => x.size() == k,
                CodaMessage::Vote2 { lock, guard, .. } => {
                    guard.size() == k && lock.as_ref().is_none_or(|lock| lock.size() == k)
                }
                CodaMessage::Echo { j, .. } => (*j as usize) < k,
            }
    }

    // D7: keep a sender's messages only for its two highest views
    fn admit(&mut self, sender: NodeId, view: u64) -> bool {
        let top = self.sender_views.entry(sender).or_insert([0, 0]);
        let [highest, second] = *top;
        if view == highest || view == second {
            return true;
        }
        if view < second {
            return false;
        }
        *top = if view > highest {
            [view, highest]
        } else {
            [highest, view]
        };
        if second > 0 {
            let is_leader = self.leader(second) == sender;
            if let Some(rx) = self.rx.get_mut(&second) {
                rx.evict(&sender, is_leader);
                if rx.is_empty() {
                    self.rx.remove(&second);
                }
            }
        }
        true
    }

    // D9: round-robin over the sorted validators, offset by the slot
    fn leader(&self, view: u64) -> NodeId {
        let n = self.validators.len() as u64;
        self.validators[(self.slot.get().wrapping_add(view) % n) as usize]
    }

    // ln:core:on_enter_header
    fn enter(&mut self, view: u64, guard: CodaMetablock<V>) {
        debug_assert!(view > self.view);
        self.view = view;
        self.timeout_elapsed = false;
        let floor = view - 1;
        self.own.retain(|w, _| *w >= floor);
        self.rx.retain(|w, _| *w >= floor);
        self.own.insert(view, OwnView::new(Some(guard.clone())));
        if floor > 0 {
            self.out.push_back(CodaOutput::Abandon { below: floor });
        }
        self.out.push_back(CodaOutput::ScheduleTimeout(view));

        if view == 1 {
            // ln:core:on_enter_dummy_view, ln:core:on_enter_dummy_vote1
            self.own_view().vote1 = Some(guard.clone());
            self.broadcast(CodaMessage::Vote1 { view, x: guard });
        } else if self.leader(view) == self.me {
            // ln:core:on_enter_if_leader
            self.broadcast(CodaMessage::Prepare { view, x: guard });
        }
    }

    fn own_view(&mut self) -> &mut OwnView<V> {
        self.own.get_mut(&self.view).expect("current view state")
    }

    // every rule fires as soon as its condition holds (paper §4 conventions)
    fn settle(&mut self) {
        if !self.is_started() {
            return;
        }
        loop {
            let mut changed = self.try_enter_higher_view();
            changed |= self.try_vote1();
            changed |= self.accept_vote2s();
            changed |= self.try_lock();
            changed |= self.view_change();
            changed |= self.amplify();
            self.try_decide();
            if !changed {
                break;
            }
        }
    }

    // ln:vc:upon_enter_next_view and ln:vc:upon_catch_up, preferring the
    // higher resulting view
    fn try_enter_higher_view(&mut self) -> bool {
        let next =
            self.rx.range(self.view..).rev().find_map(|(w, rx)| {
                Some((w.checked_add(1)?, rx.certificate(self.k, self.q.strong)?))
            });
        let catch_up = self
            .rx
            .range(self.view + 1..)
            .rev()
            .find_map(|(w, rx)| Some((*w, rx.certificate(self.k, self.q.weak)?)));
        let target = match (next, catch_up) {
            (Some(next), Some(catch_up)) if catch_up.0 > next.0 => catch_up,
            (Some(next), _) => next,
            (None, Some(catch_up)) => catch_up,
            (None, None) => return false,
        };
        // ln:vc:upon_enter_set_guard / upon_catch_up_set_guard, then enter
        self.enter(target.0, target.1);
        true
    }

    // ln:core:on_prepare_header: vote for the leader's justified proposal
    fn try_vote1(&mut self) -> bool {
        let view = self.view;
        if view < 2 || self.own[&view].vote1.is_some() {
            return false;
        }
        let Some(x) = self.rx.get(&view).and_then(|rx| rx.prepare.clone()) else {
            return false;
        };
        if !self.justified(&x) {
            return false;
        }
        // ln:core:on_prepare_broadcast_vote1
        self.own_view().vote1 = Some(x.clone());
        self.broadcast(CodaMessage::Vote1 { view, x });
        true
    }

    // ln:core:fn_justified: every entry echoed by f+1 in the previous view
    fn justified(&self, x: &CodaMetablock<V>) -> bool {
        let Some(rx) = self.rx.get(&(self.view - 1)) else {
            return false;
        };
        (0..self.k).all(|j| rx.echo_count(j as u16, &x[j]) >= self.q.weak)
    }

    fn accept_vote2s(&mut self) -> bool {
        let weak = self.q.weak;
        self.rx
            .get_mut(&self.view)
            .is_some_and(|rx| rx.update_acceptance(weak))
    }

    // ln:core:on_vote1_quorum_header: lock on a strong Vote1 quorum
    fn try_lock(&mut self) -> bool {
        let view = self.view;
        let strong = self.q.strong;
        if self.own[&view].vote2.is_some() {
            return false;
        }
        let Some(x) = self.rx.get(&view).and_then(|rx| {
            rx.vote1_count
                .iter()
                .find(|(_, count)| **count >= strong)
                .map(|(x, _)| x.0.clone())
        }) else {
            return false;
        };
        let own = self.own_view();
        own.lock = Some(x.clone());
        let guard = own.guard.clone().expect("current view has a guard");
        own.vote2 = Some((Some(x.clone()), guard.clone()));
        // ln:core:on_vote1_quorum_set_vote2
        self.broadcast(CodaMessage::Vote2 {
            view,
            lock: Some(x),
            guard,
        });
        true
    }

    // ln:core:on_vote2_quorum_header over views view−1 and view
    fn try_decide(&mut self) {
        if self.decided.is_some() {
            return;
        }
        let strong = self.q.strong;
        let decision = self
            .rx
            .range(self.view.saturating_sub(1)..=self.view)
            .find_map(|(view, rx)| {
                rx.vote2_lock_count
                    .iter()
                    .find(|(_, count)| **count >= strong)
                    .map(|(x, _)| (x.0.clone(), *view))
            });
        if let Some((metablock, view)) = decision {
            self.decided = Some(metablock.clone());
            self.out.push_back(CodaOutput::Decide { metablock, view });
        }
    }

    // ln:vc:on_vote2_quorum_compute_approved, gated on the view timeout
    // (ln:rule_view_change_gate) and evaluated by counts (count.rs)
    fn view_change(&mut self) -> bool {
        if !self.timeout_elapsed {
            return false;
        }
        let view = self.view;
        let Some(rx) = self.rx.get(&view) else {
            return false;
        };
        let echoes = count::view_change_echoes(self.q, self.k, rx.accepted_vote2s());
        let mut changed = false;
        for (j, v) in echoes {
            changed |= self.echo(view, j, v);
        }
        changed
    }

    // ln:vc:upon_echo_amplification for views view−1 and view
    fn amplify(&mut self) -> bool {
        let weak = self.q.weak;
        let amplified: Vec<(u64, u16, CodaValue<V>)> = self
            .rx
            .range(self.view.saturating_sub(1).max(1)..=self.view)
            .flat_map(|(w, rx)| {
                rx.echo
                    .iter()
                    .filter(|(_, senders)| senders.len() >= weak)
                    .map(|((j, v), _)| (*w, *j, v.clone()))
            })
            .collect();
        let mut changed = false;
        for (view, j, v) in amplified {
            changed |= self.echo(view, j, v);
        }
        changed
    }

    // ln:vc:fn_echo: broadcast each echo at most once
    fn echo(&mut self, view: u64, j: u16, v: CodaValue<V>) -> bool {
        debug_assert!(view + 1 >= self.view && view <= self.view);
        let own = self.own.entry(view).or_insert_with(|| OwnView::new(None));
        if !own.echoes.insert((j, v.clone())) {
            return false;
        }
        self.broadcast(CodaMessage::Echo { view, j, v });
        true
    }

    fn broadcast(&mut self, message: CodaMessage<V>) {
        self.out.push_back(CodaOutput::Broadcast(message));
    }
}

#[cfg(test)]
impl<V: CodaVal> Coda<V> {
    pub(crate) fn own_views(&self) -> usize {
        self.own.len()
    }

    /// The number of views holding a message of `sender`.
    pub(crate) fn views_holding(&self, sender: &NodeId) -> usize {
        self.rx.values().filter(|rx| rx.holds(sender)).count()
    }

    pub(crate) fn received_views(&self) -> usize {
        self.rx.len()
    }

    /// The number of distinct echoes of `sender` held over all views.
    pub(crate) fn echoes_held(&self, sender: &NodeId) -> usize {
        self.rx.values().map(|rx| rx.echoes_of(sender)).sum()
    }
}
