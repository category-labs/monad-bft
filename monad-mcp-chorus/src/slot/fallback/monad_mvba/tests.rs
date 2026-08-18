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

use super::{
    super::{FallbackView, Mvba},
    TimerEvent,
    messages::Message,
    test_helpers::*,
};

#[test]
fn decides_along_the_happy_path() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];
    assert_ne!(leader_of(view(1)), follower, "test assumes a follower");

    let mut instance = started(follower, &block, &validator_data);
    let startup = drain(&mut instance);
    assert_eq!(
        scheduled_timers(&startup),
        vec![TimerEvent::ViewTimeout(view(1))],
    );
    assert!(
        broadcasts(&startup).is_empty(),
        "a follower proposes nothing"
    );

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);
    assert_eq!(prepared_entries(&outputs), Some(block.entries()));

    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    let outputs = drain(&mut instance);
    assert_eq!(committed_entries(&outputs), Some(block.entries()));
    assert!(instance.decision().is_none());

    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block.entries()));
    let decision_qc = instance
        .decision_qc()
        .expect("a decision comes with its certificate");
    assert_eq!(decision_qc.verdict.0, block.entries());
    assert!(
        broadcasts(&outputs)
            .iter()
            .any(|message| matches!(message, Message::CommitQc(_))),
        "the certificate is passed on so others can decide"
    );
}

#[test]
fn a_quorum_that_arrives_before_the_proposal_fires_on_arrival() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    // a different input, so this validator holds no metablock with the voted
    // entries until something carries one to it.
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // prepare votes carry no block, so the quorum completes with nothing this
    // validator can act on.
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    let outputs = drain(&mut instance);
    assert!(broadcasts(&outputs).is_empty());

    // the proposal lands: accepting it and the waiting prepare quorum both
    // fire in the one call.
    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(prepared_entries(&outputs), Some(block.entries()));
    assert_eq!(
        committed_entries(&outputs),
        Some(block.entries()),
        "the prepare certificate forms in the same call"
    );

    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    drain(&mut instance);
    assert_eq!(instance.decision(), Some(&block.entries()));
}

#[test]
fn commit_votes_alone_decide_without_the_proposal() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // it never saw the pre-prepare and its own input is a different metablock,
    // so the certificate the commit votes form is the whole of what it decides
    // on.
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block.entries()));
    assert_eq!(
        instance
            .decision_qc()
            .expect("a decision comes with its certificate")
            .verdict
            .0,
        block.entries()
    );
}

#[test]
fn a_proposal_that_breaks_the_lock_is_ignored() {
    let validator_data = validator_data();
    let locked = metablock(1, &validator_data);
    let other = metablock(2, &validator_data);

    let follower = nodes()[0];
    let mut instance = started(follower, &locked, &validator_data);
    drain(&mut instance);

    // view 1 may have locked `locked`, and the certificate says so.
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    let tc = timeout_certificate(view(1), Some((qc, locked.clone())), &validator_data);

    let (leader, proposal) = pre_prepare(view(2), &other, Some(tc.clone()));
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        None,
        "a leader may not replace the locked value"
    );

    // the same view accepts the locked metablock.
    let mut instance = started(follower, &locked, &validator_data);
    drain(&mut instance);
    let (leader, proposal) = pre_prepare(view(2), &locked, Some(tc));
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(prepared_entries(&outputs), Some(locked.entries()));
}

#[test]
fn a_stale_validator_jumps_views_on_a_carried_certificate() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // this validator is still in view 1; the proposal for view 3 carries the
    // certificate that explains the gap.
    let tc = timeout_certificate(view(2), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(3), &block, Some(tc));
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(3))),
        "entering a view restarts its timer"
    );
    assert_eq!(
        prepared_entries(&outputs),
        Some(block.entries()),
        "the jumped-to view's proposal is accepted in the same call"
    );
}

#[test]
fn f_plus_one_timeouts_pull_this_validator_into_timing_out() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // two of four validators is f+1 here, and the timer has not fired.
    feed_timeouts(&mut instance, view(1), None, &nodes()[1..3]);
    let outputs = drain(&mut instance);

    assert_eq!(timed_out_view(&outputs), Some(view(1)));
}

#[test]
fn a_timeout_quorum_advances_the_view_and_the_new_leader_proposes() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    // the validator that leads view 2 sees the view time out.
    let next_leader = leader_of(view(2));
    let mut instance = started(next_leader, &block, &validator_data);
    drain(&mut instance);

    feed_timeouts(&mut instance, view(1), None, &quorum());
    let outputs = drain(&mut instance);

    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))));
    let proposal = proposed(&outputs).expect("the leader of view 2 proposes on entering it");
    assert_eq!(proposal.view, view(2));
    assert_eq!(
        proposal.metablock, block,
        "nothing is locked, so the leader proposes its own input"
    );
}

#[test]
fn a_locked_leader_reproposes_the_locked_metablock() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    let locked = metablock(2, &validator_data);

    let next_leader = leader_of(view(2));
    let mut instance = started(next_leader, &own, &validator_data);
    drain(&mut instance);

    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    feed_timeouts(
        &mut instance,
        view(1),
        Some((qc, locked.clone())),
        &quorum(),
    );
    let outputs = drain(&mut instance);

    let proposal = proposed(&outputs).expect("the leader of view 2 proposes on entering it");
    assert_eq!(
        proposal.metablock, locked,
        "the leader is bound to the value the previous view may have locked"
    );
}

#[test]
fn the_view_timer_makes_this_validator_time_out() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // a timer for a view already left is ignored.
    instance.handle_timer(TimerEvent::ViewTimeout(view(7)));
    assert!(drain(&mut instance).is_empty());

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));

    // and it does not time out twice for the same view.
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    assert!(timed_out_view(&drain(&mut instance)).is_none());
}

#[test]
fn a_second_proposal_cannot_displace_the_first() {
    let validator_data = validator_data();
    let first = metablock(1, &validator_data);
    let second = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &first, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &second, None);
    instance.handle_message(leader, proposal);
    drain(&mut instance);

    // the equivocating leader's second proposal arrives for the same view.
    let (leader, proposal) = pre_prepare(view(1), &first, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        None,
        "the view has already voted, and it voted for what arrived first"
    );
}

#[test]
fn a_proposal_from_the_wrong_sender_is_discarded() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    let impostor = nodes()
        .into_iter()
        .find(|node| *node != leader && *node != follower)
        .expect("the cluster has more than two validators");

    instance.handle_message(impostor, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(prepared_entries(&outputs), None);
}

#[test]
fn a_received_certificate_decides_the_metablock_it_carries() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // this validator saw neither the proposal nor the votes, and holds no
    // metablock with these entries: the message carries both halves.
    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block.entries()));
    assert!(instance.decision_qc().is_some());
}

#[test]
fn a_decided_instance_ignores_everything_after() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let other = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    drain(&mut instance);
    assert!(instance.decision().is_some());

    // a later view's proposal, a timeout quorum, and a timer all arrive.
    let tc = timeout_certificate(view(1), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(2), &other, Some(tc));
    instance.handle_message(leader, proposal);
    feed_timeouts(&mut instance, view(1), None, &quorum());
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));

    assert!(drain(&mut instance).is_empty(), "a decision is terminal");
    assert_eq!(
        instance.decision(),
        Some(&block.entries()),
        "and it never changes"
    );
}

#[test]
fn abandon_stops_this_instance_sending() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    instance.abandon();
    assert!(drain(&mut instance).is_empty(), "pending sends are dropped");

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    assert!(drain(&mut instance).is_empty());
    assert!(instance.decision().is_none());
}

#[test]
fn nothing_is_sent_before_propose() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    // the validator that leads view 1, so it would propose if it were running.
    let mut instance = mvba(leader_of(view(1)), &validator_data);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    assert!(drain(&mut instance).is_empty());

    // proposing starts participation, and the stored messages take effect at
    // once.
    instance.propose(block.clone());
    let outputs = drain(&mut instance);
    assert_eq!(prepared_entries(&outputs), Some(block.entries()));
}

#[test]
fn views_are_one_indexed() {
    assert_eq!(FallbackView::FIRST, view(1));
    assert!(FallbackView::GENESIS < FallbackView::FIRST);
}
