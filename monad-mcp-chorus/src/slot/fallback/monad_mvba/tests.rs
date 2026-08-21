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
    super::{
        super::{
            fast::{CertifiedEntry, EnterFallbackVote},
            types::{ProposalMap, Slot},
        },
        FallbackView, Mvba, Votable,
    },
    TimerEvent,
    metablock::entries_of,
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
    assert_eq!(
        instance.decision_block(),
        Some(&block),
        "the block came with the proposal this validator accepted"
    );
    assert!(
        decided_commit_qc(&outputs),
        "the certificate is passed on so others can decide"
    );
    assert!(
        requested_entries(&outputs).is_empty(),
        "nothing to fetch: the accepted proposal carried the block"
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
fn commit_votes_decide_once_the_block_is_fetched() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // it never saw the pre-prepare and its own input is a different metablock,
    // so the certificate the commit votes form settles entries whose block it
    // does not hold.
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert_eq!(
        requested_entries(&outputs),
        vec![block.entries()],
        "agreement is done; the block behind the entries is not here yet"
    );
    assert!(instance.decision().is_none());
    assert!(instance.decision_qc().is_none());

    instance.handle_message(nodes()[1], block_response(block.clone()));
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block.entries()));
    assert_eq!(instance.decision_block(), Some(&block));
    assert_eq!(
        instance
            .decision_qc()
            .expect("a decision comes with its certificate")
            .verdict
            .0,
        block.entries()
    );
    assert!(
        decided_commit_qc(&outputs),
        "the echo goes out with the decision, after retrieval"
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
    let tc = timeout_certificate(view(1), Some(qc), &validator_data);

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
    assert_eq!(
        proposal.fallback_cert, None,
        "the timeout certificate justifies this view; the fallback \
         certificate would be weight on the wire nothing checks"
    );
}

#[test]
fn the_view_1_leader_carries_the_fallback_certificate_beside_its_input() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let cert = enter_fallback_cert(&validator_data);

    let leader = leader_of(view(1));
    let mut instance = mvba(leader, &validator_data);
    instance.propose(block.clone(), Some(cert.clone()));
    let outputs = drain(&mut instance);

    let proposal = proposed(&outputs).expect("the leader of view 1 proposes as soon as it can");
    assert_eq!(proposal.view, view(1));
    assert_eq!(proposal.metablock, block);
    assert_eq!(
        proposal.justification, None,
        "view 1 has no timeout certificate to be justified by"
    );
    assert_eq!(
        proposal.fallback_cert,
        Some(cert),
        "so it carries the certificate that admitted the path instead"
    );
}

#[test]
fn a_proposal_carrying_a_certificate_for_another_slot_is_rejected() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // genuinely aggregated, and genuinely irrelevant: it admits another slot
    // to the fallback path, not this one.
    let foreign = strong_qc(
        Slot(SLOT.get() + 1),
        EnterFallbackVote,
        &quorum(),
        &validator_data,
    );
    let (leader, proposal) = pre_prepare_with_cert(view(1), &block, Some(foreign), None);

    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        None,
        "a carried certificate is checked, so an out-of-scope one is not voted on"
    );
}

#[test]
fn a_view_1_proposal_without_a_certificate_is_the_paper_s_fast_metablock() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // `fbcert = ⊥`: admission rests on the entries rather than on a
    // certificate. See the FIXME on `proposed_metablock_is_valid` -- the arm
    // is under-constrained until something claims a fast metablock.
    let (leader, proposal) = pre_prepare_with_cert(view(1), &block, None, None);

    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        Some(block.entries()),
        "an absent certificate is legal: the value decides, not the carrier"
    );
}

#[test]
fn a_locked_leader_fetches_the_block_before_reproposing_it() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    let locked = metablock(2, &validator_data);

    let next_leader = leader_of(view(2));
    let mut instance = started(next_leader, &own, &validator_data);
    drain(&mut instance);

    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc), &quorum());
    let outputs = drain(&mut instance);

    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))),
        "the view is entered either way"
    );
    assert_eq!(
        requested_entries(&outputs),
        vec![locked.entries()],
        "the lock names entries whose block the leader does not hold"
    );
    assert!(
        proposed(&outputs).is_none(),
        "it may only propose the locked value, and does not have it yet"
    );

    instance.handle_message(nodes()[1], block_response(locked.clone()));
    let outputs = drain(&mut instance);

    let proposal = proposed(&outputs).expect("the block landed, so the leader can propose");
    assert_eq!(proposal.view, view(2));
    assert_eq!(
        proposal.metablock.entries(),
        locked.entries(),
        "the leader is bound to the value the previous view may have locked"
    );
    assert_eq!(
        proposal.metablock, locked,
        "and it proposes the retrieved block verbatim"
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
fn a_received_certificate_decides_once_its_block_is_retrieved() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // this validator saw neither the proposal nor the votes: the certificate
    // settles the entries, and block sync has to supply the rest.
    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    let outputs = drain(&mut instance);

    assert_eq!(requested_entries(&outputs), vec![block.entries()]);
    assert!(instance.decision().is_none());
    assert!(
        !decided_commit_qc(&outputs),
        "a certificate this validator cannot complete is not echoed"
    );

    instance.handle_message(nodes()[2], block_response(block.clone()));
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block.entries()));
    assert_eq!(instance.decision_block(), Some(&block));
    assert!(instance.decision_qc().is_some());
    assert!(decided_commit_qc(&outputs));
}

#[test]
fn a_bogus_block_response_is_ignored() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let other = metablock(3, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    assert_eq!(
        requested_entries(&drain(&mut instance)),
        vec![block.entries()]
    );

    // a well-formed block, but not the one asked for.
    instance.handle_message(nodes()[1], block_response(other.clone()));
    assert!(drain(&mut instance).is_empty());
    assert!(instance.decision().is_none());

    // the entries asked for, carried by certificates bound to another slot:
    // the entries a `FastQc` certifies are its verdict, so tampering with its
    // scope leaves the identity of the block intact.
    let forged = ProposalMap::new(NUM_PROPOSALS, |j| {
        let entry = block.as_ref().into_iter().nth(j).unwrap().entry();
        CertifiedEntry::FastQc(strong_qc(
            (Slot(SLOT.get() + 1), j),
            entry,
            &quorum(),
            &validator_data,
        ))
    });
    assert_eq!(
        entries_of(&forged),
        block.entries(),
        "the test needs a response that matches the request"
    );

    instance.handle_message(nodes()[1], block_response(forged));
    assert!(drain(&mut instance).is_empty());
    assert!(
        instance.decision().is_none(),
        "a certified entry that is not bound to this slot is no proof"
    );

    // and the real block still decides afterwards.
    instance.handle_message(nodes()[1], block_response(block.clone()));
    drain(&mut instance);
    assert_eq!(instance.decision(), Some(&block.entries()));
}

#[test]
fn an_unsolicited_block_response_is_ignored() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // valid in every way, but nothing asked for it: taking it in would be a
    // way to fill this instance's store.
    instance.handle_message(nodes()[1], block_response(block.clone()));
    assert!(drain(&mut instance).is_empty());

    // so the certificate that arrives afterwards still has to fetch it.
    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    let outputs = drain(&mut instance);

    assert_eq!(requested_entries(&outputs), vec![block.entries()]);
    assert!(instance.decision().is_none());
}

#[test]
fn a_holder_answers_a_block_request_with_a_unicast() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    let asker = nodes()[1];
    instance.handle_message(asker, block_request(&block.entries()));
    let outputs = drain(&mut instance);

    assert_eq!(
        unicasts(&outputs),
        vec![(asker, &block_response(block.clone()))],
        "the block goes back to the sender alone"
    );
    assert!(broadcasts(&outputs).is_empty());

    // and a request for entries it holds no block for is answered with nothing.
    let other = metablock(2, &validator_data);
    instance.handle_message(asker, block_request(&other.entries()));
    assert!(drain(&mut instance).is_empty());
}

#[test]
fn a_block_is_only_requested_once() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    assert_eq!(
        requested_entries(&drain(&mut instance)),
        vec![block.entries()]
    );

    // the same certificate again, and the commit votes it aggregates on top.
    instance.handle_message(
        nodes()[2],
        commit_qc_message(view(1), &block, &validator_data),
    );
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert!(
        requested_entries(&outputs).is_empty(),
        "one request is outstanding; repeating it would flood the network"
    );
}

#[test]
fn the_view_timing_out_asks_again_for_a_pending_block() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    assert_eq!(
        requested_entries(&drain(&mut instance)),
        vec![block.entries()]
    );

    // the request, or the response to it, went missing.
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);

    assert_eq!(timed_out_view(&outputs), Some(view(1)));
    assert_eq!(
        requested_entries(&outputs),
        vec![block.entries()],
        "the timer is what makes this instance ask a second time"
    );
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
fn a_certificate_arriving_before_propose_is_fetched_once_it_does() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];
    assert_ne!(leader_of(view(1)), follower, "test assumes a follower");

    let mut instance = mvba(follower, &validator_data);

    // the certificate is recorded -- it is valid evidence whenever it arrives --
    // but an instance with no input does not participate, so it sends nothing:
    // not a request, and not the echo.
    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    assert!(drain(&mut instance).is_empty());
    assert!(instance.decision().is_none());

    // proposing starts participation, and the stored certificate is the first
    // thing it finds it needs a block for.
    instance.propose(own.clone(), Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);

    assert_eq!(
        requested_entries(&outputs),
        vec![block.entries()],
        "the fetch waits for participation, not for a fresh certificate"
    );
    assert!(
        instance.decision().is_none(),
        "the block is not here yet, so there is nothing to hand on"
    );

    instance.handle_message(nodes()[1], block_response(block.clone()));
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block.entries()));
    assert_eq!(instance.decision_block(), Some(&block));
    assert!(decided_commit_qc(&outputs));
}

#[test]
fn a_certificate_over_this_validators_own_input_decides_on_propose() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = mvba(follower, &validator_data);

    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &own, &validator_data),
    );
    assert!(drain(&mut instance).is_empty());

    // the certificate settled the entries of this validator's own input, so
    // proposing hands the block to the store and the decision falls out of the
    // same call: nothing is fetched.
    instance.propose(own.clone(), Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&own.entries()));
    assert_eq!(instance.decision_block(), Some(&own));
    assert!(decided_commit_qc(&outputs));
    assert!(requested_entries(&outputs).is_empty());
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
    instance.propose(block.clone(), Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);
    assert_eq!(prepared_entries(&outputs), Some(block.entries()));
}

#[test]
fn views_are_one_indexed() {
    assert_eq!(FallbackView::FIRST, view(1));
    assert!(FallbackView::GENESIS < FallbackView::FIRST);
}
