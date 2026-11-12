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
            types::{ProposalMap, Slot, VoteMsg},
        },
        FallbackView, Metablock, Mvba,
    },
    messages::FallbackCommitVote,
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

    assert_eq!(
        instance.decision(),
        Some(&block),
        "the block came with the proposal this validator accepted"
    );
    let decision_proof = instance
        .decision_proof()
        .expect("a decision comes with its certificate");
    assert_eq!(decision_proof.verdict.0, block.entries());
    assert!(
        decided_commit_qc(&outputs),
        "the certificate is passed on so others can decide"
    );
    assert!(
        requested_entries(&outputs).is_empty(),
        "nothing to fetch: the accepted proposal carried the block"
    );
}

/// A commit quorum can complete before this validator has even seen a prepare
/// certificate; the certificate alone decides
#[test]
fn a_commit_quorum_decides_while_still_preparing() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);
    assert_eq!(prepared_entries(&outputs), Some(block.entries()));

    // no prepare votes arrive: the commit quorum alone decides
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block));
    assert!(decided_commit_qc(&outputs));
}

#[test]
fn a_quorum_that_arrives_before_the_proposal_fires_on_arrival() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    // a different input, so this validator holds no metablock with the voted
    // entries until something carries one to it
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    let outputs = drain(&mut instance);
    assert!(broadcasts(&outputs).is_empty());

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
    assert_eq!(instance.decision(), Some(&block));
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
    // so the certificate settles entries whose block it does not hold
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert_eq!(
        requested_entries(&outputs),
        vec![block.entries()],
        "agreement is done; the block behind the entries is not here yet"
    );
    assert!(instance.decision().is_none());
    assert!(instance.decision_proof().is_none());

    instance.handle_message(nodes()[1], block_response(block.clone()));
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block));
    assert_eq!(
        instance
            .decision_proof()
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
fn a_refused_proposal_still_yields_the_certificate_it_carried() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // a view-1 certificate justifies view 2, not view 3, so the proposal is
    // refused -- but the certificate it carried is evidence in its own right
    // and the view it certifies has still been left
    let tc = timeout_certificate(view(1), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(3), &block, Some(tc));
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))),
        "the harvested certificate advances the view it certifies"
    );
    assert_eq!(
        prepared_entries(&outputs),
        None,
        "the proposal itself is refused, so nothing is voted on"
    );
}

#[test]
fn f_plus_one_timeouts_pull_this_validator_into_timing_out() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // two of four validators is f+1 here, and the timer has not fired
    feed_timeouts(&mut instance, view(1), None, &nodes()[1..3]);
    let outputs = drain(&mut instance);

    assert_eq!(timed_out_view(&outputs), Some(view(1)));
}

#[test]
fn a_timeout_quorum_advances_the_view_and_the_new_leader_proposes() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let next_leader = leader_of(view(2));
    let mut instance = started(next_leader, &block, &validator_data);
    drain(&mut instance);

    feed_timeouts(&mut instance, view(1), None, &quorum());
    let outputs = drain(&mut instance);

    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))));
    let proposal = proposed(&outputs).expect("the leader of view 2 proposes on entering it");
    assert_eq!(proposal.view, view(2));
    assert_eq!(
        proposal.value, block,
        "nothing is locked, so the leader proposes its own input"
    );
    assert!(
        matches!(proposal.justification, Justification::Tc(_)),
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
    assert_eq!(proposal.value, block);
    assert_eq!(
        proposal.justification,
        Justification::FallbackCert(Some(cert)),
        "view 1 has no timeout certificate to be justified by, so it carries \
         the certificate that admitted the path instead"
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
    // to the fallback path, not this one
    let foreign = strong_qc(
        Slot(SLOT.get() + 1),
        EnterFallbackVote,
        &quorum(),
        &validator_data,
    );
    let (leader, proposal) = pre_prepare_with_cert(view(1), &block, Some(foreign));

    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        None,
        "a carried certificate is checked, so an out-of-scope one is not voted on"
    );
}

#[test]
fn a_proposal_carrying_a_timeout_certificate_for_another_slot_is_rejected() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // genuinely aggregated, and verifiable against the slot it names -- which
    // is not this one. neither harvested nor taken as justification
    let foreign = timeout_certificate_in(Slot(SLOT.get() + 1), view(1), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(2), &block, Some(foreign));

    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert!(
        !scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))),
        "another slot's certificate is no evidence this slot left its view"
    );
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
    // certificate, and every entry here is a `FastQc`
    let (leader, proposal) = pre_prepare_with_cert(view(1), &block, None);

    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        Some(block.entries()),
        "an absent certificate is legal: the value decides, not the carrier"
    );
}

#[test]
fn a_fast_metablock_is_the_only_thing_an_absent_certificate_admits() {
    let validator_data = validator_data();
    let block = mixed_evidence_metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // one entry short of a `FastQc`, so nothing here stands in for the
    // certificate that admits the slot to the fallback path
    let (leader, proposal) = pre_prepare_with_cert(view(1), &block, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        None,
        "`fbcert = ⊥` is legal only over a fast metablock"
    );

    // the same value with a certificate beside it: mixed evidence is fine
    // once something admits the path, so it is the ⊥ arm that refused above
    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert_eq!(
        prepared_entries(&outputs),
        Some(block.entries()),
        "a carried certificate asks nothing of the entries"
    );
}

#[test]
fn a_leader_proposes_once_in_a_view() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let leader = leader_of(view(1));
    let mut instance = started(leader, &block, &validator_data);
    let outputs = drain(&mut instance);
    proposed(&outputs).expect("the leader of view 1 proposes on entering it");

    // its own pre-prepare comes back to it, and the votes for it follow: every
    // one of them runs the state machine again
    let (_, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    let outputs = drain(&mut instance);

    assert!(
        proposed(&outputs).is_none(),
        "the proposer check ran on entering the view and does not run again"
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
    // the prefetch on adopting the lock is the one request; `want` dedups the
    // passes that follow, and the retransmit timer carries it from here
    assert_eq!(
        requested_entries(&outputs),
        vec![locked.entries()],
        "the lock names entries whose block the leader does not hold"
    );
    assert!(scheduled_timers(&outputs).contains(&TimerEvent::BlockRetransmit(locked.entries())));
    assert!(
        proposed(&outputs).is_none(),
        "it may only propose the locked value, and does not have it yet"
    );

    instance.handle_message(nodes()[1], block_response(locked.clone()));
    let outputs = drain(&mut instance);

    let proposal = proposed(&outputs).expect("the block landed, so the leader can propose");
    assert_eq!(proposal.view, view(2));
    assert_eq!(
        proposal.value.entries(),
        locked.entries(),
        "the leader is bound to the value the previous view may have locked"
    );
    assert_eq!(
        proposal.value, locked,
        "and it proposes the retrieved block verbatim"
    );
}

#[test]
fn a_formed_commit_certificate_is_final_against_later_votes() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let other = metablock(2, &validator_data);

    let mut collectors = ViewCollectors::new(SLOT, view(1));
    for node in quorum() {
        let msg = VoteMsg::new_signed(
            (SLOT, view(1)),
            FallbackCommitVote(block.entries()),
            &node.keypair(),
        );
        collectors.store_commit_vote(node, msg);
    }
    let first = collectors
        .try_form_commit_qc(&validator_data)
        .expect("a supermajority forms the certificate");

    // a late vote, even one for other entries, changes nothing: the pool is
    // sealed by the first quorum
    let late = nodes()[3];
    let msg = VoteMsg::new_signed(
        (SLOT, view(1)),
        FallbackCommitVote(other.entries()),
        &late.keypair(),
    );
    collectors.store_commit_vote(late, msg);

    assert_eq!(
        collectors.try_form_commit_qc(&validator_data),
        Some(first),
        "the sealed certificate is what every later call returns"
    );
}

/// Timeouts abandoning the same view while holding different locks still
/// aggregate: the quorum is over all of them, and each lock is its own group
#[test]
fn timeouts_claiming_different_locks_aggregate_into_one_certificate() {
    let validator_data = validator_data();
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);

    let mut collectors = ViewCollectors::new(SLOT, view(2));
    for (i, node) in quorum().into_iter().enumerate() {
        // one of the three holds the lock; the other two hold none
        let carried = (i == 2).then(|| qc.clone());
        collectors.store_timeout(
            node,
            TimeoutMsg::new_signed(SLOT, view(2), carried, &node.keypair()),
        );
    }

    let tc = collectors
        .try_form_tc(&validator_data)
        .expect("three of four is a supermajority however they are split");

    assert_eq!(tc.groups.len(), 2, "one signature collection per claim");
    assert_eq!(
        tc.high_prep_qc,
        Some(qc),
        "the highest lock any group claims rides along"
    );
    assert!(tc.verify(&validator_data));
}

/// A well-formed signature that does not verify is not aggregated, so the
/// certificate a validator forms locally is one every receiver accepts
#[test]
fn a_timeout_signed_with_garbage_is_left_out_of_the_certificate() {
    let validator_data = validator_data();

    let mut collectors = ViewCollectors::new(SLOT, view(2));
    for node in nodes() {
        let mut msg = TimeoutMsg::new_signed(SLOT, view(2), None, &node.keypair());
        if node == nodes()[3] {
            msg.vote.signature.make_invalid();
        }
        collectors.store_timeout(node, msg);
    }

    let tc = collectors
        .try_form_tc(&validator_data)
        .expect("the three honest timeouts are a supermajority on their own");

    assert_eq!(tc.groups.len(), 1);
    assert!(
        tc.verify(&validator_data),
        "a certificate carrying an unverifiable signature would be refused by every receiver"
    );
}

/// A correct validator rebuilds its retransmitted timeout around the lock it
/// holds now, so a later timeout claiming a higher lock displaces the one the
/// receiver stored -- otherwise the certificate would bind the next leader to
/// less than the network provably knows
#[test]
fn a_later_timeout_claiming_a_higher_lock_replaces_the_first() {
    let validator_data = validator_data();
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);

    let mut collectors = ViewCollectors::new(SLOT, view(2));
    for node in quorum() {
        collectors.store_timeout(
            node,
            TimeoutMsg::new_signed(SLOT, view(2), None, &node.keypair()),
        );
    }

    let raised = quorum()[2];
    collectors.store_timeout(
        raised,
        TimeoutMsg::new_signed(SLOT, view(2), Some(qc.clone()), &raised.keypair()),
    );

    let tc = collectors
        .try_form_tc(&validator_data)
        .expect("the three senders are a supermajority however they are split");

    assert_eq!(
        tc.groups.len(),
        2,
        "the sender left the lockless group for one of its own"
    );
    assert_eq!(
        tc.high_prep_qc,
        Some(qc),
        "the raised lock is what the certificate binds the next leader to"
    );
    assert!(tc.verify(&validator_data));
}

/// Replacement is unconditional: a retraction moves the sender to the lockless
/// group, but the lock still rides on the other claimants, and repeating the
/// same claim changes nothing. Only the retractor's own claim is lost, which
/// it could as well have withheld from the start
#[test]
fn a_later_timeout_replaces_the_first_whatever_it_claims() {
    let validator_data = validator_data();
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);

    // `None` is the retraction, the same certificate again the no-op repeat
    for retransmission in [None, Some(qc.clone())] {
        let mut collectors = ViewCollectors::new(SLOT, view(2));
        for node in quorum() {
            collectors.store_timeout(
                node,
                TimeoutMsg::new_signed(SLOT, view(2), Some(qc.clone()), &node.keypair()),
            );
        }

        let sender = quorum()[2];
        let retracted = retransmission.is_none();
        collectors.store_timeout(
            sender,
            TimeoutMsg::new_signed(SLOT, view(2), retransmission, &sender.keypair()),
        );

        let tc = collectors
            .try_form_tc(&validator_data)
            .expect("the three senders are a supermajority however they are split");

        assert_eq!(
            tc.groups.len(),
            if retracted { 2 } else { 1 },
            "a retraction moves the sender to the lockless group, a repeat is a no-op"
        );
        assert_eq!(
            tc.high_prep_qc,
            Some(qc.clone()),
            "the other senders still claim the lock"
        );
        assert!(tc.verify(&validator_data));
    }
}

/// The prepare certificate a timeout carries is verified at ingress, so it is
/// taken as a lock right away rather than waiting for the timeout certificate
/// that will eventually carry it
#[test]
fn a_carried_certificate_raises_this_validator_lock_on_arrival() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);

    let follower = nodes()[0];
    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // a single timeout: below f+1, so nothing here pulls the validator into
    // timing out and only the carried certificate can have any effect
    feed_timeouts(&mut instance, view(1), Some(qc.clone()), &nodes()[1..2]);
    assert!(timed_out_view(&drain(&mut instance)).is_none());

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);

    let timeout = timeout_message(&outputs).expect("the timer fires this validator's own timeout");
    assert_eq!(
        timeout.high_prep_qc,
        Some(qc),
        "the lock arrived on someone else's timeout and was taken as this validator's own"
    );
}

#[test]
fn the_view_timer_makes_this_validator_time_out() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(7)));
    assert!(drain(&mut instance).is_empty());

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));

    // a further fire in the timed-out view retransmits the timeout: the
    // timeout protocol is its own recovery path, so a lost one is re-sent
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));
    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(1))));
}

#[test]
fn timing_out_re_arms_the_view_timer() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);

    assert_eq!(timed_out_view(&outputs), Some(view(1)));
    assert_eq!(
        scheduled_timers(&outputs),
        vec![TimerEvent::ViewTimeout(view(1))],
        "the fire it consumed is replaced, so retransmission keeps running"
    );
}

#[test]
fn an_echo_triggered_timeout_re_arms_the_view_timer() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // f+1 stake timed out; this validator owes its own timeout without its
    // timer having fired
    feed_timeouts(&mut instance, view(1), None, &nodes()[1..3]);
    let outputs = drain(&mut instance);

    assert_eq!(timed_out_view(&outputs), Some(view(1)));
    assert_eq!(
        scheduled_timers(&outputs),
        vec![TimerEvent::ViewTimeout(view(1))],
        "the arm replaces the view's still-pending timer, keeping one live"
    );

    // that timer is what drives the first retransmission
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));
    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(1))));
}

#[test]
fn the_echo_never_retransmits() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    drain(&mut instance);

    feed_timeouts(&mut instance, view(1), None, &nodes()[1..3]);

    assert!(
        timed_out_view(&drain(&mut instance)).is_none(),
        "only a timer fire retransmits; the echo condition never clears"
    );
}

#[test]
fn retransmission_repeats_pending_block_requests() {
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
    let outputs = drain(&mut instance);
    assert_eq!(requested_entries(&outputs), vec![block.entries()]);
    assert!(scheduled_timers(&outputs).contains(&TimerEvent::BlockRetransmit(block.entries())));

    instance.handle_timer(TimerEvent::BlockRetransmit(block.entries()));
    let outputs = drain(&mut instance);
    assert_eq!(requested_entries(&outputs), vec![block.entries()]);
    assert!(scheduled_timers(&outputs).contains(&TimerEvent::BlockRetransmit(block.entries())));

    // fetching runs on its own cadence: a view that runs out re-sends this
    // validator's timeout and nothing else
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));
    assert!(requested_entries(&outputs).is_empty());
}

#[test]
fn a_retransmitted_timeout_carries_the_current_lock() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(
        timeout_message(&outputs)
            .expect("the timer fires this validator's timeout")
            .high_prep_qc,
        None,
        "nothing is locked yet"
    );

    let qc = prepare_qc(view(1), &block.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc.clone()), &nodes()[1..2]);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(
        timeout_message(&outputs)
            .expect("the retransmission repeats the broadcast")
            .high_prep_qc,
        Some(qc),
        "the retransmission is rebuilt around the lock held now"
    );
}

#[test]
fn a_lookback_view_timeout_still_raises_the_lock() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    feed_timeouts(&mut instance, view(1), None, &quorum());
    drain(&mut instance);

    // one view behind now, and carrying a lock this node never saw
    let qc = prepare_qc(view(1), &block.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc.clone()), &nodes()[3..4]);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(2)));
    let outputs = drain(&mut instance);
    assert_eq!(
        timeout_message(&outputs)
            .expect("the timer fires this validator's timeout")
            .high_prep_qc,
        Some(qc),
        "adoption is a monotone max, so a lookback-view carrier still raises it"
    );
}

#[test]
fn advancing_views_stops_retransmitting_the_old_view() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    drain(&mut instance);

    feed_timeouts(&mut instance, view(1), None, &quorum());
    let outputs = drain(&mut instance);
    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))));

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    assert!(
        drain(&mut instance).is_empty(),
        "a left view is never retransmitted for"
    );
}

#[test]
fn a_certificate_preempts_a_retransmission_in_the_same_pass() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    drain(&mut instance);

    feed_timeouts(&mut instance, view(1), None, &quorum());
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);

    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))));
    assert_eq!(
        timed_out_view(&outputs),
        None,
        "the view it would retransmit for is already left"
    );
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
    // settles the entries, and block sync has to supply the rest
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

    assert_eq!(instance.decision(), Some(&block));
    assert!(instance.decision_proof().is_some());
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

    instance.handle_message(nodes()[1], block_response(other));
    assert!(drain(&mut instance).is_empty());
    assert!(instance.decision().is_none());

    // the entries asked for, carried by certificates bound to another slot:
    // the entries a `FastQc` certifies are its verdict, so tampering with its
    // scope leaves the identity of the block intact
    let block_entries = block.entries();
    let forged = Metablock::new(ProposalMap::new(NUM_PROPOSALS, |j| {
        let entry = block_entries[j].clone();
        CertifiedEntry::FastQc(strong_qc(
            (Slot(SLOT.get() + 1), j),
            entry,
            &quorum(),
            &validator_data,
        ))
    }));
    assert_eq!(
        forged.entries(),
        block.entries(),
        "the test needs a response that matches the request"
    );

    instance.handle_message(nodes()[1], block_response(forged));
    assert!(drain(&mut instance).is_empty());
    assert!(
        instance.decision().is_none(),
        "a certified entry that is not bound to this slot is no proof"
    );

    instance.handle_message(nodes()[1], block_response(block.clone()));
    drain(&mut instance);
    assert_eq!(instance.decision(), Some(&block));
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
    // way to fill this instance's store
    instance.handle_message(nodes()[1], block_response(block.clone()));
    assert!(drain(&mut instance).is_empty());

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
        vec![(asker, &block_response(block))],
        "the block goes back to the sender alone"
    );
    assert!(broadcasts(&outputs).is_empty());

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
fn each_want_arms_its_own_retransmit_timer() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let locked = metablock(3, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    let outputs = drain(&mut instance);
    assert_eq!(requested_entries(&outputs), vec![block.entries()]);
    assert_eq!(
        scheduled_timers(&outputs),
        vec![TimerEvent::BlockRetransmit(block.entries())],
        "a want arms the timer that re-drives it"
    );

    // a lock on a second missing block: its own want on its own timer, so a
    // request registered late is never paced by one registered early
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc), &nodes()[1..2]);
    let outputs = drain(&mut instance);

    assert_eq!(requested_entries(&outputs), vec![locked.entries()]);
    assert_eq!(
        scheduled_timers(&outputs),
        vec![TimerEvent::BlockRetransmit(locked.entries())],
        "the second want arms a timer of its own"
    );
}

#[test]
fn the_retransmit_timer_re_arms_while_its_request_is_pending() {
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
    drain(&mut instance);

    for _ in 0..2 {
        instance.handle_timer(TimerEvent::BlockRetransmit(block.entries()));
        let outputs = drain(&mut instance);

        assert_eq!(requested_entries(&outputs), vec![block.entries()]);
        assert_eq!(
            scheduled_timers(&outputs),
            vec![TimerEvent::BlockRetransmit(block.entries())],
            "the request is still outstanding, so the timer stays alive"
        );
    }
}

#[test]
fn the_retransmit_timer_dies_once_its_block_arrives() {
    let validator_data = validator_data();
    let locked = metablock(3, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // a lock, not a commit QC: the fetch completes without deciding, so the
    // fire lands on a running instance that no longer wants the block
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc), &nodes()[1..2]);
    assert_eq!(
        requested_entries(&drain(&mut instance)),
        vec![locked.entries()]
    );

    instance.handle_message(nodes()[1], block_response(locked.clone()));
    drain(&mut instance);
    assert_eq!(instance.decision(), None);

    // the trailing fire the response could not cancel
    instance.handle_timer(TimerEvent::BlockRetransmit(locked.entries()));
    let outputs = drain(&mut instance);

    assert!(requested_entries(&outputs).is_empty());
    assert!(
        scheduled_timers(&outputs).is_empty(),
        "the block is held, so the timer is not re-armed"
    );
}

#[test]
fn a_fire_retransmits_only_its_own_block() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let locked = metablock(3, &validator_data);
    let own = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc), &nodes()[1..2]);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::BlockRetransmit(block.entries()));
    let outputs = drain(&mut instance);

    assert_eq!(
        requested_entries(&outputs),
        vec![block.entries()],
        "one fire re-sends one request; the other want keeps its own schedule"
    );
    assert_eq!(
        scheduled_timers(&outputs),
        vec![TimerEvent::BlockRetransmit(block.entries())],
    );
}

#[test]
fn a_decided_instance_lets_the_retransmit_timer_die() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let locked = metablock(3, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // a lock on a block this validator never gets, so the fetch outlives the
    // decision it does reach
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    feed_timeouts(&mut instance, view(1), Some(qc), &nodes()[1..2]);
    assert_eq!(
        requested_entries(&drain(&mut instance)),
        vec![locked.entries()]
    );

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    drain(&mut instance);
    assert_eq!(instance.decision(), Some(&block));

    instance.handle_timer(TimerEvent::BlockRetransmit(locked.entries()));
    let outputs = drain(&mut instance);

    assert!(requested_entries(&outputs).is_empty());
    assert!(
        scheduled_timers(&outputs).is_empty(),
        "a decided instance stops fetching: it holds the one block it needed"
    );
}

#[test]
fn abandon_silences_the_block_retransmit() {
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
    drain(&mut instance);

    instance.abandon();
    instance.handle_timer(TimerEvent::BlockRetransmit(block.entries()));

    assert!(drain(&mut instance).is_empty());
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

    let tc = timeout_certificate(view(1), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(2), &other, Some(tc));
    instance.handle_message(leader, proposal);
    feed_timeouts(&mut instance, view(1), None, &quorum());
    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));

    // a decided instance takes no catch-up either: the window stays hard
    let far = timeout_certificate(view(12), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(13), &other, Some(far));
    instance.handle_message(leader, proposal);

    assert!(drain(&mut instance).is_empty(), "a decision is terminal");
    assert_eq!(instance.decision(), Some(&block), "and it never changes");
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
    // not a request, and not the echo
    instance.handle_message(
        nodes()[1],
        commit_qc_message(view(1), &block, &validator_data),
    );
    assert!(drain(&mut instance).is_empty());
    assert!(instance.decision().is_none());

    instance.propose(own, Some(enter_fallback_cert(&validator_data)));
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

    assert_eq!(instance.decision(), Some(&block));
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
    // same call: nothing is fetched
    instance.propose(own.clone(), Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&own));
    assert!(decided_commit_qc(&outputs));
    assert!(requested_entries(&outputs).is_empty());
}

#[test]
fn nothing_is_sent_before_propose() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let mut instance = mvba(leader_of(view(1)), &validator_data);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    assert!(drain(&mut instance).is_empty());

    instance.propose(block.clone(), Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);
    assert_eq!(prepared_entries(&outputs), Some(block.entries()));
}

#[test]
fn views_are_one_indexed() {
    assert_eq!(FallbackView::FIRST, view(1));
    assert!(FallbackView::GENESIS < FallbackView::FIRST);
}

#[test]
fn a_timed_out_view_neither_votes_nor_raises_its_lock() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);
    assert_eq!(
        prepared_entries(&outputs),
        Some(block.entries()),
        "it voted to prepare before the view ran out"
    );

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));

    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    let outputs = drain(&mut instance);
    assert!(
        committed_entries(&outputs).is_none(),
        "a validator that has sent its timeout casts no further vote in the view"
    );

    // the lock it reports is the one it held when it timed out: adopting a
    // certificate formed afterwards would put the view's timeout certificate
    // below what a commit quorum there could have been built on
    feed_timeouts(&mut instance, view(1), None, &quorum());
    drain(&mut instance);
    instance.handle_timer(TimerEvent::ViewTimeout(view(2)));
    let outputs = drain(&mut instance);

    let timeout = timeout_message(&outputs).expect("the next view runs out too");
    assert_eq!(timeout.view(), view(2));
    assert!(
        timeout.high_prep_qc.is_none(),
        "the certificate that completed after the timeout did not become its lock"
    );
}

#[test]
fn a_timed_out_view_still_decides_what_it_accepted() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    // not this validator's own input, so nothing but the accepted phase keeps
    // the block reachable once the view times out
    let block = metablock(2, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::ViewTimeout(view(1)));
    let outputs = drain(&mut instance);
    assert_eq!(timed_out_view(&outputs), Some(view(1)));

    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert_eq!(
        instance.decision(),
        Some(&block),
        "timing out stops this validator voting, never deciding"
    );
    assert!(
        decided_commit_qc(&outputs),
        "and the certificate is passed on as from any other phase"
    );
}

/// A receiver that stored a sender's lockless timeout still gets its raised
/// claim into the certificate, whatever order the quorum completes in
#[test]
fn a_retransmitted_timeout_with_a_raised_lock_reaches_the_certificate() {
    let validator_data = validator_data();
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);

    let mut collectors = ViewCollectors::new(SLOT, view(2));
    for node in &quorum()[..2] {
        collectors.store_timeout(
            *node,
            TimeoutMsg::new_signed(SLOT, view(2), None, &node.keypair()),
        );
    }

    let raised = quorum()[0];
    collectors.store_timeout(
        raised,
        TimeoutMsg::new_signed(SLOT, view(2), Some(qc.clone()), &raised.keypair()),
    );

    let last = quorum()[2];
    collectors.store_timeout(
        last,
        TimeoutMsg::new_signed(SLOT, view(2), None, &last.keypair()),
    );

    let tc = collectors
        .try_form_tc(&validator_data)
        .expect("three of four is a supermajority");

    assert_eq!(
        tc.high_prep_qc,
        Some(qc),
        "the next leader is bound to the newest lock the retransmission spread"
    );
    assert!(tc.verify(&validator_data));
}

// ---------------- adopting a lock ----------------

#[test]
fn adopting_a_lock_requests_the_block_behind_it() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);

    let follower = nodes()[0];
    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    // a single timeout: below f+1, so nothing here pulls this validator into
    // timing out and only the carried certificate can have any effect
    feed_timeouts(&mut instance, view(1), Some(qc), &nodes()[1..2]);
    let outputs = drain(&mut instance);

    assert_eq!(
        requested_entries(&outputs),
        vec![locked.entries()],
        "a lock is fetched when it is adopted, not when a view forces the question"
    );
}

#[test]
fn an_adopted_lock_over_a_held_block_requests_nothing() {
    let validator_data = validator_data();
    let own = metablock(1, &validator_data);
    let qc = prepare_qc(view(1), &own.entries(), &validator_data);

    let follower = nodes()[0];
    let mut instance = started(follower, &own, &validator_data);
    drain(&mut instance);

    feed_timeouts(&mut instance, view(1), Some(qc), &nodes()[1..2]);
    let outputs = drain(&mut instance);

    assert!(
        requested_entries(&outputs).is_empty(),
        "the lock names this validator's own input; there is nothing to fetch"
    );
}

// ---------------- the decided echo ----------------

#[test]
fn deciding_arms_the_decided_echo() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let mut instance = decided(nodes()[0], &block, &validator_data);
    let outputs = drain(&mut instance);

    assert!(instance.decision().is_some());
    assert!(decided_commit_qc(&outputs));
    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::DecidedEcho),
        "one broadcast is not something a laggard can rely on having seen"
    );
}

#[test]
fn the_decided_echo_rebroadcasts_the_commit_certificate_and_re_arms() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let mut instance = decided(nodes()[0], &block, &validator_data);
    drain(&mut instance);

    for _ in 0..2 {
        instance.handle_timer(TimerEvent::DecidedEcho);
        let outputs = drain(&mut instance);

        assert!(decided_commit_qc(&outputs));
        assert_eq!(scheduled_timers(&outputs), vec![TimerEvent::DecidedEcho]);
    }
}

#[test]
fn abandon_silences_the_decided_echo() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let mut instance = decided(nodes()[0], &block, &validator_data);
    drain(&mut instance);

    instance.abandon();
    instance.handle_timer(TimerEvent::DecidedEcho);

    assert!(
        drain(&mut instance).is_empty(),
        "the fast path taking the slot over is what ends the obligation"
    );
}

/// The echo timer is armed only at the moment of decision, so one firing
/// earlier is a wiring bug, not a race to tolerate
#[test]
#[should_panic(expected = "echo decision timer scheduled while not in decided state")]
fn a_decided_echo_before_any_decision_panics() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);

    let mut instance = started(nodes()[0], &block, &validator_data);
    drain(&mut instance);

    instance.handle_timer(TimerEvent::DecidedEcho);
}

/// The whole recovery loop: the echo is the discovery step, and the block half
/// stays pull-based behind it
#[test]
fn a_laggard_catches_up_on_the_echo() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let own = metablock(2, &validator_data);

    let mut decider = decided(nodes()[0], &block, &validator_data);
    drain(&mut decider);

    // it holds a different input and never saw the proposal or the votes: the
    // one-shot broadcast at decision time is gone, so nothing it holds says the
    // slot was settled
    let laggard_id = nodes()[2];
    let mut laggard = started(laggard_id, &own, &validator_data);
    drain(&mut laggard);
    assert!(laggard.decision().is_none());

    decider.handle_timer(TimerEvent::DecidedEcho);
    let outputs = drain(&mut decider);
    let echo = broadcasts(&outputs)
        .into_iter()
        .find(|message| matches!(message, Message::CommitQc(_)))
        .expect("the echo re-broadcasts the certificate")
        .clone();

    laggard.handle_message(nodes()[0], echo);
    let outputs = drain(&mut laggard);
    assert_eq!(
        requested_entries(&outputs),
        vec![block.entries()],
        "the certificate settles the entries; the block is asked for"
    );

    decider.handle_message(laggard_id, block_request(&block.entries()));
    let outputs = drain(&mut decider);
    let (to, response) = unicasts(&outputs)
        .into_iter()
        .next()
        .expect("a decided instance answers block requests in every phase");
    assert_eq!(to, laggard_id);
    let response = response.clone();

    laggard.handle_message(nodes()[0], response);
    drain(&mut laggard);

    assert_eq!(laggard.decision(), Some(&block));
    assert!(laggard.decision_proof().is_some());
}

// ---------------- admission window ----------------

#[test]
fn a_pre_prepare_beyond_the_window_catches_a_lagging_node_up() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // 13 is past view 1 + MAX_FUTURE_VIEWS, but the certificate it carries is
    // proof the views below it were left
    let tc = timeout_certificate(view(12), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(13), &block, Some(tc));
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(13))),
        "the certificate jumps this instance to the proposal's view"
    );
    assert_eq!(
        prepared_entries(&outputs),
        Some(block.entries()),
        "the jump and the proposal acceptance land in the same call"
    );
}

#[test]
fn a_refused_far_ahead_proposal_still_yields_its_certificate() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    // a view-12 certificate justifies view 13, not 14; both are past the window
    let tc = timeout_certificate(view(12), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(14), &block, Some(tc));
    instance.handle_message(leader, proposal);
    let outputs = drain(&mut instance);

    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(13))),
        "the harvested certificate advances the view it certifies"
    );
    assert_eq!(
        prepared_entries(&outputs),
        None,
        "the proposal itself is refused, so nothing is voted on"
    );
}

#[test]
fn a_commit_quorum_completing_after_a_view_change_still_decides() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_timeouts(&mut instance, view(1), None, &quorum());
    let outputs = drain(&mut instance);
    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(2))),
        "the certificate for view 1 moves this instance on"
    );

    // votes cast before the view change are still in flight when it happens
    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert_eq!(instance.decision(), Some(&block));
    assert!(
        decided_commit_qc(&outputs),
        "the quorum completed one view back, and still decides"
    );
}

#[test]
fn commit_votes_two_views_back_are_beyond_the_lookback() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = started(follower, &block, &validator_data);
    drain(&mut instance);

    let (leader, proposal) = pre_prepare(view(1), &block, None);
    instance.handle_message(leader, proposal);
    feed_timeouts(&mut instance, view(1), None, &quorum());
    feed_timeouts(&mut instance, view(2), None, &quorum());
    drain(&mut instance);

    feed_commit_votes(&mut instance, view(1), &block, &quorum());
    let outputs = drain(&mut instance);

    assert!(
        instance.decision().is_none(),
        "the lookback is one view, not an unbounded tail"
    );
    assert!(!decided_commit_qc(&outputs));
}

#[test]
fn a_pre_input_instance_buffers_a_catch_up_only_inside_the_window() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    // nothing consumes an out-of-window certificate before `propose`, so the
    // window stays hard here
    let mut instance = mvba(follower, &validator_data);
    let tc = timeout_certificate(view(12), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(13), &block, Some(tc));
    instance.handle_message(leader, proposal);

    instance.propose(block, Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);

    assert!(scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(1))));
    assert!(
        !scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(13))),
        "an instance that cannot advance stores nothing above its window"
    );
}

#[test]
fn a_buffered_in_window_certificate_jumps_at_propose() {
    let validator_data = validator_data();
    let block = metablock(1, &validator_data);
    let follower = nodes()[0];

    let mut instance = mvba(follower, &validator_data);
    let tc = timeout_certificate(view(5), None, &validator_data);
    let (leader, proposal) = pre_prepare(view(6), &block, Some(tc));
    instance.handle_message(leader, proposal);

    instance.propose(block.clone(), Some(enter_fallback_cert(&validator_data)));
    let outputs = drain(&mut instance);

    assert!(
        scheduled_timers(&outputs).contains(&TimerEvent::ViewTimeout(view(6))),
        "in-window buffering still cascades once the instance can run"
    );
    assert_eq!(prepared_entries(&outputs), Some(block.entries()));
}

#[test]
fn a_signer_in_two_groups_invalidates_the_certificate() {
    let validator_data = validator_data();
    let locked = metablock(2, &validator_data);
    let qc = prepare_qc(view(1), &locked.entries(), &validator_data);
    let with_lock = timeout_certificate(view(2), Some(qc.clone()), &validator_data);
    let without = timeout_certificate(view(2), None, &validator_data);
    assert!(with_lock.verify(&validator_data) && without.verify(&validator_data));

    // every signer now backs both the lockless and the locked claim
    let mut groups = without.groups;
    groups.extend(with_lock.groups.iter().cloned());
    let tc = TimeoutCertificate {
        slot: SLOT,
        view: view(2),
        groups,
        high_prep_qc: Some(qc),
    };

    assert!(
        !tc.verify(&validator_data),
        "a signer in two groups is equivocation, not extra stake"
    );
}

// ---------------- the genericity pin ----------------

/// A second value type, with nothing of a metablock about it: its projection is
/// a bare integer and its validation context is `()`
///
/// The suite above only ever instantiates the MVBA at `V = Metablock`, so it
/// cannot notice a metablock-specific assumption creeping back into the generic
/// code. Instantiating it here at a value type that shares none of those
/// properties is what does
mod toy_value {
    use std::sync::Arc;

    use super::super::{
        super::{
            super::types::{NodeId, ValidatorData, VoteMsg},
            MVBAOutput, Mvba, ValidateCert, ValidateInput, Votable,
        },
        MakesValidationContext, MonadMvba, MvbaContext, TimerEvent,
        messages::{FallbackCommitVote, Justification, MvbaMessage, PrePrepareMsg, PrepareVote},
        test_helpers::{
            DELTA, NUM_PROPOSALS, SLOT, leader_of, nodes, quorum, validator_data, view,
        },
    };

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct TestValue(u64);

    impl ValidateInput for TestValue {
        /// Nothing about a slot or a validator set: the context is the value
        /// type's business, and this value type needs none
        type Context = ();

        fn validate(&self, (): &Self::Context) -> bool {
            true
        }

        fn fbcert_optional(&self) -> bool {
            false
        }
    }

    impl Votable for TestValue {
        type Entries = u64;

        fn entries(&self) -> Self::Entries {
            self.0
        }
    }

    impl MakesValidationContext<TestValue> for MvbaContext {
        fn make_validation_context(&self) {}
    }

    /// The certificate that admits this value type, sharing its empty context
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct TestCert;

    impl ValidateCert for TestCert {
        type Context = ();

        fn validate(&self, (): &Self::Context) -> bool {
            true
        }
    }

    fn instance(
        node: NodeId,
        validator_data: &Arc<ValidatorData>,
    ) -> MonadMvba<TestValue, TestCert> {
        MonadMvba::new(MvbaContext {
            slot: SLOT,
            num_proposals: NUM_PROPOSALS,
            node_id: node,
            key: Arc::new(node.keypair()),
            validator_data: validator_data.clone(),
            delta: DELTA,
        })
    }

    /// The happy path at a value type that is not a metablock: a proposal is
    /// accepted, prepare and commit quorums form over the projection, and the
    /// decision hands back the value itself
    #[test]
    fn a_second_value_type_reaches_a_decision() {
        let validator_data = validator_data();
        let leader = leader_of(view(1));
        let follower = *nodes()
            .iter()
            .find(|node| **node != leader)
            .expect("four validators, one leader");
        let value = TestValue(7);

        let mut instance = instance(follower, &validator_data);
        instance.propose(value.clone(), Some(TestCert));
        drain(&mut instance);

        let proposal = PrePrepareMsg::new_signed(
            SLOT,
            view(1),
            value.clone(),
            Justification::FallbackCert(Some(TestCert)),
            &leader.keypair(),
        );
        instance.handle_message(leader, MvbaMessage::PrePrepare(proposal));
        assert!(
            drain(&mut instance)
                .into_iter()
                .any(|output| matches!(output, MVBAOutput::Broadcast(MvbaMessage::Prepare(_)))),
            "an accepted proposal is voted to prepare"
        );

        for node in quorum() {
            let msg = VoteMsg::new_signed(
                (SLOT, view(1)),
                PrepareVote::<TestValue>(value.entries()),
                &node.keypair(),
            );
            instance.handle_message(node, MvbaMessage::Prepare(msg));
        }
        assert!(
            drain(&mut instance)
                .into_iter()
                .any(|output| matches!(output, MVBAOutput::Broadcast(MvbaMessage::Commit(_)))),
            "a prepare certificate over the projection is voted to commit"
        );

        for node in quorum() {
            let msg = VoteMsg::new_signed(
                (SLOT, view(1)),
                FallbackCommitVote::<TestValue>(value.entries()),
                &node.keypair(),
            );
            instance.handle_message(node, MvbaMessage::Commit(msg));
        }

        assert_eq!(
            instance.decision(),
            Some(&value),
            "the decision is the value itself, not a projection of it"
        );
        assert!(instance.decision_proof().is_some());
    }

    /// `drain`, for this instantiation: the shared helper is typed at
    /// `V = Metablock`
    fn drain(
        instance: &mut MonadMvba<TestValue, TestCert>,
    ) -> Vec<MVBAOutput<MvbaMessage<TestValue, TestCert>, TimerEvent<TestValue>>> {
        std::iter::from_fn(|| instance.poll()).collect()
    }
}
