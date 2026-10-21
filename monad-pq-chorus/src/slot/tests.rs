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

//! Scripted single-instance tests (§14.2): validator 0 of n = 4 with
//! proposers 0 and 1 (K = 2), unless stated otherwise.

use std::sync::Arc;

use monad_mcp_chorus::stub::types::{
    FixedProposerSchedule, ProposerConfig, ProposerSchedule, ProposerSet, RotatingProposerSchedule,
    RoundRobinLeaderSchedule, ScheduleError, SignedProposalHeader,
};

use super::{PqTimer, test_util::*};
use crate::{
    config::ReleaseChunks,
    da::{PqDACommand, ProposalDAEvent},
    finalization::{PqEvent, PqPath},
    message::{CodaMessage, PqMessage},
    slot::evidence::Evidence,
    types::{CodaValue, Entry, NodeId, PathEntry, ProposalMap},
};

const N: u64 = 4;

fn two_proposers() -> Arc<dyn ProposerSchedule + Send + Sync> {
    Arc::new(FixedProposerSchedule::new(vec![
        NodeId::dummy(0),
        NodeId::dummy(1),
    ]))
}

fn harness() -> Harness {
    Harness::new(0, N, two_proposers())
}

fn h(j: u64, byte: u8) -> SignedProposalHeader {
    header(SLOT, j, byte)
}

fn root(j: u64, byte: u8) -> Entry {
    Entry::Root(pid_of(&h(j, byte)))
}

fn timers(outs: &[Out]) -> Vec<PqTimer> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Timer(_, timer) => Some(timer.clone()),
            _ => None,
        })
        .collect()
}

fn entries_of(outs: &[Out]) -> Vec<Entry> {
    let votes = votes(outs);
    assert_eq!(votes.len(), 1, "exactly one vote");
    votes[0]
        .body
        .entries
        .as_ref()
        .into_iter()
        .copied()
        .collect()
}

fn path_entries_of(outs: &[Out]) -> Vec<PathEntry> {
    let votes = path_votes(outs);
    assert_eq!(votes.len(), 1, "exactly one path vote");
    votes[0]
        .body
        .entries
        .as_ref()
        .into_iter()
        .copied()
        .collect()
}

// validators 1..=count vote `entries`
fn votes_from(harness: &mut Harness, voters: &[u64], entries: &[Entry]) -> Vec<Out> {
    voters
        .iter()
        .flat_map(|voter| harness.message(*voter, signed_vote(*voter, entries)))
        .collect()
}

fn path_votes_from(harness: &mut Harness, voters: &[u64], entries: &[PathEntry]) -> Vec<Out> {
    voters
        .iter()
        .flat_map(|voter| harness.message(*voter, signed_path_vote(*voter, entries)))
        .collect()
}

fn coda_vote1(outs: &[Out]) -> Option<ProposalMap<CodaValue<Entry>>> {
    outs.iter().find_map(|out| match out {
        Out::Broadcast(PqMessage::Coda(CodaMessage::Vote1 { view: 1, x })) => Some(x.clone()),
        _ => None,
    })
}

#[test]
fn early_vote_once_every_chunk_arrived() {
    let mut config = config();
    config.early_vote = true;
    let mut harness = Harness::with_config(0, N, two_proposers(), config);
    let outs = harness.chunk(0, &h(0, 1));
    assert!(votes(&outs).is_empty(), "one proposer still missing");

    let outs = harness.chunk(1, &h(1, 2));
    assert_eq!(entries_of(&outs), vec![root(0, 1), root(1, 2)]);
    assert!(da_commands(&outs).contains(&&PqDACommand::ReleaseChunks));

    // the deadline does not vote again
    let outs = harness.deadline();
    assert!(votes(&outs).is_empty());
}

#[test]
fn deadline_vote_carries_negatives_and_arms_timers() {
    let mut harness = harness();
    harness.chunk(1, &h(1, 2));
    let outs = harness.deadline();
    assert_eq!(entries_of(&outs), vec![Entry::Bot, root(1, 2)]);
    let mut armed = timers(&outs);
    armed.sort_by_key(|timer| format!("{timer:?}"));
    assert_eq!(
        armed,
        vec![
            PqTimer::Accountability(0),
            PqTimer::Accountability(1),
            PqTimer::Adoption,
            PqTimer::DecodeGrace,
            PqTimer::PathVoteGate,
            PqTimer::Retransmit(0),
        ]
    );
    // a chunk after the deadline changes nothing (ln:qv:chunk)
    let outs = harness.chunk(0, &h(0, 1));
    assert!(votes(&outs).is_empty());
}

#[test]
fn early_vote_disabled_waits_for_deadline() {
    let mut config = config();
    config.early_vote = false;
    let mut harness = Harness::with_config(0, N, two_proposers(), config);
    harness.chunk(0, &h(0, 1));
    assert!(votes(&harness.chunk(1, &h(1, 2))).is_empty());
    assert_eq!(
        entries_of(&harness.deadline()),
        vec![root(0, 1), root(1, 2)]
    );
}

#[test]
fn release_on_receipt_releases_at_the_first_event() {
    let mut config = config();
    config.release_chunks = ReleaseChunks::Receipt;
    let mut harness = Harness::with_config(0, N, two_proposers(), config);
    let outs = harness.da(0, ProposalDAEvent::HeaderSeen(h(0, 1)));
    assert_eq!(da_commands(&outs), vec![&PqDACommand::ReleaseChunks]);
    let outs = harness.deadline();
    assert!(!da_commands(&outs).contains(&&PqDACommand::ReleaseChunks));
}

#[test]
fn all_fast_path_vote_before_the_gate_then_speculation() {
    let mut harness = harness();
    harness.chunk(0, &h(0, 1));
    let entries = [root(0, 1), Entry::Bot];
    let outs = harness.deadline();
    assert_eq!(entries_of(&outs), entries.to_vec());
    // with our own vote, two more make 2f+1 matching entries everywhere
    let outs = votes_from(&mut harness, &[1, 2], &entries);
    assert_eq!(
        path_entries_of(&outs),
        vec![PathEntry::Fast(root(0, 1)), PathEntry::Fast(Entry::Bot)]
    );
    assert!(events(&outs).is_empty(), "speculation waits for the check");

    let outs = harness.da(0, ProposalDAEvent::Decoded(pid_of(&h(0, 1))));
    assert_eq!(
        events(&outs),
        vec![&PqEvent::Speculative {
            metablock: metablock(&entries)
        }]
    );
}

#[test]
fn fallback_entry_after_decoding_a_weak_root() {
    let mut harness = harness();
    let r = root(0, 1);
    harness.deadline();
    votes_from(&mut harness, &[1, 2], &[r, Entry::Bot]);
    // our own all-negative vote: r has f+1 = 2, ⊥ has 1 at j = 0
    let outs = harness.timer(PqTimer::PathVoteGate);
    assert!(
        path_votes(&outs).is_empty(),
        "waits for the root to resolve"
    );
    let outs = harness.da(0, ProposalDAEvent::Decoded(pid_of(&h(0, 1))));
    assert_eq!(
        path_entries_of(&outs),
        vec![PathEntry::Fallback(r), PathEntry::Fast(Entry::Bot)]
    );
}

#[test]
fn decode_grace_expiry_votes_fallback_bot() {
    let mut harness = harness();
    harness.deadline();
    votes_from(&mut harness, &[1, 2], &[root(0, 1), Entry::Bot]);
    assert!(path_votes(&harness.timer(PqTimer::PathVoteGate)).is_empty());
    let outs = harness.timer(PqTimer::DecodeGrace);
    assert_eq!(
        path_entries_of(&outs),
        vec![PathEntry::Fallback(Entry::Bot), PathEntry::Fast(Entry::Bot)]
    );
}

#[test]
fn invalid_encoding_yields_fallback_bot() {
    let mut harness = harness();
    harness.deadline();
    votes_from(&mut harness, &[1, 2], &[root(0, 1), Entry::Bot]);
    harness.timer(PqTimer::PathVoteGate);
    let outs = harness.da(0, ProposalDAEvent::DecodingFailed(pid_of(&h(0, 1))));
    assert_eq!(path_entries_of(&outs)[0], PathEntry::Fallback(Entry::Bot));
}

#[test]
fn weak_voted_root_is_pulled_with_its_header() {
    let mut harness = harness();
    let r = root(0, 1);
    let pid = pid_of(&h(0, 1));
    let outs = votes_from(&mut harness, &[1], &[r, Entry::Bot]);
    assert!(da_commands(&outs).is_empty(), "one voter is below f+1");

    let outs = votes_from(&mut harness, &[2], &[r, Entry::Bot]);
    let voters = vec![NodeId::dummy(1), NodeId::dummy(2)];
    let commands = da_commands(&outs);
    assert!(commands.contains(&&PqDACommand::PullRoot {
        j: 0,
        pid,
        voters: voters.clone()
    }));
    assert!(commands.contains(&&PqDACommand::PullHeader {
        j: 0,
        missing: vec![(pid, voters.iter().copied().collect())]
    }));

    // a further voter grows the pull; the header pull is retried per tick
    let outs = votes_from(&mut harness, &[3], &[r, Entry::Bot]);
    assert!(matches!(
        da_commands(&outs)[..],
        [PqDACommand::PullRoot { voters, .. }] if voters.len() == 3
    ));
    // 2f+1 matching entries everywhere: the path vote goes out at once
    assert_eq!(path_votes(&outs).len(), 1);
    let generation = timers(&outs)
        .into_iter()
        .find_map(|timer| match timer {
            PqTimer::Retransmit(generation) => Some(generation),
            _ => None,
        })
        .expect("first broadcast arms retransmission");
    let outs = harness.timer(PqTimer::Retransmit(generation));
    assert!(
        da_commands(&outs)
            .iter()
            .any(|command| matches!(command, PqDACommand::PullHeader { j: 0, .. }))
    );
    assert_eq!(path_votes(&outs).len(), 1, "own path vote retransmitted");

    // once seen, the header is no longer pulled
    harness.da(0, ProposalDAEvent::HeaderSeen(h(0, 1)));
    let outs = harness.timer(PqTimer::Retransmit(generation + 1));
    assert!(
        !da_commands(&outs)
            .iter()
            .any(|command| matches!(command, PqDACommand::PullHeader { .. }))
    );
}

fn retransmit_generation(outs: &[Out]) -> u32 {
    timers(outs)
        .into_iter()
        .find_map(|timer| match timer {
            PqTimer::Retransmit(generation) => Some(generation),
            _ => None,
        })
        .expect("first broadcast arms retransmission")
}

#[test]
fn a_dropped_root_pull_is_repeated_until_resolved() {
    let mut harness = harness();
    let pid = pid_of(&h(0, 1));
    let generation = retransmit_generation(&harness.deadline());
    let outs = votes_from(&mut harness, &[1, 2], &[root(0, 1), Entry::Bot]);
    let pull = PqDACommand::PullRoot {
        j: 0,
        pid,
        voters: vec![NodeId::dummy(1), NodeId::dummy(2)],
    };
    assert!(da_commands(&outs).contains(&&pull));

    // the first pull is lost: each retransmission tick repeats it
    for tick in 0..2 {
        let outs = harness.timer(PqTimer::Retransmit(generation + tick));
        assert!(da_commands(&outs).contains(&&pull));
    }

    harness.da(0, ProposalDAEvent::Decoded(pid));
    let outs = harness.timer(PqTimer::Retransmit(generation + 2));
    assert!(
        !da_commands(&outs)
            .iter()
            .any(|command| matches!(command, PqDACommand::PullRoot { .. }))
    );
}

#[test]
fn fast_entries_wait_for_corroboration_before_adoption() {
    let mut harness = harness();
    let r = root(0, 1);
    harness.deadline();
    harness.timer(PqTimer::PathVoteGate);
    harness.timer(PqTimer::Adoption);
    // path votes claim a fast root that no vote corroborates yet
    let fast = [PathEntry::Fast(r), PathEntry::Fallback(Entry::Bot)];
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &fast);
    assert!(coda_vote1(&outs).is_none());

    // f+1 positive votes, but the root is unresolved
    let outs = votes_from(&mut harness, &[1, 2], &[r, Entry::Bot]);
    assert!(coda_vote1(&outs).is_none());

    let outs = harness.da(0, ProposalDAEvent::Decoded(pid_of(&h(0, 1))));
    let adopted = coda_vote1(&outs).expect("adopted and entered coda");
    assert_eq!(adopted[0], CodaValue::Value(r));
    assert_eq!(adopted[1], CodaValue::Value(Entry::Bot));
}

#[test]
fn adoption_waits_for_the_two_delta_gate() {
    let mut harness = harness();
    harness.deadline();
    let fallback = [PathEntry::Fallback(Entry::Bot); 2];
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &fallback);
    assert!(coda_vote1(&outs).is_none());
    let outs = harness.timer(PqTimer::Adoption);
    assert_eq!(
        coda_vote1(&outs),
        Some(ProposalMap::new(2, |_| CodaValue::Value(Entry::Bot)))
    );
}

#[test]
fn adoption_reads_fallback_roots_with_weak_support() {
    let mut harness = harness();
    let r = root(1, 2);
    harness.deadline();
    harness.timer(PqTimer::Adoption);
    path_votes_from(
        &mut harness,
        &[1, 2],
        &[PathEntry::Fallback(Entry::Bot), PathEntry::Fallback(r)],
    );
    let outs = path_votes_from(
        &mut harness,
        &[3],
        &[
            PathEntry::Fallback(Entry::Bot),
            PathEntry::Fallback(Entry::Bot),
        ],
    );
    let adopted = coda_vote1(&outs).expect("clean sets everywhere");
    assert_eq!(adopted[1], CodaValue::Value(r));
}

#[test]
fn coda_decision_finalizes_on_the_coda_path() {
    let mut harness = harness();
    harness.deadline();
    harness.timer(PqTimer::Adoption);
    let fallback = [PathEntry::Fallback(Entry::Bot); 2];
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &fallback);
    let x = coda_vote1(&outs).expect("entered coda");
    for sender in [1, 2] {
        harness.message(
            sender,
            PqMessage::Coda(CodaMessage::Vote1 {
                view: 1,
                x: x.clone(),
            }),
        );
    }
    let mut outs = Vec::new();
    for sender in [1, 2] {
        let vote2 = CodaMessage::Vote2 {
            view: 1,
            lock: Some(x.clone()),
            guard: x.clone(),
        };
        outs.extend(harness.message(sender, PqMessage::Coda(vote2)));
    }
    assert_eq!(
        finals(&outs),
        vec![&PqEvent::Final {
            metablock: metablock(&[Entry::Bot, Entry::Bot]),
            path: PqPath::Coda,
            at_view: Some(1),
            path_votes: vec![],
        }]
    );
    assert!(halts(&outs).is_empty(), "finalizing is not halting");
}

#[test]
fn coda_decision_reports_the_deciding_view_after_moving_on() {
    let mut harness = harness();
    harness.deadline();
    harness.timer(PqTimer::Adoption);
    let fallback = [PathEntry::Fallback(Entry::Bot); 2];
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &fallback);
    let x = coda_vote1(&outs).expect("entered coda");
    // a strong echo certificate for view 1 moves us to view 2
    let mut outs = Vec::new();
    for sender in [1, 2, 3] {
        for j in 0..2 {
            let echo = CodaMessage::Echo {
                view: 1,
                j,
                v: x[j as usize],
            };
            outs.extend(harness.message(sender, PqMessage::Coda(echo)));
        }
    }
    assert!(timers(&outs).contains(&PqTimer::Coda(2)), "entered view 2");
    assert!(finals(&outs).is_empty());

    // late view-1 Vote2s decide, for view 1
    let mut outs = Vec::new();
    for sender in [1, 2, 3] {
        let vote2 = CodaMessage::Vote2 {
            view: 1,
            lock: Some(x.clone()),
            guard: x.clone(),
        };
        outs.extend(harness.message(sender, PqMessage::Coda(vote2)));
    }
    assert!(matches!(
        finals(&outs)[..],
        [PqEvent::Final {
            path: PqPath::Coda,
            at_view: Some(1),
            ..
        }]
    ));
}

#[test]
fn fast_finalize_excludes_an_invalid_encoding() {
    let mut harness = harness();
    let r = root(0, 1);
    let fast = [PathEntry::Fast(r), PathEntry::Fast(Entry::Bot)];
    votes_from(&mut harness, &[1, 2], &[r, Entry::Bot]);
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &fast);
    assert!(finals(&outs).is_empty(), "the root's check is unresolved");

    let outs = harness.da(0, ProposalDAEvent::DecodingFailed(pid_of(&h(0, 1))));
    let finals = finals(&outs);
    let [
        PqEvent::Final {
            metablock: x,
            path,
            path_votes,
            ..
        },
    ] = finals[..]
    else {
        panic!("one final event: {outs:?}");
    };
    assert_eq!(x, &metablock(&[Entry::Bot, Entry::Bot]));
    assert_eq!(*path, PqPath::Fast);
    assert_eq!(path_votes.len(), 3);
    // pins precede the event; the Finalized broadcast is alive
    let pins: Vec<_> = da_commands(&outs)
        .into_iter()
        .filter(|command| matches!(command, PqDACommand::PinFinal { .. }))
        .collect();
    assert_eq!(
        pins,
        vec![
            &PqDACommand::PinFinal { j: 0, pid: None },
            &PqDACommand::PinFinal { j: 1, pid: None }
        ]
    );
    assert!(outs.contains(&Out::Broadcast(PqMessage::Finalized(x.clone()))));
}

#[test]
fn finalize_pins_roots_and_decoded_holders() {
    let mut harness = harness();
    let r = root(0, 1);
    let pid = pid_of(&h(0, 1));
    harness.da(0, ProposalDAEvent::Decoded(pid));
    votes_from(&mut harness, &[1, 2], &[r, Entry::Bot]);
    path_votes_from(
        &mut harness,
        &[1],
        &[PathEntry::Fallback(r), PathEntry::Fast(Entry::Bot)],
    );
    let outs = path_votes_from(
        &mut harness,
        &[2, 3],
        &[PathEntry::Fast(r), PathEntry::Fast(Entry::Bot)],
    );
    assert!(finals(&outs).is_empty(), "only two fast entries for r");
    let outs = path_votes_from(
        &mut harness,
        &[0],
        &[PathEntry::Fast(r), PathEntry::Fast(Entry::Bot)],
    );
    let commands = da_commands(&outs);
    assert!(commands.contains(&&PqDACommand::PinFinal {
        j: 0,
        pid: Some(pid)
    }));
    assert!(commands.contains(&&PqDACommand::AddDecodedHolders {
        j: 0,
        pid,
        node_ids: vec![NodeId::dummy(1)]
    }));
}

#[test]
fn gadget_finalizes_at_weak_and_halts_at_strong() {
    let mut harness = harness();
    let x = metablock(&[root(0, 1), Entry::Bot]);
    let outs = harness.message(1, PqMessage::Finalized(x.clone()));
    assert!(finals(&outs).is_empty());

    // f+1 = 2: finalize through the gadget and echo
    let outs = harness.message(2, PqMessage::Finalized(x.clone()));
    assert!(matches!(
        finals(&outs)[..],
        [PqEvent::Final { path: PqPath::Gadget, metablock, .. }] if *metablock == x
    ));
    assert!(outs.contains(&Out::Broadcast(PqMessage::Finalized(x.clone()))));
    // our own echo makes 2f+1 = 3: halt
    let halts = halts(&outs);
    assert_eq!(halts.len(), 1);
    assert_eq!(halts[0].metablock, x);
    assert_eq!(halts[0].path, PqPath::Gadget);
}

#[test]
fn finalize_once_and_only_at_halt() {
    let mut harness = harness();
    let fast = [PathEntry::Fast(Entry::Bot); 2];
    votes_from(&mut harness, &[1, 2], &[Entry::Bot, Entry::Bot]);
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &fast);
    assert_eq!(finals(&outs).len(), 1);
    assert!(
        halts(&outs).is_empty(),
        "fast finalize does not close the slot"
    );

    let x = metablock(&[Entry::Bot, Entry::Bot]);
    let mut outs = Vec::new();
    for sender in [1, 2, 3] {
        outs.extend(harness.message(sender, PqMessage::Finalized(x.clone())));
    }
    assert!(finals(&outs).is_empty());
    let halts = halts(&outs);
    assert_eq!(halts.len(), 1);
    assert_eq!(halts[0].path, PqPath::Fast);
}

#[test]
fn inputs_after_halt_are_ignored() {
    let mut harness = harness();
    let x = metablock(&[Entry::Bot, Entry::Bot]);
    for sender in [1, 2] {
        harness.message(sender, PqMessage::Finalized(x.clone()));
    }
    assert!(harness.slot.halted);

    assert!(harness.deadline().is_empty());
    assert!(harness.timer(PqTimer::PathVoteGate).is_empty());
    assert!(harness.timer(PqTimer::Retransmit(0)).is_empty());
    assert!(harness.chunk(0, &h(0, 1)).is_empty());
    assert!(
        harness
            .message(3, signed_vote(3, &[Entry::Bot, Entry::Bot]))
            .is_empty()
    );
    assert!(harness.message(3, PqMessage::Finalized(x)).is_empty());
}

#[test]
fn equivocating_headers_are_evidence() {
    let mut harness = harness();
    harness.da(0, ProposalDAEvent::HeaderSeen(h(0, 1)));
    let outs = harness.da(0, ProposalDAEvent::HeaderSeen(h(0, 2)));
    assert!(matches!(
        events(&outs)[..],
        [PqEvent::Evidence(Evidence::EquivocatingHeaders {
            j: 0,
            ..
        })]
    ));
    // filed once
    assert!(events(&harness.da(0, ProposalDAEvent::HeaderSeen(h(0, 3)))).is_empty());
}

#[test]
fn conflicting_votes_and_path_votes_are_evidence() {
    let mut harness = harness();
    votes_from(&mut harness, &[2], &[Entry::Bot, Entry::Bot]);
    // a retransmission of the same vote is not a conflict
    assert!(events(&votes_from(&mut harness, &[2], &[Entry::Bot, Entry::Bot])).is_empty());
    let outs = votes_from(&mut harness, &[2], &[root(0, 1), Entry::Bot]);
    assert!(matches!(
        events(&outs)[..],
        [PqEvent::Evidence(Evidence::ConflictingVotes { signer, .. })] if *signer == NodeId::dummy(2)
    ));

    path_votes_from(&mut harness, &[3], &[PathEntry::Fast(Entry::Bot); 2]);
    let outs = path_votes_from(&mut harness, &[3], &[PathEntry::Fallback(Entry::Bot); 2]);
    assert!(matches!(
        events(&outs)[..],
        [PqEvent::Evidence(Evidence::ConflictingPathVotes { .. })]
    ));
}

#[test]
fn forged_votes_are_ignored() {
    let mut harness = harness();
    // validator 1's vote claimed by validator 2
    let outs = harness.message(2, signed_vote(1, &[Entry::Bot, Entry::Bot]));
    assert!(outs.is_empty());
    assert!(harness.slot.votes.is_empty());
    // a vote of the wrong size, and one from a non-validator
    assert!(harness.message(1, signed_vote(1, &[Entry::Bot])).is_empty());
    assert!(
        harness
            .message(9, signed_vote(9, &[Entry::Bot, Entry::Bot]))
            .is_empty()
    );
    assert!(harness.slot.votes.is_empty());
}

#[test]
fn duplicate_timers_are_idempotent() {
    let mut harness = harness();
    let first = harness.deadline();
    assert!(harness.deadline().is_empty(), "second deadline is a no-op");
    votes_from(&mut harness, &[1, 2], &[Entry::Bot, Entry::Bot]);

    for _ in 0..3 {
        harness.timer(PqTimer::PathVoteGate);
        harness.timer(PqTimer::Adoption);
        harness.timer(PqTimer::DecodeGrace);
    }
    assert_eq!(harness.slot.votes.len(), 3);

    // a duplicated retransmission firing does not fork the chain
    let generation = timers(&first)
        .into_iter()
        .find_map(|timer| match timer {
            PqTimer::Retransmit(generation) => Some(generation),
            _ => None,
        })
        .unwrap();
    let rearmed = timers(&harness.timer(PqTimer::Retransmit(generation)));
    assert_eq!(rearmed, vec![PqTimer::Retransmit(generation + 1)]);
    assert!(harness.timer(PqTimer::Retransmit(generation)).is_empty());

    // accountability fires at most once per round
    harness.timer(PqTimer::Accountability(1));
    assert!(events(&harness.timer(PqTimer::Accountability(1))).is_empty());
}

#[test]
fn accountability_evidence_for_an_unfinalized_slot() {
    let mut harness = harness();
    harness.da(0, ProposalDAEvent::HeaderSeen(h(0, 1)));
    harness.deadline();
    votes_from(&mut harness, &[1], &[Entry::Bot, Entry::Bot]);
    // f = 1: ⌊f/2⌋ + 1 = 1 negative vote next to a header suspects proposer 0
    let outs = harness.timer(PqTimer::Accountability(0));
    let suspects: Vec<_> = events(&outs)
        .into_iter()
        .filter_map(|event| match event {
            PqEvent::Evidence(evidence @ Evidence::PartialDissemination { j: 0, .. }) => {
                Some(evidence.culprit())
            }
            _ => None,
        })
        .collect();
    assert_eq!(suspects, vec![Some(NodeId::dummy(0))]);

    let fallback = [
        PathEntry::Fallback(Entry::Bot),
        PathEntry::Fallback(Entry::Bot),
    ];
    path_votes_from(&mut harness, &[1, 2, 3], &fallback);
    let outs = harness.timer(PqTimer::Accountability(1));
    let kinds: Vec<&str> = events(&outs)
        .into_iter()
        .filter_map(|event| match event {
            PqEvent::Evidence(evidence) => Some(evidence.kind()),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, vec!["fallback-quorum", "fallback-quorum", "votes"]);
}

#[test]
fn no_accountability_once_finalized() {
    let mut harness = harness();
    harness.deadline();
    votes_from(&mut harness, &[1, 2], &[Entry::Bot, Entry::Bot]);
    let outs = path_votes_from(&mut harness, &[1, 2, 3], &[PathEntry::Fast(Entry::Bot); 2]);
    assert_eq!(finals(&outs).len(), 1);
    assert!(events(&harness.timer(PqTimer::Accountability(0))).is_empty());
    assert!(events(&harness.timer(PqTimer::Accountability(1))).is_empty());
}

// a rotating schedule: K = 5 proposal indices over `n` validators
fn rotating(n: u64) -> Arc<RotatingProposerSchedule<RoundRobinLeaderSchedule>> {
    let config = ProposerConfig {
        concurrent_proposers: 5,
        observation_cutoff: 5,
        rotation_slack: 5,
        slots_per_epoch: 100,
    };
    let algorithm = RoundRobinLeaderSchedule::new(&config);
    Arc::new(RotatingProposerSchedule::new(config, algorithm, validator_data(n)).unwrap())
}

fn occupied(schedule: &dyn ProposerSchedule) -> Vec<(usize, NodeId)> {
    schedule
        .proposers_at(SLOT)
        .unwrap()
        .iter()
        .filter_map(|(j, proposer)| Some((j, proposer?)))
        .collect()
}

#[test]
fn single_node_with_vacant_lanes_finalizes_from_its_own_chunk() {
    let schedule = rotating(1);
    let lanes = occupied(schedule.as_ref());
    assert_eq!(
        lanes,
        vec![(lanes[0].0, NodeId::dummy(0))],
        "one occupied lane"
    );
    let j = lanes[0].0;
    let mut config = config();
    config.early_vote = true;
    let mut harness = Harness::with_config(0, 1, schedule, config);

    let header = header(SLOT, 0, 1);
    let outs = harness.chunk(j, &header);
    let mut expected = vec![Entry::Bot; 5];
    expected[j] = Entry::Root(pid_of(&header));
    assert_eq!(
        entries_of(&outs),
        expected,
        "early vote on the occupied lane"
    );
    assert_eq!(path_votes(&outs).len(), 1, "own vote is a quorum");
    assert!(
        !da_commands(&outs)
            .iter()
            .any(|command| matches!(command, PqDACommand::PullRoot { .. })),
        "own chunk needs no pull"
    );

    let outs = harness.da(j, ProposalDAEvent::Decoded(pid_of(&header)));
    let metablock = ProposalMap::new(5, |i| expected[i]);
    let events = events(&outs);
    assert_eq!(
        events[0],
        &PqEvent::Speculative {
            metablock: metablock.clone()
        }
    );
    assert!(matches!(
        events[1],
        PqEvent::Final { metablock: x, path: PqPath::Fast, path_votes, .. }
            if *x == metablock && path_votes.len() == 1
    ));
    let halts = halts(&outs);
    assert_eq!(halts.len(), 1, "own Finalized is a strong quorum");
    assert_eq!(halts[0].metablock, metablock);
}

#[test]
fn roots_at_vacant_indices_are_rejected() {
    let schedule = rotating(4);
    let vacant = (0..5)
        .find(|j| occupied(schedule.as_ref()).iter().all(|(o, _)| o != j))
        .expect("K = 5 over 4 validators leaves a vacancy");
    let mut harness = Harness::new(0, 4, schedule);

    let mut entries = vec![Entry::Bot; 5];
    entries[vacant] = root(1, 1);
    assert!(harness.message(1, signed_vote(1, &entries)).is_empty());
    assert!(harness.slot.votes.is_empty());

    let mut path = vec![PathEntry::Fast(Entry::Bot); 5];
    path[vacant] = PathEntry::Fallback(root(1, 1));
    harness.message(1, signed_path_vote(1, &path));
    assert!(harness.slot.path_votes.is_empty());

    // negative entries there are required and accepted
    harness.message(1, signed_vote(1, &[Entry::Bot; 5]));
    assert_eq!(harness.slot.votes.len(), 1);
    // and DA events for the vacant index are ignored
    assert!(harness.chunk(vacant, &header(SLOT, 1, 1)).is_empty());
}

#[derive(Debug)]
struct Unavailable;

impl ProposerSchedule for Unavailable {
    fn num_indices(&self) -> usize {
        3
    }

    fn proposers_at(&self, _slot: crate::types::Slot) -> Result<ProposerSet, ScheduleError> {
        Err(ScheduleError::BeforeAnchor {
            queried: 0,
            anchor: 1,
        })
    }
}

#[test]
fn unknown_schedule_leaves_the_slot_inert() {
    let mut harness = Harness::new(0, N, Arc::new(Unavailable));
    assert!(harness.deadline().is_empty());
    assert!(harness.chunk(0, &h(0, 1)).is_empty());
    assert!(
        harness
            .message(1, signed_vote(1, &[Entry::Bot; 3]))
            .is_empty()
    );
    assert!(harness.timer(PqTimer::Adoption).is_empty());
}
