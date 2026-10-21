# Quantum Cadence client: design

Status: design, pre-implementation (2026-10-21).
Spec: `~/tmp/mcp-paper/pq-cadence/quantum-cadence.tex` (v0.9). Alg. 1 = Quantum Chorus (`algorithms/quantum-chorus.tex`), Alg. 2 = Coda (`algorithms/coda.tex`), count rule = Claim `clm:coda:count_rule` (`appendix/b-coda-proofs.tex:596`).
Constraint: no `monad-mcp-*` crate, script or config is modified. Everything new lives in `monad-pq-*`.

Citations are `path:line` relative to the repo root unless marked `paper:`.

---

## 1. Summary of verdicts

| Q | Verdict |
|---|---|
| Q1 runtime | `QuantumChorus` implements `monad_mcp_chorus::stub::SlotConsensus` and runs in the **unchanged** `CadenceRuntime<QuantumChorus, MonadConductor<NopAcs<SlotDeadline>>>`. The slot emits `SlotOutput::Finalize` **at halt** (paper §6: completion at halting). The ledger-visible finalization is delivered earlier, at finalize time, through `SlotOutput::CommitOptimistic(PqEvent::Final{..})`, which the runtime forwards to `FinalizationObserver::handle_optimistic_commit`. No runtime duplication. |
| Q2 DA | **Duplicate** `monad-mcp-da` as `monad-pq-da` now. The S11, Merkle and Reed-Solomon logic, the wire format and `ChorusDAEvent` stay identical. Commands become `PqDACommand`, a superset of `ChorusDACommand` with `PullRoot`. Upstream DA never pulls a root that is not the proposer's first-seen pid (`proposer_rc/mod.rs:177-187`, `254-269`), forwards owned chunks only once, and treats `PullHeader` as one-shot. So a fast or fallback entry for a rival root may never resolve, and Coda input stalls (section 4.3, B1). The paper's "chunks attached to f+1 positive votes" becomes "f+1 positive votes, and DA reports `Decoded(pid)` / `DecodingFailed(pid)`", backed by `PullRoot` to those voters. A bounded `decode_grace` (≤ Δ/2) covers the path vote. PQ header signatures are a later env change in the same crate. |
| Q3 RPC | Reuse `monad-mcp-rpc` **unchanged**. The pq `node.toml` is a strict superset of `monad_mcp_node::config::NodeConfig` (new keys only in a new `[quantum]` table). The pq node builds its proposer schedule with the same `NodeConfig::epoch_handle()` and speaks the same UDP framing (`monad_mcp_node::network`). |
| Q4 node | Depend on `monad-mcp-node` as a library for `network::*`, `component::{Component, Dispatch, Link, Proposing*, Repeater*, Mempool*}`, `config::NodeConfig` (whole struct, `epoch_handle()`, `peers()`, `repeater()`), `EpochHandle`, `ledger::LedgerSink` and `init_logging`. Duplicate `component/cadence.rs`, `component/da.rs` (the DA type differs), `component::{spawn, run, sleep_until_due}` together with `clock.rs`, `runtime.rs`, `node.rs`, `async_node.rs`, `finalization.rs`, `ledger::{new_block, log_finalized, Recorder}`, `main.rs`, the swarm example and the test harnesses. `spawn` is pub but takes the private `Clock` (`component/mod.rs:68,79,108`). The `ledger` functions are pub but Chorus-typed. |
| Q5 explorer | Reuse `monad-mcp-explorer` **unchanged**. The pq node writes the same ledger with `monad_mcp_chorus::ledger::LedgerWriter`, maps paths onto `FinalizationPath::{Fast, Fallback}` and puts an opaque proof in `proof.rlp`. |

---

## 2. Crate layout

```
monad-pq-chorus/          lib: Quantum Chorus slot consensus + Coda + PQ signature seam
  Cargo.toml              deps: monad-mcp-chorus (features=["enable_stub"]), alloy-rlp (derive),
                          bytes, derive_more, thiserror, tracing, tiny-keccak
                          dev: monad-mcp-chorus-sim, monad-sim, monad-sim-swarm, proptest, rand
  src/lib.rs
  src/sig.rs              PqSignature, PqSigner, PqVerifier, StubPq, signing domains, Signed<B>
  src/quorum.rs           Quorum { n, f, weak, strong } from ValidatorData (equal stake enforced)
  src/types.rs            Entry, Metablock, PathEntry, CodaValue, CodaMetablock
  src/message.rs          PqMessage + CodaMessage, hand-written RLP [tag, ..]
  src/coda/mod.rs         Coda<V>: the protocol state machine (Alg. 2), sans-IO
  src/coda/view.rs        OwnView / ReceivedView, per-view state
  src/coda/count.rs       count-rule evaluation (Claim clm:coda:count_rule)
  src/coda/sim.rs         #[cfg(test)] deterministic Coda-only swarm
  src/da.rs               PqDACommand (consumed by monad-pq-da; the Chorus layering is mirrored here)
  src/slot/mod.rs         QuantumChorus: impl SlotConsensus (Alg. 1)
  src/slot/availability.rs  copy of monad-mcp-chorus slot/availability.rs (pub(crate) there)
  src/slot/votes.rs       VoteTally: per-proposer positive/negative sets
  src/slot/second_round.rs  accepted entries, clean sets, adopt()
  src/slot/gadget.rs      Finalized tally, halting gadget
  src/slot/retransmit.rs  alive-message set + Retransmit timer
  src/slot/evidence.rs    Evidence + detectors
  src/finalization.rs     PqEvent, PqHalt, PqPath, PqProof
  src/config.rs           QuantumConfig (per-slot), QuantumContext (shared)
  tests/                  swarm tests (section 14.3)

monad-pq-da/              lib: duplicate of monad-mcp-da (all modules), changes listed in section 4.5
  Cargo.toml              deps: as monad-mcp-da, plus monad-pq-chorus (for PqDACommand)

monad-pq-node/            lib + bin `monad-pq-node`
  Cargo.toml              deps: monad-pq-chorus, monad-pq-da, monad-mcp-chorus (enable_stub),
                          monad-mcp-node (path), tokio, toml, serde, tracing, bytes, alloy-rlp
                          dev: monad-sim, monad-sim-swarm, tempfile
  src/lib.rs              run_node(PqNodeConfig)
  src/main.rs             monad-pq-node <node.toml>
  src/config.rs           PqNodeConfig { base: NodeConfig, quantum: QuantumSection }, two-pass parse
  src/component/cadence.rs  PqCadence (dup of monad-mcp-node component/cadence.rs, S = QuantumChorus)
  src/component/da.rs     PqDA = monad_pq_da::DARuntime<NodeProposerSchedule> + DAInput (dup of component/da.rs)
  src/component/spawn.rs  spawn/run/sleep_until_due (dup of component/mod.rs:68-120; needs the local Clock)
  src/runtime.rs          PqEffect, PqRuntime trait, PqNodeRuntime (dup of runtime.rs)
  src/node.rs             PqNode (sync stepper, dup of node.rs)
  src/async_node.rs       dup of async_node.rs
  src/clock.rs            dup of clock.rs (private upstream)
  src/finalization.rs     PqFinalizationCollector, PqFinalizedSlot (dup of finalization.rs)
  src/ledger.rs           pq_new_block, log_finalized, PqRecorder (reuses LedgerSink)
  src/evidence.rs         EvidenceSink: <ledger.dir>/evidence/*.rlp
  src/wire.rs             frame-size check + fragmentation hook (section 6.4)
  examples/swarm.rs       in-process N-node swarm
  examples/latency.rs     e2e latency from the ledger (section 14.6)
  swarm.sh                local multi-process swarm (copy of monad-mcp-node/swarm.sh, renamed)
  e2e/run.sh              copy of monad-mcp-explorer/e2e/run.sh with pq node binary
  tests/in_process.rs, tests/scenarios/{harness,loss,mute,partition,tamper,equivocation}.rs
  deploy/                 section 13
```

Reused unchanged, as Cargo deps or binaries: `monad-mcp-chorus` (runtime, slot manager, timers, conductor, `NopAcs`, proposer schedule, planner, ledger, stub env), `monad-mcp-node` (as a library), the `monad-mcp-rpc` binary + `mcp-tx`, the `monad-mcp-explorer` binary, `monad-mcp-chorus-sim` (dev-dep) and `monad-sim(-swarm)`.

Root `Cargo.toml` needs no edit. `members = ["monad-*"]` picks up the new dirs. `monad-mcp-chorus` and `monad-mcp-da` already have workspace entries (`Cargo.toml:40-41`). `monad-mcp-node` is taken by `path`, as `monad-mcp-rpc/Cargo.toml:30` does. `Cargo.lock` changes.

There is no `monad-pq-rpc`. `monad-pq-da` is required in v1 (section 4.5).

---

## 3. Q1: running inside the unchanged runtime

### 3.1 Evidence

- The trait is fully generic. `SlotConsensus` (`monad-mcp-chorus/src/slot/mod.rs:25-56`) puts only `Clone`/`Hash` bounds on its associated types. An external impl already exists in spirit: `DummySlotConsensus` (`slot/dummy.rs:64-73`) and the sim crate instantiate the runtime from outside the crate.
- **Slot closing.** On `SlotOutput::Finalize` the runtime does four things in order (`runtime.rs:194-203`): `observer.handle_finalization`, then DA `Completed`, then `conductor.handle_slot_finalization`, then `slot_manager.close`. A closed slot receives no further inputs (`slot_manager.rs:68-74`). It is still polled until the cap passes it (`slot_manager.rs:76-84`, `100-106`).
- **`Fault` does not tell the conductor** (`runtime.rs:204-210`), so it would stall the cap. Never use it.
- **`CommitOptimistic(data)`** is forwarded to `observer.handle_optimistic_commit(now, slot, &data)` and to nothing else (`runtime.rs:189-191`). It does not change slot or conductor state.
- **Messages for slots without an instance are dropped**: not yet open, closed, or below the cap (`runtime.rs:140-147`, `slot_manager.rs:68-74`). DA events are dropped the same way (`runtime.rs:283-293`). There is no buffer; `slot_manager.rs:45` has only a commented-out `PendingSlotBuffers`.
- **The conductor advances only on `Finalize`.** `CompletionTracker` advances the cap through the contiguous completed prefix (`conductor/mod.rs:288-332`, `398-419`). The next window opens when `cap > sync_boundary_slot(prev window)` (`conductor/deadline_agreement.rs:58-77`). With `NopAcs` the decision is immediate (`conductor/acs/nop.rs:37-39`) and the deadline is the natural schedule (`deadline_agreement.rs:71`). A slot that never completes stalls window opening. The only rescue is a peer `CapAdvance` at least `lag_threshold` ahead (`conductor/mod.rs:425-443`).
- **Timers.** `ScheduleTimer` is relative to the runtime clock when the output is drained (`runtime.rs:174-177`). Despite the "last one wins" doc (`slot/mod.rs:60-62`), `Timers::schedule` never dedupes (`timers.rs:48-51`), and timers are never cancelled on close (`runtime.rs:127-129`).

### 3.2 Mechanism (chosen)

The pq slot emits these outputs:

| Paper event | Slot output | Consumer |
|---|---|---|
| speculative finalize (`ln:qv:speculate`) | `CommitOptimistic(PqEvent::Speculative { metablock })` | node: log only (no execution yet) |
| finalize (`ln:qv:fastdecide`, `ln:qv:fallbackdecide`, `ln:qv:echo`, `ln:qv:halt`) | `DA(PinFinal{j,pid})` ×K, `DA(AddDecodedHolders{..})`, then `CommitOptimistic(PqEvent::Final { metablock, path, at_view })` | node: ledger join, mempool settle, the proposing chaining gate, the `finalized` log line |
| evidence detected | `CommitOptimistic(PqEvent::Evidence(Evidence))` | node: evidence sink + `warn!` |
| halt (`ln:qv:halt`) | `Finalize(PqHalt { metablock, path })` | runtime: conductor completion, slot close. Node: hands `PqMessage::Finalized(x)` to the Repeater |

`OptimisticCommitData = PqEvent` is therefore the slot's observer event channel. The name is upstream's; the semantics are documented here.

**Why halt-time `Finalize` is right.**
1. Paper §6 (`paper:sections/6-conductor.tex`, "Completion at halting") defines completion at halt, and the halting lemma (`lem:qv:eventualhalting`) guarantees it in every slot all correct validators participate in.
2. The slot must keep running Coda and the gadget after finalizing (`paper:sections/4-quantum-chorus.tex:92,182`). `close` stops input delivery, so `Finalize` cannot come earlier.
3. The halt-time cost is about one extra quorum hop of cap latency. It does not affect the ledger (which keys on `PqEvent::Final`) or proposal sealing (section 3.4).

**Alternative rejected:** duplicate `runtime.rs` + `slot_manager.rs` (about 470 lines) to add a post-finalize phase and a pre-open Finalized relay. It costs a fork of the runtime that tracks upstream, and the only gain is the §6 relay, which §3.3 shows is not needed under the fixed `NopAcs` schedule.

### 3.3 Messages for unopened and closed slots

- **Unopened.** Dropped by the runtime. This matches the paper's rule that "a validator keeps no message of the slot that arrives before it starts participating" (`paper:4-quantum-chorus.tex:27`).
- **The §6 Finalized relay for the current and next window is not implemented.** Under `NopAcs` every validator opens a window at the same natural deadlines, as soon as its own cap passes the sync boundary. Finalized messages of halted slots stay alive in the node Repeater until `cap - certificate_retention` (section 7). A validator that opens the slot late therefore still receives them. A validator more than `lag_threshold` behind jumps via `CapAdvance`. Deviation D5.
- **Closed (halted).** Dropped. The paper agrees: "a validator that has halted handles no more messages of the slot" (`paper:4-quantum-chorus.tex:182`).

### 3.4 Ledger latency, cap and window

- **Ledger.** Written at `PqEvent::Final` + DA `Resolved`. Halt is not on the ledger path.
- **Cap.** Advances at halt, about finalize + one quorum hop (mcp_test: about 90 ms).
  - Window opening needs `(slots_per_window - sync_boundary_slots + 1) · slot_interval` ≥ halt latency + Δ + propose lead. mcp_test has `slots_per_window=100, sync_boundary_slots=80`, so 21 · 100 ms = 2.1 s ≥ about 0.4 s on the fast path and about 0.9 s on Coda view 1. OK.
  - In-flight slots are bounded by about `2·slots_per_window - sync_boundary_slots` = 120 (from `conductor/mod.rs:398-419`, `deadline_agreement.rs:58-77`).
- **Chaining gate.** `ProposalPlanner` seals slot r only when `r < chained_cap + observation_cutoff` (`monad-mcp-chorus/src/proposing.rs:204-211`; `OBSERVATION_CUTOFF = 5`, `monad-mcp-node/src/config.rs:332`).
  - Timings below use the worst-case one-way h_max = 120 ms. With the quorum-latency figure h_q = 90 ms (section 8.2), finalize is D+150 and halt D+240.
  - If fed the conductor cap, slot r would need slot r−5 to have *halted* by `D_r − 150 ms`: D_{r−5} + 210 ms (fast finalize) + 120 ms (halt) = `D_r − 170 ms`. A 20 ms margin is too thin.
  - Instead, `PqNodeRuntime` feeds `ProposingInput::CapAdvance(max(local_final_prefix, conductor_cap))` (M1).
    - `local_final_prefix` is the contiguous prefix of `PqEvent::Final` slots.
    - `conductor_cap` comes from `handle_chain_advance`, which covers slots skipped by a `CapAdvance` jump that never finalize locally.
    - The value only grows.
  - The margin becomes `D_r − 150 − (D_{r−5} + 210)` = 140 ms. This is node-local policy (the planner holds no consensus state, `proposing.rs:58-62`). Deviation D15.
- **No fallback cascade after a Coda slot** (M2). Slot r on Coda view 1 finalizes at about D_r + 480 ms, so the gate blocks sealing of slots r+5 and r+6 until then. Those seals fall 20–120 ms before their deadlines.
  - Set `withhold_before_deadline = L − 10` (140 ms on mcp_test). The late seal is then withheld rather than disseminated to only part of the validator set (`proposing.rs` `min_lead`).
  - Every validator then votes ⊥ for that lane, and the slot still takes the fast path (fast ⊥).
  - With `withhold = 100`, a seal at D−120 would be disseminated late. It could split, and each Coda slot could cause the next one.
- **Never emit `Finalize` twice.** The runtime does not dedupe; the conductor warns "duplicate slot completion" (`conductor/mod.rs:300-303`). Guard with `halted: bool`.

---

## 4. Q2: DA (duplicated as monad-pq-da)

### 4.1 Facts

- **`DARuntime<S>` is generic only over the schedule** (`monad-mcp-da/src/runtime.rs:79`). It is hard-wired to `ChorusDAEvent` (output `DAOutput::Consensus`, `runtime.rs:242-263`) and to `ChorusDACommand` (`runtime.rs:202`). The node component `component::DA = DARuntime<NodeProposerSchedule>` with `DAInput` (`monad-mcp-node/src/component/mod.rs:30-36`, `component/da.rs`) is bound to upstream DA. With `monad-pq-da`, the pq node carries its own copy, `PqDA`.
- **Events** (`monad-mcp-chorus/src/slot/chorus.rs:156-174`):
  - `HeaderSeen(SignedProposalHeader)` fires once per pid, for at most the first two distinct pids of a proposer (`monad-mcp-da/src/proposer_rc/mod.rs:203-217`, `177-187`).
  - `ProposerObligationFulfilled(pid)` fires once the self-owned chunks sent by the author have arrived proof-verified. It is trimmed to ceil(owed·10/11) (`instance_rc/mod.rs:169-176`, `obligation_tracker.rs:19-56`).
  - `Decoded(pid)` means decode, then re-encode, then root equality. `DecodingFailed(pid)` is the canonical-encoding failure (`proposer_rc/mod.rs:163-171`, `instance_rc/mod.rs:288-295`).
- **No timestamps.** Consensus snapshots at D itself.
- **Forwarding.** Every owner re-broadcasts each proof-verified owned chunk to all validators except itself and the author, once, gated by `ReleaseChunks` (`egress.rs:35-40`, `85-90`). Recovery is pull-based: first pull at D+Δ, then every 2Δ (`runtime.rs:60-66`). Chorus releases at D (`monad-mcp-chorus/src/slot/fast.rs:292`).
- **Decodability.** S11 (the node's scheme, `monad-mcp-node/src/runtime.rs:292`) guarantees "every set of weight > W/3 still decodes after packet loss trims" (`monad-mcp-da/src/encoding_scheme/swiper/mod.rs:82`). That is the paper's "any f+1 symbols suffice" under equal stake. The D25 stub repetition code (redundancy 2.5) does not meet this; tests must use S11 when DA decodability matters.
- **Header signature.** The stub 65-byte recoverable checksum (`monad-mcp-da/src/spec.rs:26`, `header.rs:43-65`, `monad-mcp-chorus/src/env/stub.rs:273-277`).

### 4.2 Mapping of Alg. 1 needs

| Alg. 1 need | DA realization | Where in QuantumChorus |
|---|---|---|
| `ln:qv:chunk`: valid chunk of j before D sets `chunk_i[j]` | the first `ProposerObligationFulfilled(pid)` (or `Decoded(pid)`) for j with a seen header, received **before** `handle_deadline`. Same rule as `ProposalAvailability::fetch_proposal` (`monad-mcp-chorus/src/slot/availability.rs:84-89`). Three edge cases: (1) `fetch_proposal` returns `None` while the header is unseen, even after `Decoded(pid)`. Every chunk packet carries the header, so `HeaderSeen` precedes in practice. (2) With `owed = 0`, the obligation fires on the first header (`obligation_tracker.rs:85-96`). Equal stake gives every validator ≥ 1 ticket, and the config check asserts it. (3) DA drops chunks for slots not yet opened (`monad-mcp-da/src/runtime.rs:122-124`). Whole windows open ahead of their deadlines, so this only affects a node that opens late | `handle_da_event` → `self.da[j].ingest(..)`. At vote time, `chunk[j] = da[j].fetch_proposal()` if `!deadline_passed`. Vacant j: fixed ⊥ (section 10.1) |
| `ln:qv:allornothing`: vote carries chunks for positive entries | **not required** (D1). Votes are accepted on signature + completeness + one per sender. The voter's chunks travel by DA forwarding, released at vote time | `on_vote` |
| "decode from the f+1 chunks attached and re-encode" (`ln:qv:decode`, `ln:qv:adoptcheck`, `ln:qv:speccheck`, `ln:qv:fastcheck`) | `checked(j, r)`: `Some(r)` if `da[j].is_decoded(r)`, `Some(Bot)` if `DecodingFailed(r)`, `None` (wait) otherwise. The result is chunk-set independent, as the paper notes (`paper:4-quantum-chorus.tex:92`) | `slot::availability::ProposalAvailability::{is_decoded,is_resolved}` |
| "fast entry ⟨fast, r⟩ accepted once f+1 positive ⟨j,r⟩ with chunks" (`ln:qv:accept`) | `pos[j][r].len() ≥ weak` **and** `da[j].is_resolved(r)` | `second_round::try_accept` |
| proposal retrieval for the ledger after finalize | `PinFinal{j, Some(pid)}` per root, `PinFinal{j, None}` per ⊥, then `DAOutput::Resolved{slot, j, pid, Result<Bytes, InvalidEncoding>}`. `AddDecodedHolders{j, pid, node_ids}` with the senders of ⟨fallback, pid⟩ entries, who decoded by construction (`ln:qv:decode`) | `finalize()`. The node joins `Resolved` in `PqFinalizationCollector` |
| release of chunks of non-finalized roots | `PinFinal` drops other roots (`proposer_rc/mod.rs:236-252`); slot GC on `CapAdvance` + `completed_slot_retention` | automatic |
| header for an unknown voted pid | `PullHeader{j, missing: [(pid, voters)]}` once `pos[j][pid].len() ≥ weak` and no `HeaderSeen(pid)`. It is **re-emitted on every `Retransmit` tick** until `HeaderSeen(pid)` (upstream emits it once) | `votes::on_new_positive`, `retransmit::tick` |
| data of any root with ≥ weak positive voters (needed by `ln:qv:decode`, `ln:qv:accept`, `ln:qv:adoptcheck`, `ln:qv:fastcheck`) | **new** `PqDACommand::PullRoot{j, pid, voters}`, emitted once `pos[j][pid].len() ≥ weak` (non-vacant j), with the voter set updated as votes arrive. monad-pq-da tracks the pid and sends `YourChunks` requests to those voters every 2Δ until `Decoded`/`DecodingFailed` or the slot is GC'd (section 4.5) | `votes::on_new_positive` |
| equivocating proposer | two `HeaderSeen` with distinct pids for one j → `EquivCert`, assembled consumer-side (`availability.rs:71-82`) | `evidence::on_header` |

### 4.3 Gaps and how they are handled

1. **No per-sender chunk event.** The paper's all-or-nothing acceptance (`ln:qv:allornothing`) cannot be checked. The relaxation is sound:
   - **Agreement** uses the chunks only through the check result, and that is chunk-set independent.
   - **Liveness of acceptance.** A correct sender of ⟨fast, r⟩ saw `strong` positive votes, so at least `weak` correct holders release and forward their owned chunks. DA decodes within Δ after GST, or via pull.
   - **The one new failure mode.** At `ln:qv:decode` (the path vote), the paper always holds the chunks. Here, f Byzantine voters may vote positive without forwarding. A correct validator then waits for `Decoded/DecodingFailed(r)` up to `decode_grace` (≤ Δ/2) past D+Δ and falls back to ⟨fallback, ⊥⟩.
   - **Fallback entries are unconstrained in clean sets**, so agreement is unaffected. Proposal inclusion is unaffected: a correct proposer yields fast entries. Deviation D1.
2. **Coda-input stall with upstream DA (B1, the reason for duplicating DA).** Three upstream properties combine:
   - A proposer index tracks at most its first two headers before commit, and only pids with a recorded header (`proposer_rc/mod.rs:177-187`).
   - Pulls run only for `pin.pid()`: the *first-seen* pid while `Open` (`proposer_rc/mod.rs:254-269`).
   - Owned chunks are forwarded once (`slot_rc.rs:363-376`), and `PullHeader` is one-shot.

   Now take an equivocating proposer with roots r1 and r2. A correct node N first sees r1, or N is partitioned over [D, D+Δ] and misses the forwards. Then N never pulls r2. If ≥ weak correct validators send ⟨fast, r2⟩ or ⟨fallback, r2⟩, N cannot accept the fast entries, because acceptance needs `resolved(r2)`. N may then never hold a clean set and never inputs to Coda. Coda termination needs every correct validator to input. The halting gadget helps only if some correct validator halts, and with Coda stuck none may.

   `monad-pq-da` fixes this with `PullRoot` (any root with ≥ weak positive voters is tracked and pulled from them until resolved) plus `PullHeader` retries. At most ⌊n/weak⌋ ≤ 3 roots per index can have ≥ weak voters, because each validator votes one root per index. That bounds the extra tracking.
3. **The PQ proposer-header signature cannot be plugged in yet.** `SIGNATURE_LEN = 65` and the v1 layout are fixed (`monad-mcp-da/src/spec.rs:26`; `wire/v1.rs`). DA headers are signed over `version | scheme header` without a domain prefix (`wire/v1.rs:169-172`). This is unlike consensus signing bytes (`monad-mcp-chorus/src/spec.rs:105-116`). The variable-length env goes into `monad-pq-da` in WP8.
4. **n ≡ 0 (mod 3).** With equal stake, weak = f+1 = n/3 is not more than W/3. S11's "weight > W/3 decodes" (`swiper/mod.rs:82`) then does not guarantee decoding from weak correct holders. mcp_test (n=8) and n = 4 or 7 are fine. Config load documents this and `warn!`s for n ≡ 0 (mod 3). It does not reject: decoding then relies on a fast quorum (strong − f > W/3 holders).

### 4.4 When to release

Emit `ReleaseChunks` at the vote: at D, or at the early vote. This is the DA image of the paper's "attach C to the Vote", and it keeps Chorus's hiding behaviour. `QuantumConfig.release_chunks = Vote | Receipt` exists so the paper §7 evaluation variant (forward on arrival, emitted from the first `handle_da_event`) can be measured. The default is `Vote`.

### 4.5 `monad-pq-da`: the changes against `monad-mcp-da`

All modules are copied verbatim (encoding_scheme/{d25, swiper, stub}, chunk_tree, assignment, wire, egress, instance_rc, proposer_rc, slot_rc, runtime, header, spec, env). `DAOutput` and `ChorusDAEvent` are unchanged. Commands become `monad_pq_chorus::da::PqDACommand`:

```rust
pub enum PqDACommand {
    ReleaseChunks, AddDecodedHolders { j, pid, node_ids }, PinFinal { j, pid: Option<ProposalId> },
    PullHeader { j, missing: MissingHeaders }, IngestProposalHeader { sender, header },   // as ChorusDACommand
    PullRoot { j: ProposalIndex, pid: ProposalId, voters: Vec<NodeId> },                 // new
}
```

| Change | File (copied) | Behaviour |
|---|---|---|
| tracking | `proposer_rc/mod.rs` `tracks` | While `Open`, track a pid if its header is recorded **or** it is in `vouched` (pids named by `PullRoot`). A third or later distinct header for a vouched pid is recorded, and `HeaderSeen` is emitted for it. Bound: the size of `vouched` ≤ ⌊n/weak⌋ per index |
| pulling | `proposer_rc/mod.rs` `poll_chunk_requests`, `proposer_rc/recovery.rs` | One `Recovery` per vouched pid, not only `pin.pid()`. It sends `YourChunks` to `voters`, then `MyChunks` to decoded holders. The first pull is at once, then every `pull_interval` (2Δ), until resolved, `PinFinal` for another pid, or slot GC |
| `PullHeader` | `slot_rc.rs` | unchanged semantics. Consensus re-issues it on each retransmit tick; the requests are rate-limited by the existing serving quota |
| `PinFinal` | unchanged | drops the other roots, `vouched` included |
| tests | copied tests + new: `pull_root_recovers_a_rival_root`, `third_root_tracked_when_vouched`, `pull_retries_until_decoded` | |

The node wire format is unchanged (`Packet::Chunk`, `Packet::ChunkRequest`).

---

## 5. Q3–Q5: RPC, node and explorer reuse

### 5.1 RPC (unchanged)

- **Config parse.** The rpc parses the node file as `monad_mcp_node::config::NodeConfig` (`monad-mcp-rpc/src/config.rs:232-236`). Its own table is parsed separately through `ConfigFile { rpc }` (`config.rs:145-149`, `RpcSection::parse` at `200-203`). The rpc takes `monad-mcp-node` as a path dep (`monad-mcp-rpc/Cargo.toml:30`). `NodeConfig` has no `deny_unknown_fields` (`monad-mcp-node/src/config.rs:38-65`). Only `LeaderElectionConfig` (`config.rs:265`) and `RpcSection` (`monad-mcp-rpc/src/config.rs:123-125`) are strict. So:
  - Every required `NodeConfig` key is kept with stub-u64 types.
  - All pq parameters go in a new top-level `[quantum]` table.
  - No new keys go in `[leader_election]` or `[rpc]`.
- **Proposer schedule.** The rpc recomputes it with `NodeConfig::epoch_handle()` (`monad-mcp-rpc/src/schedule.rs`). The pq node calls the same function (`monad-mcp-node/src/config.rs:136-152`), so routing matches by construction.
- **Wire.** `encode_frame(sender, &Packet::Tx(rlp))` (`monad-mcp-rpc/src/udp.rs:31-33`). The pq node uses `monad_mcp_node::network::stub::UdpNetwork` unchanged, which provides kind 4 and the validator-set sender filter.
- **Ledger follow.** `LedgerFollower` over `<dir>/blocks` (`monad-mcp-chorus/src/ledger/store.rs:474`). Satisfied by §5.3.
- **Operational.** Run `[proposal] source = "mempool"`.

### 5.2 Node library reuse (Q4)

`monad-mcp-node/src/lib.rs:16-31,42-49`: private `async_node, clock, epoch, finalization, logging, node, runtime`; pub `component, config, ledger, network`; re-exports `EpochHandle, NodeProposerSchedule, FinalizedSlot, init_logging, Node, Effect, NodeRuntime, Runtime`.

| Item | Use |
|---|---|
| `network::{Packet, Inbound, Outbound, NetworkHandle, stub::UdpNetwork, encode_frame, decode_frame}` (`network/mod.rs:134-141`, `network/stub.rs:31-40,162-177`) | reuse |
| `component::{Component, Dispatch, Link}` (`component/mod.rs`) | reuse |
| `component::spawn` + private `run`, `sleep_until_due` (`component/mod.rs:68,79,108`) and the private `clock::Clock` | duplicate together (`spawn` takes the unnameable `Clock`) |
| `component::{DA, DAInput}` (`component/da.rs`) | duplicate as `PqDA` over `monad_pq_da::DARuntime` |
| `component::{Proposing, ProposingInput, ProposingOutput}` (needs `&EpochHandle`, `&ProposalConfig`) | reuse |
| `component::{Repeater, RepeaterConfig, RepeaterInput, RepeaterOutput, Recipients}`, generic `M` (`component/repeater.rs:26-72`) | reuse with `M = PqMessage`, Finalization-only (section 7) |
| `component::mempool::*` | reuse |
| `config::NodeConfig` + `epoch_handle()`, `peers()`, `repeater()`, `cadence.conductor(..)`, `validate()` | reuse |
| `EpochHandle` (all fields pub) | reuse. `QuantumContext` is built from its fields |
| `ledger::LedgerSink` (`ledger.rs:125-181`) | reuse |
| `init_logging` | reuse |
| `component/cadence.rs`: `Conductor`/`CadenceWireMsg`/`CadenceRuntime<Chorus,_>` hard-wired (`cadence.rs:15-16,36`) | duplicate as `PqCadence` |
| `runtime.rs`: `Effect::Repeater(RepeaterInput<ChorusMessage>)`, `Effect::Ledger(FinalizedSlot)` (`runtime.rs:41-49`), uses `.path()/.pids()/.certificate_message()` (`runtime.rs:229-250`) | duplicate |
| `node.rs`, `async_node.rs`, `finalization.rs` (`SlotFinalization`-typed, `finalization.rs:32-44`) | duplicate |
| `ledger::{new_block, log_finalized, Recorder}` (`ledger.rs:41-122`) | pub, but typed on `FinalizedSlot`/`SlotFinalization`; duplicate |

### 5.3 Explorer (unchanged)

- **Deps.** `monad-mcp-chorus` ledger types only.
- **Proof.** `proof.rlp` is opaque; the explorer reads only its size and hex (`monad-mcp-explorer/src/api.rs:428,534-545`).
- **Paths.** It assumes the two variants of the ledger's `FinalizationPath` (`monad-mcp-chorus/src/ledger/block.rs:30-33`). This is a different type from the Chorus-internal `FinalizationPath` (`slot/chorus.rs:114`). See also `monad-mcp-explorer/src/index.rs:228-232`; `stats.rs:106`).
- **pq mapping.** `PqPath::Fast → Fast`, `PqPath::Coda → Fallback`, `PqPath::Gadget → Fallback`. The exact path goes in the proof (`PqProof.path`). See Open Question O2.

---

## 6. Messages, signatures, sizes

### 6.1 Types

```rust
// types.rs
pub enum Entry { Root(ProposalId), Bot }                 // vote entry, metablock value (Value = root | ⊥)
pub type Metablock = ProposalMap<Entry>;                 // ProposalMap has RLP (monad-mcp-chorus/src/types.rs:646,782,795)
pub enum PathEntry { Fast(Entry), Fallback(Entry) }      // Alg. 1 entry_i[j]
pub enum CodaValue<V> { Value(V), Empty }                // Empty = EMPTY (ALLSAFE), never ⊥/none
pub type CodaMetablock<V> = ProposalMap<CodaValue<V>>;

// sig.rs
pub struct PqSignature(pub Bytes);                       // variable length, opaque
pub trait PqSigner: Send + Sync { fn sign(&self, msg: &[u8]) -> PqSignature; }
pub trait PqVerifier: Send + Sync {
    fn verify(&self, signer: NodeId, msg: &[u8], sig: &PqSignature) -> bool; // key lookup by NodeId; non-recoverable
    fn max_sig_len(&self) -> usize;
}
pub struct StubPq { len: usize }  // sig = keccak256(signer_id || msg) padded to `len` (default 32); `len` emulates ML-DSA sizes
pub struct Signed<B> { pub body: B, pub sig: PqSignature }
// signing bytes = Domain::PREFIX ++ rlp([slot, signer, body])  (monad-mcp-chorus spec.rs:105-116 convention, assert_signing_prefix)
pub struct VoteDomain;     // PREFIX = b"\x18monad/pq-cadence/vote/1\n"
pub struct PathVoteDomain; // PREFIX = b"\x1dmonad/pq-cadence/path-vote/1\n"

// message.rs  (RLP: [tag_u8, ..], like ChorusMessage, chorus.rs:583-669)
pub struct VoteBody { pub entries: ProposalMap<Entry> }           // E
pub struct PathVoteBody { pub entries: ProposalMap<PathEntry> }   // F
pub enum PqMessage {
    Vote(Signed<VoteBody>),            // tag 1
    PathVote(Signed<PathVoteBody>),    // tag 2
    Finalized(Metablock),              // tag 3, unsigned (authenticated channel)
    Coda(CodaMessage<Entry>),          // tag 4, unsigned
}
pub enum CodaMessage<V> {
    Prepare { view: u64, x: CodaMetablock<V> },                                   // 1
    Vote1   { view: u64, x: CodaMetablock<V> },                                   // 2
    Vote2   { view: u64, lock: Option<CodaMetablock<V>>, guard: CodaMetablock<V> }, // 3
    Echo    { view: u64, j: u16, v: CodaValue<V> },                               // 4
}
```

Notes:
- The signer is the authenticated channel sender. Signed bytes include `(slot, signer)`, so stored evidence is self-describing and transferable.
- Signatures are only for accountability (`paper:3-model.tex`, Cryptography). The paper's Coda and Finalized are unsigned.
- Vote entries name `ProposalId`, the pid of the header (root + scheme), following commit `a883fb23` ("vote on proposal id").

### 6.2 Signature config (forward-compatible)

`[quantum] signature = "stub"` with `stub_signature_len`. Later, `"ml-dsa-44"` with `secret_key_file` and `[[quantum.validators]] node_id, pubkey_hex`. That stays in `[quantum]`, so `NodeConfig` parsing is unaffected. `QuantumContext` holds `Arc<dyn PqSigner>` and `Arc<dyn PqVerifier>`. Verification cost lands in `on_vote` and `on_path_vote` only.

### 6.3 Size table (K = 5, RLP)

The payload budget is 1472 − 9 (frame header, `network/stub.rs:36-40`) = 1463 B. The slot wrapper `[1, slot, msg]` adds about 12 B (`message.rs:43-57`).

| Message | Body | Stub sig (32 B) framed | ML-DSA-44 (2420 B) framed | Notes |
|---|---|---|---|---|
| Vote (all positive) | 5 × 33 + 2 = 167 | ≈ 225 B | ≈ 2615 B (2 fragments) | negative entry = 1 B |
| PathVote | 5 × 36 + 2 = 182 | ≈ 240 B | ≈ 2630 B (2 fragments) | |
| Finalized | 167 | ≈ 185 B | ≈ 185 B | unsigned |
| Coda Prepare / Vote1 | ≤ 5 × 36 + 2 + 9 | ≈ 210 B | same | unsigned |
| Coda Vote2 | 2 metablocks ≈ 365 | ≈ 390 B | same | unsigned |
| Coda Echo | view + j + value ≈ 45 | ≈ 65 B | same | up to (c+1)·K per view per validator |

Limits under 1463 B unfragmented, with the stub sig: Vote K ≤ 42, PathVote K ≤ 38, **Vote2 K ≤ 19**, Prepare/Vote1 K ≤ 39.

### 6.4 Fragmentation hook (needed for ML-DSA, or K > 19)

`monad-pq-node/src/wire.rs`:

```rust
pub trait SlotWire {
    fn encode(&mut self, msg: &PqCadenceWireMsg) -> Vec<Bytes>;                 // ≥1 Packet::Cadence payloads
    fn decode(&mut self, from: NodeId, payload: Bytes, now: Timestamp) -> Option<PqCadenceWireMsg>;
}
```

- **v1 (`Unfragmented`).** One payload per message. If the payload exceeds 1463 B: `warn!("pq message exceeds frame, dropped", kind, len)`, plus a unit test that every v1 message at K=5 with the stub sig fits.
- **Later (`Fragmenting`).** A fragment payload starts with byte `0x01` (RLP list prefixes are ≥ 0xc0, so it is unambiguous), then `msg_id: u32, idx: u8, count: u8`, then the bytes.
  - The reassembler is keyed by `(from, msg_id)`, with a 2Δ TTL and at most 64 entries per sender.
  - It stays within `Packet::Cadence`, so `monad_mcp_node::network` is unchanged.

---

## 7. Retransmission and forever-alive (Q7)

The paper retransmits every broadcast until abandoned (`paper:3-model.tex`). Alg. 1 never abandons; Coda abandons views below V−1 on entering V (`ln:rule_abandon`).

| Phase | Mechanism | Retransmitted | Stops when |
|---|---|---|---|
| before halt | `slot/retransmit.rs`: an `alive: Vec<PqMessage>` set inside the slot, plus a single `Timer::Retransmit` chain. An `armed` flag is set when the first message becomes alive. Each firing re-arms one timer, so duplicates never multiply. Every `retransmit_interval` (default Δ) it re-emits `Broadcast(m)` for every alive m and re-issues pending `PullHeader`s | own Vote, PathVote, Finalized, Coda messages of views `view−1` and `view` (Prepare if leader, Vote1, Vote2, Echoes) | Coda view entry removes views < V−1 (exact `ln:rule_abandon`). Halt drops all of them |
| after halt | node `Repeater<PqMessage>` fed **only** `RepeaterInput::Finalization(slot, PqMessage::Finalized(x))` at halt, then `CapAdvance`. `RepeaterInput::Completed(slot)` (`repeater.rs:65-70`) is forwarded on `SlotLifecycle::Completed`, as upstream does (`runtime.rs:217-218`); it only drops undecided records, of which there are none | `Finalized(x)` to everyone, every `[repeater] interval` | `handle_cap_advance` keeps a certificate while `slot ≥ cap − certificate_retention` **or** it has never been repeated, so each is sent at least once (`component/repeater.rs:123-135`) |
| slot lifetime | runtime `CloseSlots{cap}` drops the instance (`slot_manager.rs:86-98`) | none | cap |

- **The node does not `Record` slot broadcasts in the Repeater.** This differs from `monad-mcp-node/src/runtime.rs` (records at 118-123). In-slot retransmission already covers them, with exact abandonment semantics.
- **Dropping all but Finalized at halt is safe** (deviation D6). Suppose correct validator p halts at t. Then p received `strong` matching `Finalized(x)`, and at least `strong − f ≥ weak` of them are from correct validators C. Those validators keep `Finalized(x)` alive: in-slot until their own halt, then in the Repeater. Every correct validator q in the slot therefore eventually receives `weak` copies, finalizes x (`ln:qv:echo`) and broadcasts `Finalized(x)`. With all correct validators broadcasting, q receives `strong` and halts (`ln:qv:halt`). This argument uses no Vote, PathVote or Coda message after t, so abandoning them at halt does not affect any correct validator's finalization or halting (`clm:qv:halting`, `lem:qv:eventualhalting`).
  - Agreement is unaffected, because halting validators send nothing new.
  - The only assumption is that the Repeater keeps `Finalized(x)` alive until every correct validator has halted. Retention of `certificate_retention` slots past the cap, plus the `CapAdvance` jump for nodes `lag_threshold` behind, provides that in practice.
  - The paper's never-abandon rule covers the remaining case, a node more than retention but less than `lag_threshold` behind. R7 tracks it: keep `certificate_retention ≥ lag_threshold`. The config check enforces this, and the defaults are 50 ≥ 20.
- **Self loopback.** The node loops self-broadcasts back (as `monad-mcp-node/src/runtime.rs:118-123`), so every handler is idempotent per sender.

---

## 8. Timers and latency budget (Q8)

### 8.1 Timers (all relative, scheduled in handlers)

| Timer | Scheduled in | Fires at | Handler |
|---|---|---|---|
| deadline | runtime | D | `handle_deadline`: set `deadline_passed`; vote if not voted; schedule `PathVoteGate`(Δ), `DecodeGrace`(Δ + g), `Adoption`(2Δ), `Accountability`(2Δ + g, 4Δ + g), where g = `decode_grace` |
| early vote | none (event) | when `early_vote` is set and `chunk[j]` is set for every **non-vacant** j | `try_vote`. Off by default (`QuantumConfig.early_vote = false`), so votes go out at D as in upstream Chorus (`monad-mcp-chorus/src/slot/fast.rs:288-324`). The early path vote (all lanes strong-matching, before D+Δ) is not gated |
| `PathVoteGate` | deadline | D+Δ | set `pv_gate`; `try_path_vote` |
| `DecodeGrace` | deadline | D+Δ+decode_grace | set `decode_grace_over`; `try_path_vote` |
| `Adoption` | deadline | D+2Δ | set `adopt_gate`; `try_adopt` |
| `CodaTimeout(V)` | `coda.enter(V)` | entry + 5Δ | `coda.on_timeout(V)` if `V == view` |
| `Retransmit` | first broadcast, then self | every `retransmit_interval` | re-broadcast alive set |
| `Accountability(k)` | deadline | D+2Δ, D+4Δ | evidence detectors (section 11) |

Duplicate timers can fire (`timers.rs:48-51`). Every handler checks a once-flag or the current view.

### 8.2 Budget

Notation: L = `propose_before_deadline`, h_q = one-way time until the `strong`-th fastest arrival, h_max = slowest correct one-way. mcp_test RTT is 6.7–238 ms, so h_max ≈ 120 ms; with n=8 and strong=6, h_q ≈ 90 ms (conservative). Tx path: rpc → proposer t_r (nearest proposer via the latency matrix, worst 120), wait for seal t_s ≤ slot_interval, then L to D.

**Fast path, mcp_test** (Δ=150, L=150, slot=100):

| step | time |
|---|---|
| seal + disseminate | D−150 |
| all chunks at every validator (max over 5 proposers) | ≤ D−30 → early vote, release chunks |
| `strong` votes + forwarded chunks (decode) | ≤ D+60 → path vote early (all-fast rule) |
| `strong` path votes, decode done | ≤ D+150 → **finalize**, ledger (D+210 if every hop takes h_max = 120; section 3.4 uses this) |
| `strong` Finalized | ≤ D+240 → halt (cap) (D+330 with h_max) |
| **e2e tx** | t_r + t_s + L + 150 ≤ 120 + 100 + 150 + 150 = **520 ms** worst; ≈ 30 + 50 + 150 + 150 = 380 ms typical |

**Split proposer → Coda view 1, mcp_test** (one proposer reaches between `weak` and 2f validators by D; all others timely):

| step | time |
|---|---|
| votes (early for some, at D for the rest) | ≤ D+90 at `strong` |
| forwarded chunks of positive voters (released at their vote ≤ D) | ≤ D+120 → `Decoded(r)` |
| path vote at the D+Δ gate (not all fast) | D+150; fallback entry r for the split proposer |
| `strong` path votes received + accepted | ≤ D+240 |
| adoption at max(D+2Δ, clean sets) | D+300 → Coda view 1 Vote1 |
| `strong` Vote1 → lock, Vote2 | D+390 |
| `strong` Vote2 → decide → **finalize** | D+480 (paper bound D+4Δ = D+600) |
| **e2e tx** | ≤ 120 + 100 + 150 + 480 = **850 ms**; typical ≈ 710 ms; at the paper's bound 970 ms |

**Effect of `decode_grace` (g, M5).** g matters only when some proposer has ≥ weak positive votes and its root is unresolved at D+Δ. In that case:
- path votes go out by D+Δ+g, so adoption happens by D+2Δ+g;
- Coda view-1 finalize happens by D+4Δ+g, which is D+675 on mcp_test with g = 75;
- e2e worst case is 120 + 100 + 150 + 675 = 1045 ms.

The paper's timely-slot bounds (`lem:qv:acc-half` at D+2Δ, `lem:qv:acc-f` at D+4Δ) shift by g, so the accountability timers fire at D+2Δ+g and D+4Δ+g. In the benign split-proposer case, decode completes by about D+120 < D+Δ, so g is not on the path (risk R6).

**Later views** (faulty leaders) start after a 5Δ = 750 ms timeout and exceed 1 s by design. They occur only under faults or asynchrony.

**Local** (Δ=50, L=100, slot=100, hops < 1 ms):
- fast: e2e ≈ t_s + L + ~2 ms ≈ 100–200 ms.
- Coda view 1: L + 2Δ + 2 hops ≈ 200 ms after seal, so e2e ≤ 300 ms.

**Single node.** n=1, f=0, weak=strong=1. Everything cascades within one handler chain: own chunk → early vote → path vote → finalize → Finalized → halt. e2e ≈ t_s + L.

### 8.3 Recommended values

| | delta | slot_interval | propose_before_deadline | withhold_before_deadline | slots_per_window / sync_boundary / lag_threshold | retransmit_interval | decode_grace | repeater interval / retention |
|---|---|---|---|---|---|---|---|---|
| local single / swarm | 50 | 100 | 100 | 90 | 100 / 80 / 20 | 50 | 25 | 200 / 50 |
| mcp_test | 150 | 100 | 150 | 140 | 100 / 80 / 20 | 150 | 75 | 500 / 50 |

`withhold_before_deadline = L − 10` keeps late seals (for example behind the chaining gate after a Coda slot) from being disseminated to only part of the validator set (section 3.4). `decode_grace ≤ Δ/2` is a config check.

Keep Δ=150 on mcp_test. Δ=120 would shave 60 ms off the Coda path but sits at h_max with no margin; the Coda timeout (5Δ) and adoption depend on it.

---

## 9. Coda (Q9): data structures and handlers

### 9.1 State (persistent-memory layout of the paper, in memory)

```rust
pub struct Coda<V: Clone + Eq + Hash + Ord> {
    me: NodeId, k: usize, q: Quorum, validators: Arc<[NodeId]>, slot: Slot,
    view: u64,                         // ln:core:ps_view (0 = not started)
    timeout_elapsed: bool,             // "5Δ elapsed since entering view_i" (time of entry = timer)
    decided: Option<CodaMetablock<V>>,
    own: BTreeMap<u64, OwnView<V>>,    // only {view-1, view}  (line:persistant_memory)
    rx: BTreeMap<u64, ReceivedView<V>>,// views ≥ view-1 that some sender's window still covers
    sender_views: HashMap<NodeId, [u64; 2]>, // per sender: its two highest views seen (M3)
    out: VecDeque<CodaOutput<V>>,      // Broadcast(CodaMessage) | ScheduleTimeout(view) | Abandon(below_view) | Decide(mb)
}
pub struct OwnView<V> {
    guard: Option<CodaMetablock<V>>,   // ln:core:pv_guard
    lock: Option<CodaMetablock<V>>,    // ln:core:pv_lock
    vote1: Option<CodaMetablock<V>>,   // ln:core:pv_votes
    vote2: Option<(Option<CodaMetablock<V>>, CodaMetablock<V>)>,
    echoes: HashSet<(u16, CodaValue<V>)>, // ln:core:pv_echo_set
}
pub struct ReceivedView<V> {
    prepare: Option<CodaMetablock<V>>,                       // first Prepare from leader(view)
    vote1: HashMap<NodeId, CodaMetablock<V>>,                // first per sender
    vote1_count: HashMap<CodaMetablock<V>, usize>,
    vote2_raw: HashMap<NodeId, (Option<CodaMetablock<V>>, CodaMetablock<V>)>, // first per sender (decision rule counts these)
    vote2_lock_count: HashMap<CodaMetablock<V>, usize>,      // over vote2_raw, lock ≠ none
    accepted: HashSet<NodeId>,                               // ln:vc:rule_one_vote2_per_sender
    pending_lock: HashMap<CodaMetablock<V>, Vec<NodeId>>,    // vote2 awaiting f+1 vote1 for its lock
    u: usize,                                                // |U|
    l: HashMap<CodaMetablock<V>, usize>,                     // |L_ℓ|
    c: Vec<HashMap<CodaValue<V>, usize>>,                    // c_j(v) over U's guards, j < k
    echo: HashMap<(u16, CodaValue<V>), BTreeSet<NodeId>>,    // senders per (j, v)
}
```

**Memory bound (M3).** The bound is per sender, not relative to the local view:
- A message of view W from sender s is kept only if W ≥ view − 1 and W is among the two highest views seen from s.
- When s moves to a higher view, its entries in views below its second-highest are removed from every `rx[W]`, together with their counts. A `rx[W]` with no remaining entries is dropped.
- A correct sender only sends in its current view and the one below (`clm:coda:echoes_per_view`, `ln:rule_abandon`), so nothing a correct validator still sends is ever discarded.
- A Byzantine sender cannot make the receiver hold more than two views of its messages.

Total received state is O(n · 2 · (3 + (c+1)k)) messages per slot, independent of view numbers. Catch-up (`ln:vc:upon_catch_up`) works for any V > view: the f+1 echoes it needs come from correct validators at their current views, which are always within those senders' windows. Own state stays two views (`line:persistant_memory`). Deviation D7.

### 9.2 Handlers (rule → function)

All rules are evaluated by `Coda::settle()`, a fixpoint loop run after every input, so "fires as soon as its condition holds" holds, including for kept messages a view entry completes (`paper:5-coda.tex:67`).

| Paper label | Function | Logic |
|---|---|---|
| `ln:core:init_header`, `init_set_proposal`, `init_enter_view1` | `Coda::start(input)` | `own[1].guard = input`; `enter(1)` |
| `ln:core:on_enter_header`, `on_enter_set_view` | `enter(V)` | `view = V`; `timeout_elapsed = false`; drop `own`/`rx` < V−1; emit `Abandon(V−1)` (`ln:rule_abandon`) and `ScheduleTimeout(V)` |
| `ln:core:on_enter_dummy_view`, `on_enter_dummy_vote1` | `enter(1)` | `own[1].vote1 = guard`; broadcast `Vote1{1, guard}` |
| `ln:core:on_enter_if_leader` | `enter(V≥2)` | if `leader(V) == me`: broadcast `Prepare{V, own[V].guard}` |
| `ln:core:on_prepare_*` | `try_vote1()` | `rx[view].prepare = Some(x)` (only from `leader(view)`); if `justified(x) && own[view].vote1.is_none()` → set and broadcast `Vote1{view, x}` |
| `ln:core:fn_justified`, `fn_justified_return` | `justified(x)` | ∀j: `rx[view−1].echo[(j, x[j])].len() ≥ weak` |
| `ln:core:on_vote1_quorum_*` | `try_lock()` | ∃x: `rx[view].vote1_count[x] ≥ strong` and `own[view].vote2.is_none()` → `lock = x`, `vote2 = (Some x, guard)`, broadcast |
| `ln:core:on_vote2_quorum_header`, `_if_undecided` | `try_decide()` | for V ∈ {view−1, view}: ∃x: `rx[V].vote2_lock_count[x] ≥ strong` → `decided = x`, emit `Decide(x)` once |
| `ln:core:on_timer_*` | `on_timeout(V)` | if `V == view`: `timeout_elapsed = true`; if `vote2.is_none()`: `vote2 = (None, guard)`, broadcast |
| `ln:vc:rule_accept_vote2_bot` | `on_vote2` | lock = none → accept immediately: `u += 1`, `c[j][guard[j]] += 1` ∀j |
| `ln:vc:rule_accept_vote2_lock` | `on_vote2`, `on_vote1` | lock = x: accept when `vote1_count[x] ≥ weak` (re-checked through `pending_lock` on each Vote1): `l[x] += 1` |
| `ln:vc:rule_one_vote2_per_sender` | `on_vote2` | the first Vote2 per sender per view only (`vote2_raw`) |
| `ln:vc:on_vote2_quorum_compute_approved`, `set_approved_lock`, `set_approved_allsafe`, `guard_if`, `guard_echo`, `allsafe_echo`, `ln:rule_view_change_gate`, `ln:core:notation_every_witness` | `view_change_echoes()` (in `count.rs`), run when `timeout_elapsed` and on each newly accepted Vote2 of `view` | Claim `clm:coda:count_rule` over `rx[view]`: (1) ∀ℓ with `l[ℓ] ≥ 1 && l[ℓ] + u ≥ strong`: `echo(view, j, ℓ[j])` ∀j; (2) if `u ≥ strong`: ∀j, ∀v with `c[j][v] ≥ weak`: `echo(view, j, v)`; (3) ∀j with `Σ_w min(c[j][w], f) ≥ strong`: `echo(view, j, Empty)`. Cost O(\|M\|·k) per evaluation |
| `ln:vc:fn_echo`, `fn_echo_if`, `fn_echo_broadcast` | `echo(V, j, v)` | if `(j,v) ∉ own[V].echoes`: insert, broadcast `Echo{V, j, v}` |
| `ln:vc:upon_enter_next_view`, `upon_enter_set_guard`, `upon_enter_enter` | `try_advance()` | the max V ≥ view such that ∀j ∃v: `rx[V].echo[(j,v)].len() ≥ strong` → `own[V+1].guard = x` (per j, tie-break as below), `enter(V+1)` |
| `ln:vc:upon_catch_up`, `upon_catch_up_set_guard`, `upon_catch_up_enter` | `try_catch_up()` | the max V > view (unbounded) in `rx` such that ∀j ∃v: `echo.len() ≥ weak` → `own[V].guard = x`, `enter(V)` |
| `ln:vc:upon_echo_amplification`, `_invoke` | `amplify()` | V ∈ {view−1, view}, ∀(j,v) with `echo.len() ≥ weak` → `echo(V, j, v)` |
| `ln:core:storage_rule`, `line:persistant_memory` | `enter(V)` pruning | see the state section |

- **Leader.** `leader(V) = validators[(slot + V) mod n]` over `ValidatorData::nodes()` sorted (`monad-mcp-chorus/src/env/stub.rs:156`). Rotating the start by slot avoids one fixed first leader (D9).
- **Messages before `start`.** Buffered in `rx` under the same per-sender window. This is equivalent to the paper's redelivery by retransmission at start, and it saves up to one `retransmit_interval` on view 1.
- **Tie-breaks.** When two values for proposer j qualify in `try_advance` or `try_catch_up`, pick the one with more echo senders, then the smallest by `Ord`. The `Ord` order is `Value(Root(pid))` by pid, then `Value(Bot)`, then `Empty`.
- **Termination vs halting.** Coda has no stopping rule. On `Decide(x)`, QuantumChorus finalizes (`ln:qv:fallbackdecide`, Empty → Bot) and keeps feeding Coda until halt. At halt the instance is dropped with the slot. A Coda decision never closes the slot by itself.
- **Equal stake.** Required (D8). `Quorum` for n validators: `f = ⌊(n−1)/3⌋`, `weak = f+1`, `strong = ⌈(n+f+1)/2⌉`, so n=8 gives (2, 3, 6) and n=4 gives (1, 2, 3).
  - With n ≠ 3f+1, two different guard entries may each reach `weak` inside one S. The count rule echoes both. The safety arguments use only "q − f ≥ f+1 correct in S" and quorum intersection `2q − n ≥ f+1`, both of which hold. The proof's uniqueness remark (`b-coda-proofs.tex`, count_rule proof) is not needed for safety.

---

## 10. Quantum Chorus slot: state and rule map

```rust
pub struct QuantumChorus {
    slot: Slot, ctx: Arc<QuantumShared>, cfg: QuantumConfig, q: Quorum, k: usize,
    proposers: ProposerSet,
    // Alg. 1 local variables (persistent memory, in memory)
    chunk: ProposalMap<Option<ProposalId>>,        // chunk_i[j] (root; chunk bytes stay in DA)
    entry: ProposalMap<Option<PathEntry>>,         // entry_i[j]
    adopted: ProposalMap<Option<Entry>>,           // adopted_i[j]
    finalized: Option<(Metablock, PqPath)>,        // finalized_i
    halted: bool,
    sent_vote: Option<Signed<VoteBody>>, sent_path_vote: Option<Signed<PathVoteBody>>, sent_finalized: bool, speculated: bool,
    votes: BTreeMap<NodeId, Signed<VoteBody>>,       // votes_i
    path_votes: BTreeMap<NodeId, Signed<PathVoteBody>>, // pathVotes_i
    finalized_msgs: BTreeMap<NodeId, Metablock>,     // received Finalized, one per validator
    // derived
    tally: VoteTally,             // pos[j]: HashMap<ProposalId, BTreeSet<NodeId>>, neg[j]: BTreeSet<NodeId>
    second: SecondRound,          // accepted[j]: Vec<(NodeId, PathEntry)> (arrival order), pending_fast[j], clean[j]: Option<CleanSet>
    fast_tally: ProposalMap<HashMap<Entry, BTreeSet<NodeId>>>, // path votes with ⟨fast, v⟩ per j
    fin_tally: HashMap<Metablock, BTreeSet<NodeId>>,
    da: ProposalMap<ProposalAvailability>,
    flags: Gates { deadline_passed, pv_gate, decode_grace_over, adopt_gate },
    coda: Option<Coda<Entry>>, coda_buffer: Vec<(NodeId, CodaMessage<Entry>)>,
    alive: Retransmit, evidence: EvidenceLog, outputs: VecDeque<SlotOutput<Self>>,
}
```

`Config = QuantumConfig { delta, decode_grace, retransmit_interval, coda_timeout_deltas: u32 (=5), release_chunks, early_vote }`.

`DACommand = monad_pq_chorus::da::PqDACommand` (section 4.5).

`Context = QuantumContext { node_id, signer: Arc<dyn PqSigner>, verifier: Arc<dyn PqVerifier>, validator_data, header_auth, proposers: Arc<dyn ProposerSchedule + Send + Sync> }`, built from `monad_mcp_node::EpochHandle` (`monad-mcp-node/src/epoch.rs`).

`Timer = PqTimer { PathVoteGate, DecodeGrace, Adoption, Accountability(u8), Retransmit, Coda(u64) }`. `Message = PqMessage`. `DAEvent = ChorusDAEvent`. `OptimisticCommitData = PqEvent`. `FinalizationData = PqHalt`.

### 10.1 Vacant proposer indices (M4)

`ProposerSet::proposer(j) == None` (`monad-mcp-chorus/src/proposers.rs:335-375`) marks a vacant index. Rotation vacancies exist on every schedule, and for n=1 with K=5 most indices are vacant. For a vacant j:
- `chunk[j]`, `entry[j]` and `adopted[j]` are fixed to `Bot` / `Fast(Bot)` / `Bot`.
- j is excluded from the early-vote condition, `PullHeader`, `PullRoot`, decode waits, `try_speculate`'s and `try_fast_finalize`'s resolution checks, and every evidence detector.
- A Vote with `Root(_)` at a vacant j, or a PathVote with a root at a vacant j, is rejected as malformed (logged at debug, not counted). Negative entries there are still required for completeness, since `entries.len() == k`.
- Coda input and output for j are `Bot`. A Coda decision of `Value(Root)` at a vacant j cannot happen (obligation).

Scripted test: n=1, K=5 with 4 vacant lanes finalizes each slot from its own chunk alone. Another test: a vote with a root at a vacant j is rejected.

`new()` calls `proposers_at(slot)`. On error it emits nothing and stays inert, so no panic. Note that an inert slot never completes; that would be a wiring error, so log it at `error!`.

After every input, `settle()` runs these checks in order until nothing changes: `try_vote`, `try_accept`, `try_path_vote`, `try_speculate`, `try_fast_finalize`, `try_clean`, `try_adopt`, coda drain, `try_gadget`.

| Paper label | Function | Logic |
|---|---|---|
| `ln:qv:disseminate` | outside the slot: `Proposing` + `AssembledProposal::build_s11` in the node (`monad-mcp-node/src/runtime.rs:281-317`), reused | proposer seals at D − L |
| `ln:qv:chunk` | `handle_da_event` | `da[j].ingest(ev)`. `HeaderSeen` also feeds evidence. If `!deadline_passed && chunk[j].is_none()` → `chunk[j] = da[j].fetch_proposal().pid()`. `ReleaseChunks` on the first event if `release_chunks == Receipt` |
| `ln:qv:vote` (+ early vote) | `try_vote` | once; when `deadline_passed \|\| (early_vote && ∀ non-vacant j: chunk[j].is_some())`: `E[j] = chunk[j].map(Root).unwrap_or(Bot)`, sign, broadcast, add to `alive`, emit `DA(ReleaseChunks)` |
| `ln:qv:allornothing` | `on_vote(sender, v)` | sender in valset; `entries.len() == k`; `verify(sender, bytes, sig)`; no root at a vacant j; first per sender (a second distinct one → `Evidence::ConflictingVotes`); update `tally`; on reaching weak for a pid: `PullRoot` (and `PullHeader` if the header is unseen) |
| `ln:qv:fastroot`, `fastbot`, `decode`, `pathvote` | `try_path_vote` | once; fires when `(pv_gate && votes.len() ≥ strong) \|\| ∀j ∃v: count_j(v) ≥ strong`. Per j: if `pos[j][r] ≥ strong` → `Fast(Root r)`; else if `neg[j] ≥ strong` → `Fast(Bot)`; else if ∃r with `pos[j][r] ≥ weak` (choose the highest count, tie → smallest pid; the choice is fixed when first made) → `checked(j,r)`: `Some(v)` → `Fallback(v)`; `None` → **wait**, unless `decode_grace_over`, then `Fallback(Bot)`; else `Fallback(Bot)`. Sign, broadcast, add to `alive` |
| `ln:qv:speccheck`, `ln:qv:speculate` | `try_speculate` | once; if every own entry is `Fast(v)` and `finalized.is_none()`: wait until every root is resolved, map with `checked`, emit `CommitOptimistic(Speculative)` |
| `ln:qv:accept` | `try_accept` | path vote received (sig ok, one per sender, a second distinct one → evidence): `Fallback(_)` accepted at once; `Fast(Root r)` accepted when `pos[j][r] ≥ weak && da[j].is_resolved(r)`; `Fast(Bot)` when `neg[j] ≥ weak`. The pending ones are re-checked on vote and DA events |
| `ln:qv:clean` | `try_clean` | per j, while `clean[j].is_none()`: let F = accepted fallback, A_v = accepted `Fast(v)`. If ∃v with `\|A_v\| + \|F\| ≥ strong` → S = A_v ∪ F (first `strong` by arrival) for the v completed by the triggering entry; else if `\|F\| ≥ strong` → S = first `strong` of F. Recorded once ("first set obtained") |
| `ln:qv:adopt`, `adoptcheck`, `adoptbot`, `input` | `try_adopt` | once; `adopt_gate && ∀j clean[j]`: S has `Fast(Bot)` → Bot; S has `Fast(Root r)` → `checked(j,r)` (resolved by acceptance); else ∃r with ≥ weak `Fallback(Root r)` in S (possible for two roots when n ≠ 3f+1: highest count, then smallest pid) → Root r; else Bot. Then `coda = Coda::start(adopted)`, replay `coda_buffer`, `debug!("entering fallback coda")` |
| `ln:qv:fastcheck`, `fastdecide` | `try_fast_finalize` | ∀j ∃v_j: `fast_tally[j][v_j].len() ≥ strong`, and for roots `pos[j][v_j] ≥ weak && resolved` → x[j] = `checked(j, v_j)` → `finalize(x, Fast)` |
| `ln:qv:fallbackdecide` | coda `Decide(y)` | x[j] = `Empty → Bot`, `Value(v) → v` → `finalize(x, Coda)` |
| `ln:qv:echo` | `try_gadget` | ∃x: `fin_tally[x].len() ≥ weak` → `finalize(x, Gadget)` |
| `ln:qv:halt` | `try_gadget` | ∃x: `fin_tally[x].len() ≥ strong` → `finalize(x, Gadget)` if not finalized; `halt()` |
| finalize | `finalize(x, path)` | if not finalized: record; broadcast `Finalized(x)` + add to `alive`; `PinFinal` ×k; `AddDecodedHolders` (fallback-entry senders per root); `CommitOptimistic(Final)` |
| halt | `halt()` | once: `halted = true`; clear `alive`; `Finalize(PqHalt{x, path})`; ignore all later inputs |

After `finalize`, QuantumChorus keeps processing Vote/PathVote/Coda/Finalized until halt, as the paper requires (`paper:4-quantum-chorus.tex:92`).

`PqHalt { metablock, path }` exposes `pids() -> ProposalMap<Option<ProposalId>>`, `path() -> PqPath` and `finalized_message() -> PqMessage`, the analogues of `SlotFinalization::{pids, path, certificate_message}` (`monad-mcp-chorus/src/slot/chorus.rs:120-154`).

---

## 11. Accountability (Q10)

```rust
pub enum Evidence {
    EquivocatingHeaders { j: ProposalIndex, a: SignedProposalHeader, b: SignedProposalHeader }, // proves proposer faulty
    ConflictingVotes { signer: NodeId, a: Signed<VoteBody>, b: Signed<VoteBody> },             // proves voter faulty
    ConflictingPathVotes { signer: NodeId, a: Signed<PathVoteBody>, b: Signed<PathVoteBody> },
    PartialDissemination { j: ProposalIndex, header: SignedProposalHeader, negatives: Vec<(NodeId, Signed<VoteBody>)> },
    FallbackQuorum { j: ProposalIndex, path_votes: Vec<(NodeId, Signed<PathVoteBody>)> },     // lem:qv:acc-f case 1
}
```

| Detector | When | Source |
|---|---|---|
| `EquivocatingHeaders` | a second distinct `HeaderSeen` for j | `ProposalAvailability::ingest` returns `EquivCert` (`availability.rs:43-82`) |
| `ConflictingVotes` / `ConflictingPathVotes` | a second validly signed, different body from the same sender | `on_vote`, `on_path_vote` |
| `PartialDissemination` (`lem:qv:acc-half`) | `Accountability(D+2Δ)` with not finalized: per j holding a header and ≥ ⌊f/2⌋+1 negative votes | `tally.neg[j]` + `da[j].header_for` |
| `FallbackQuorum` (`lem:qv:acc-f`) | `Accountability(D+4Δ)` with not finalized: per j with ≥ strong path votes holding a fallback entry | `path_votes` |

Surfacing:
1. `CommitOptimistic(PqEvent::Evidence)` → node `EvidenceSink`, which hands it to an `evidence-writer` thread (never the event loop) that writes `<ledger.dir>/evidence/<slot:012>-<kind>-<culprit>.rlp`. RLP of `Evidence` contains signed bodies plus `(slot, signer)`, so a third party can verify it with the signer's pubkey.
2. Only the self-proving kinds (equivocating headers, conflicting votes, conflicting path votes) keep their plain kind and log `warn!("misbehavior proven")`. `FallbackQuorum` and `PartialDissemination` hold only for a timely slot, so they and vote dumps are filed as `suspect-<kind>` and logged as `"misbehavior suspected"`.
3. Files more than `[quantum] evidence_retention_slots` (default 1000) behind the newest are pruned, including a previous run's; vote dumps are capped at 10 per 100 slots.

Evidence stays outside `blocks/`, so `LedgerReader`, the explorer and `cruft.sh` (which matches `^[0-9]{12}$` under `blocks/`) are unaffected.

Two notes on limits:
- `PartialDissemination` convicts only under ≤ ⌊f/2⌋ Byzantine validators (the lemma's premise). It is filed as kind `suspect-partial-dissemination`.
- Cross-validator `ConflictingVotes` (`lem:qv:acc-f` case 2) needs two validators' views. v1 dumps all signed votes of a slot not finalized by D+4Δ to `evidence/<slot>-suspect-votes-<self>.rlp`, for offline comparison (deviation D20).

Ledger `proof.rlp` = `rlp(PqProof { version: 1, path: PqPath, metablock, path_votes: Vec<Signed<PathVoteBody>> })`. The path votes are the `strong` fast-path votes, included when `[quantum] ledger_proof = "path-votes"` (default with the stub sig); otherwise they are left empty. There is no transferable certificate by design.

---

## 12. Config schema (`node.toml` for monad-pq-node)

The file is a superset of `monad_mcp_node::config::NodeConfig`. Parse it twice:
1. `toml::from_str::<NodeConfig>` (ignores `[quantum]` and `[rpc]`).
2. `toml::from_str::<QuantumFile { #[serde(default)] quantum: QuantumSection }>`, using the `RpcSection::parse` pattern (`monad-mcp-rpc/src/config.rs:145-149`).

Then call `NodeConfig::validate()`, plus pq checks:
- all stakes equal, with every validator owed ≥ 1 S11 chunk;
- `decode_grace ≤ delta / 2`;
- `retransmit_interval > 0`;
- `certificate_retention ≥ lag_threshold`;
- `warn!` if `withhold_before_deadline < propose_before_deadline − 10`;
- `warn!` if n ≡ 0 (mod 3) (section 4.3 item 4);
- `[proposal] source = "mempool"` when `[rpc]` is present.

```toml
node_id = 0
proposal_key_pair = 0
cadence_key_pair = 0          # unused by pq consensus; kept for NodeConfig/rpc compatibility
genesis_deadline = 1791053335316

[[validators]]
node_id = 0
stake = 1                     # must be equal for all validators (pq check)
chorus_pubkey = 0
address = "84.32.186.173:8002"
# ... one per validator

[network]
port = 8002

[cadence]
delta = 150
slot_interval = 100
slots_per_window = 100
sync_boundary_slots = 80
lag_threshold = 20

[repeater]                    # post-halt Finalized retransmission
interval = 500
certificate_retention = 50

[da]
completed_slot_retention = 50

[proposal]
source = "mempool"
num_proposals = 5
propose_before_deadline = 150
withhold_before_deadline = 140   # L − 10 (section 3.4)

[leader_election]             # strict table: no new keys
rotation_slack = 95

[mempool]

[ledger]
dir = "/home/monad/monad-pq/ledger"

[quantum]
signature = "stub"            # "stub" | (later) "ml-dsa-44"
stub_signature_len = 32       # 2420 emulates ML-DSA-44 sizes; rejected until fragmentation lands
retransmit_interval = 150     # ms, default = cadence.delta
decode_grace = 75             # ms, default = cadence.delta / 2; must be ≤ delta / 2
coda_timeout_deltas = 5
release_chunks = "vote"       # "vote" | "receipt"
early_vote = false            # default; true votes as soon as every lane's chunk arrived
ledger_proof = "path-votes"   # "metablock" | "path-votes"
evidence = true
evidence_retention_slots = 1000

[rpc]                         # read by monad-mcp-rpc only
latency = "latency.toml"
```

Binaries:
- `monad-pq-node`, new.
- `monad-mcp-rpc`, `mcp-tx` and `monad-mcp-explorer`, reused builds.

---

## 13. Deploy layout (`monad-pq-node/deploy/`)

This is a copy of `monad-mcp-node/deploy/`, with the names changed in `lib.sh` and in the units:

| file | change vs monad-mcp-node/deploy |
|---|---|
| `lib.sh` | `unit=monad-pq-node rpc_unit=monad-pq-rpc explorer_unit=monad-pq-explorer cruft_unit=monad-pq-cruft remote_rel=monad-pq remote_root='$HOME/monad-pq'`. Port `${PQ_PORT:-8002}` unchanged; ssh settings unchanged |
| `hosts.txt` | identical 8 hosts (line index = node_id) |
| `build.sh` | `cargo build --release -p monad-pq-node -p monad-mcp-rpc -p monad-mcp-explorer --bin monad-pq-node --bin monad-mcp-rpc --bin monad-mcp-explorer`. The dirty check covers `monad-pq-*` and `monad-mcp-*`. dist names are `monad-pq-node-<sha>`, `monad-mcp-rpc-<sha>`, `monad-mcp-explorer-<sha>` |
| `deploy.sh` | ships to `~/monad-pq/{bin,config,run,logs,ledger/blocks,ledger/evidence}` and units `monad-pq-{node,rpc,explorer,cruft}.service`, `monad-pq-cruft.timer`, `monad-pq.slice` |
| `gen-config.sh` | the `[quantum]` table added; `ledger.dir=/home/monad/monad-pq/ledger`; same defaults as section 8.3 |
| `push-config.sh`, `preflight.sh`, `latency.sh`, `check-latency.py`, `cruft.sh`, `cruft.env`, `run.sh` | paths `~/monad-pq`, unit names; the pgrep pattern becomes `monad-pq/bin/curren[t]` |
| `netctl.sh` | the same subcommands, plus `takeover` (stop `monad-mcp-explorer`, `monad-mcp-rpc`, `monad-mcp-node` user units, wait for 8002 to be free) and `handback` (stop pq, start mcp units). `start` refuses unless 8002 is free |
| `report.sh` | greps `finalized slot=N block=… at=…` (unchanged line) and `entering fallback coda` / `coda decided slot=… view=…` in place of the MVBA strings; adds a `halted` latency column |
| units | `monad-pq-node.service`: `ExecStart=%h/monad-pq/bin/current %h/monad-pq/config/node.toml`, `Environment=RUST_LOG=info,monad_pq_chorus=debug`, `Slice=monad-pq.slice`, `MemoryAccounting=no`, `KillSignal=SIGINT`, `Restart=no`. `monad-pq-rpc.service`: `ExecStart=%h/monad-pq/bin/rpc-current --node-config %h/monad-pq/config/node.toml`. `monad-pq-explorer.service`: `EnvironmentFile=%h/monad-pq/config/explorer.env` with `EXPLORER_ARGS=--ledger-dir /home/monad/monad-pq/ledger` |
| `config/<host>/node.toml`, `explorer.env` (ewr-002, sgp-008) | generated |
| `README.md` | the procedure: `build → preflight → deploy → netctl takeover → netctl start → status/report → netctl handback` |

The pq node keeps these log lines byte-compatible:
- `udp bound` (from the reused `UdpNetwork`, `network/stub.rs:55`);
- `finalized slot=N block=+-+++ at=<ns>` (duplicated `log_finalized`, emitted at `PqEvent::Final`);
- `proposing` (duplicated runtime).

New lines: `halted slot=N at=<ns>`, `entering fallback coda`, `coda decided slot=N view=V`, `misbehavior evidence`.

---

## 14. Test plan

### 14.1 Coda standalone (`monad-pq-chorus/src/coda/sim.rs` + tests)

The harness is a deterministic event queue over `Coda<u8>` instances with per-link delay, loss and partition, a seeded RNG, and Byzantine actors (scripted message injection).

| Test | Expectation |
|---|---|
| unanimous inputs, synchronous | all decide in view 1 at start + 2 hops |
| split inputs for one proposer | no view-1 decision; that proposer decides Empty or one correct input; the others decide their unanimous value (strong unanimity per proposer) |
| faulty leader in view 2 (silent / equivocating Prepare) | decision in the first view with a correct leader, within 5Δ of its first entry |
| f silent validators | termination |
| 10–30 % loss with retransmission | termination; agreement |
| partition healed at T | a laggard catches up via `upon_catch_up`; decides |
| Byzantine echo flood (values never input) | obligation: no correct validator decides an uninput value |
| memory | `own` ≤ 2 views; `rx` holds ≤ 2 views per sender across 50 views; a Byzantine sender spamming views 10³..10⁶ costs ≤ 2 views of its messages |
| catch-up far ahead | a node held back for 20 views catches up to the current view in one `upon_catch_up` |
| count rule | a property test: `view_change_echoes()` equals brute-force enumeration of all sets S (n ≤ 7) |

Property tests (proptest over schedules and inputs, n ∈ {4, 7, 8}): agreement, strong unanimity per proposer, obligation per proposer, decision monotonic.

### 14.2 QuantumChorus slot (scripted, single instance)

Feed `handle_da_event` / `handle_message` / `handle_timer` and assert the outputs:
- early vote;
- the D vote with negatives;
- path-vote cases (fast root, fast ⊥, fallback decode, decode-grace ⊥);
- the acceptance rule;
- clean-set choice;
- adopt cases;
- fast finalize with an invalid encoding → ⊥;
- gadget at weak and strong;
- `Finalize` exactly once and only at halt;
- the ignore-after-halt rule;
- evidence on equivocating headers and votes;
- duplicate timers idempotent.

### 14.3 Slot swarm (`monad-pq-chorus/tests/`, using `monad-mcp-chorus-sim`)

`CadenceSwarmBuilder::add_generic_node` runs a `ScriptedDa<R>` runtime wrapper. It delegates to `CadenceRuntime<QuantumChorus, Conductor>` and injects `ChorusDAEvent`s from a per-test table (per (slot, j, node): fulfilled-at, decoded-at). A `Faulty<S>` `SlotConsensus` wrapper rewrites or drops outputs for Byzantine nodes.

| Scenario | Assertion |
|---|---|
| fast path, n=4 and 8, latency h | finalize at seal + 3h with early vote (new code; upstream Chorus has none); halt at +1h; windows roll over (`cadence_conductor` style) |
| no early vote (`early_vote=false`) | finalize at D + 2h |
| split proposer (chunks to between weak and 2f nodes) | all adopt the same metablock at D+2Δ; Coda view 1 decides at about D+2Δ+2h; the ledger path is Fallback |
| silent first leaders (views 2..f+1 faulty) with diverging inputs | decision in a later view; latency ≤ (views)·(5Δ + 2h) |
| 10 % loss | all finalize the same metablocks; progress continues |
| partition of f nodes for 1 s, then heal | laggards finalize via the gadget or `CapAdvance`; no divergence |
| equivocating proposer (two roots) | `EquivocatingHeaders` evidence; agreement; that lane is ⊥ or one root everywhere |
| equivocating proposer (r1 to some validators, r2 to others, ≥ weak positive each) + one correct node partitioned over [D, D+Δ] (B1) | the partitioned node resolves both roots via `PullRoot`, obtains clean sets, inputs Coda; all correct finalize and halt. Run with real `monad-pq-da` (14.4 harness) and with ScriptedDa |
| Coda slot followed by ≥ 10 slots (M2) | slots r+1..r+10 all finalize on the fast path (lanes blocked by the chaining gate are withheld → fast ⊥); no Coda cascade |
| vacant indices (M4), n=1 and K=5 | finalizes every slot; votes with a root at a vacant j are rejected |
| equivocating voter | `ConflictingVotes` evidence at the receivers that see both |
| halting | every correct node emits `Finalize` once; post-halt messages are ignored; the Repeater-only Finalized lets a node that drops all pre-halt traffic still finalize |

Properties checked on every swarm run:
- agreement: equal `PqEvent::Final` metablocks per slot across nodes;
- proposal inclusion: in timely slots every correct proposer's lane is a root;
- Coda obligation: every root decided via Coda was adopted by some correct node;
- strong unanimity: unanimous adoption → no Empty.

### 14.4 Node (`monad-pq-node/tests/`)

- `in_process.rs`: a single node; txs via `Packet::Tx`; ledger blocks readable by `LedgerReader`; `BlockMeta::check()` passes.
- `scenarios/` (dup of `monad-mcp-node/tests/scenarios/harness.rs`, real DA over `monad-sim-swarm`):
  - `loss` (8 nodes, 2 never start, 10 % loss, 300 slots; no gaps);
  - `mute`;
  - `partition`;
  - `cap_jump` (2 of 8 cut off for 4 s, past `lag_threshold`; they rejoin by a cap jump);
  - `wan` (mcp_test timing over the mcp_test RTT/2 matrix: ≥ 95 % fast path; 7 of 8 live with 2 % loss: no gaps);
  - `tamper` (equivocating proposer via a runtime wrapper);
  - `equivocation_partition` (B1 scenario with real DA).
- `loopback.rs`: 4 `run_node`s over loopback UDP on ephemeral ports for a few seconds; their ledgers agree.
- `stub_signature_len = 2420` (or a `num_proposals` whose slot messages exceed one frame) is rejected by config validation until fragmentation lands.

### 14.5 Local swarm and e2e

- `monad-pq-node/swarm.sh [nodes] [secs]`: `SWARM_DIR=/tmp/pq-swarm`, `BASE_PORT=9300`. It checks finalized-slot agreement (lane pids) across nodes and the all-committed ratio. `examples/swarm.rs` (one process) counts up from 9400, and `config.example.toml` uses 9290.
- `monad-pq-node/e2e.sh [--swarm N] [--seconds S]`: the pq node + `monad-mcp-rpc` + `monad-mcp-explorer` on free ports, under tx load through the explorer, the rpc and raw udp; it reports e2e latency and the swarm summary. No Playwright checks.

### 14.6 e2e latency

`monad-pq-node/examples/latency.rs --ledger-dir D [--since slot]` reads blocks with `LedgerReader`. For every tx with `sent_at_ns > 0` it reports p50/p90/p99 of `finalized_at_ns − sent_at_ns`, split by path.

Drive load with `mcp-tx send --rpc http://127.0.0.1:8080 --count 500 --interval 20` (sets `sent_at_ns`), or direct with `mcp-tx send --node <addr> --sender-id <id>`.

Acceptance:
- local swarm: p99 < 400 ms;
- mcp_test: fast-path p99 < 600 ms; slots on the Coda view-1 path p99 < 1 s.

---

## 15. Work breakdown (independent agents, in order)

Each work package (WP) ends with `cargo check -p <crate>`, `cargo test -p <crate>` and nightly rustfmt. Implementers follow the repo rules: edit with Read/Edit/Write (no sed/python rewrites), and keep comments to 1–2 lines.

1. **WP1: `monad-pq-chorus` skeleton: types, messages, signatures, quorum, `PqDACommand`.**
   - Files: `sig.rs`, `types.rs`, `message.rs`, `quorum.rs`, `config.rs`, `da.rs`.
   - Land `da.rs` (the `PqDACommand` enum of section 4.5) first, so WP-DA can start.
   - Accept:
     - RLP round-trip for every message;
     - `assert_signing_prefix` on both domains;
     - a sign/verify test that `StubPq` rejects a wrong signer, slot or body;
     - a size test reproducing the 6.3 table at K=5 and asserting fit ≤ 1463 B with the stub;
     - `Quorum` table for n = 1, 4, 7, 8;
     - the equal-stake check.
1b. **WP-DA: `monad-pq-da`** (depends only on WP1's `da.rs`; parallel with WP1–WP3).
   - Copy `monad-mcp-da` verbatim, switch commands to `PqDACommand`, and implement section 4.5 (vouched tracking, per-pid `Recovery` + `PullRoot` pulls, the existing tests kept).
   - Accept:
     - all copied tests pass unchanged except for the type rename;
     - new tests `pull_root_recovers_a_rival_root`, `third_root_tracked_when_vouched`, `pull_retries_until_decoded`;
     - a byte-identical wire format, proven by a round-trip of upstream packets through the pq decoder.
2. **WP2: Coda standalone** (depends on WP1 types only).
   - Files: `coda/{mod,view,count,sim}.rs`.
   - Accept: every 14.1 test and property; the count rule matches brute force; memory-bound test.
3. **WP3: QuantumChorus slot** (WP1 + WP2).
   - Files: `slot/*`, `finalization.rs`.
   - Accept: every 14.2 scripted test, including the vacant-index tests (section 10.1); `Finalize` only at halt; no panic on an unknown proposer schedule.
4. **WP4: slot swarm tests** (WP3; WP-DA for the real-DA variant of the B1 scenario, which may land in WP5's harness).
   - Files: `tests/{fast,early_vote,split_proposer,faulty_leaders,loss,partition,equivocation,equivocation_partition,coda_no_cascade,vacant,halting}.rs`, `tests/common/{scripted_da,faulty}.rs`.
   - Accept: every 14.3 scenario and property, run with `MonadConductor<NopAcs<SlotDeadline>>` through ≥ 3 windows.
5. **WP5: `monad-pq-node`** (WP3 + WP-DA).
   - Files: config (two-pass parse), `PqCadence`, `PqDA`, `component/spawn.rs` + clock, runtime/node/async_node, finalization collector (keyed on `PqEvent::Final`, joined with `DAOutput::Resolved`), ledger (`pq_new_block`, `log_finalized`), evidence sink, Repeater wiring (Finalization only), Proposing chaining gate fed `max(local Final prefix, conductor cap)`, `wire.rs` (Unfragmented), `main.rs`, `examples/swarm.rs`.
   - Accept:
     - `in_process` and `scenarios` tests (14.4);
     - `monad-mcp-rpc` parses the pq `config.example.toml` (a test in the pq crate calling `monad_mcp_node::config::NodeConfig` parse plus the `RpcSection`-style parse);
     - a single node finalizes at about seal time.
6. **WP6: rpc/explorer integration and latency tooling** (WP5).
   - Files: `swarm.sh`, `e2e/run.sh`, `examples/latency.rs`.
   - Accept:
     - `e2e/run.sh --swarm 5` passes the explorer checks;
     - `mcp-tx status` reports committed;
     - local p99 < 400 ms (14.6).
7. **WP7: deploy** (WP5).
   - Files: everything in section 13.
   - Accept:
     - `preflight.sh` passes after `netctl takeover`;
     - `netctl start` brings 8 nodes to `finalized` lines with cross-host agreement in `report.sh`;
     - mcp_test e2e p99 meets 14.6;
     - `netctl handback` restores the mcp units.
8. **WP8 (later): fragmentation + ML-DSA-44.** `wire.rs` `Fragmenting`; a `PqSigner` impl; a `monad-pq-da` env with variable-length proposer signatures.
   - Accept: `stub_signature_len = 2420` scenario tests finalize.

Dependencies:
- WP1 `da.rs` → WP-DA.
- WP1 → WP2 → WP3 → WP4.
- WP3 + WP-DA → WP5 → {WP6, WP7}.

WP-DA runs in parallel with WP1–WP3. WP2 can start once `types.rs` is fixed.

---

## 16. Deviations from the paper

| # | Deviation | Reason |
|---|---|---|
| D1 | Votes carry no chunks. "f+1 positive votes with chunks" is replaced by "f+1 positive votes and DA resolved the root". The path vote waits at most `decode_grace` for decoding before ⟨fallback, ⊥⟩ | User decision (DA forwarding, paper §7 deployment note). DA has no per-sender chunk events (section 4.3). Agreement and inclusion are unaffected (section 4.3) |
| D2 | Vote acceptance (`ln:qv:allornothing`) checks signature + completeness only | consequence of D1 |
| D3 | A positive entry also results from `Decoded(pid)` before D, provided `HeaderSeen(pid)` has arrived (`fetch_proposal` needs the header) | the `ProposalAvailability` rule (`availability.rs:51-58,84-89`); "holds its symbol under r" is what the entry asserts |
| D4 | Slot completion (`Finalize`) at halt; ledger finalization via `CommitOptimistic(PqEvent::Final)` | the unchanged runtime closes on `Finalize` (`runtime.rs:194-203`); matches §6 |
| D5 | No pre-participation Finalized relay (§6) | the runtime drops messages for unopened slots (`runtime.rs:145-147`). Fixed `NopAcs` schedule + Repeater retention + `CapAdvance` jump cover it |
| D6 | Broadcasts are abandoned at halt, except Finalized (kept until cap − retention, and sent at least once) | the full argument is in section 7: after the first correct halt, finalization and halting at every correct validator use only Finalized messages. Requires `certificate_retention ≥ lag_threshold` (R7) |
| D7 | Coda keeps received messages only for each sender's two highest views (≥ view−1); messages before Coda start are buffered | bounds memory per sender against Byzantine high-view spam without limiting catch-up; buffering is equivalent to redelivery |
| D8 | Equal stake required; `strong = ⌈(n+f+1)/2⌉` for n ≠ 3f+1 | paper model is n = 3f+1 by count; mcp_test has n = 8 |
| D9 | `leader(V) = validators[(slot+V) mod n]` | the paper only says round-robin |
| D10 | Persistent memory is in memory only; no crash recovery | user decision |
| D11 | Stub signatures; the proposer header signature is DA's 65-byte stub | user decision; DA layout fixed (`monad-mcp-da/src/spec.rs:26`) |
| D12 | Authenticated channels assumed; the transport trusts the claimed sender id (`network/stub.rs`) | Coda/Finalized rely on authentication; `monad-wireauth` integration later |
| D13 | Conductor = `MonadConductor<NopAcs>` fixed schedule; unsigned `CapAdvance` trusted | user decision (signature-free ACS deferred) |
| D14 | Ledger path: Gadget is recorded as Fallback; the exact path is in `proof.rlp` | ledger enum is two-valued (`ledger/block.rs:30-33`); explorer unchanged |
| D15 | The proposal chaining gate follows local finalization, not conductor completion | halt-based cap would cost proposals (section 3.4) |
| D16 | DA is duplicated (`monad-pq-da`). It additionally tracks and pulls every root with ≥ weak positive voters (`PullRoot`), and consensus retries `PullHeader` | upstream tracks ≤ 2 roots and pulls only the first-seen one (`proposer_rc/mod.rs:177-187,254-269`), which can stall Coda input (section 4.3 item 2) |
| D22 | Vacant proposer indices are fixed ⊥ and excluded from waits, pulls and evidence | rotation vacancies (`proposers.rs:335-375`); single node with K=5 |
| D17 | In-slot timer dedupe by once-flags | `timers.rs:48-51` does not dedupe |
| D18 | `ReleaseChunks` at vote time (default) | DA image of "attach C to the Vote" |
| D19 | Clean set: the deterministic first-completed set | the paper allows any first set |
| D20 | Cross-validator conflicting-vote evidence is offline (vote dumps) | no evidence-gossip protocol in the paper |
| D21 | Proposers seal at D − L (planner lead), not at D − Δ | existing planner (`proposing.rs`); paper §7 evaluation does the same |

---

## 17. Risks and open questions

Risks:
- **R1.** The Coda path at the paper's bound (D+4Δ) gives e2e ≈ 970 ms on mcp_test. Fast and typical view-1 cases are well under 1 s. Measure before tuning Δ.
- **R2.** Frame size: the Vote2 limit is K ≤ 19 unfragmented, and ML-DSA votes need fragmentation (WP8).
- **R3.** Unsigned `CapAdvance` and an unauthenticated transport: any host can forge a sender id. This is inherited and accepted for testing.
- **R4.** An equivocating proposer adds DA pulls: up to ⌊n/weak⌋ roots per index, each pulled every 2Δ until resolved. The cost is bounded by the existing serving quota (`serving_tracker.rs`), and the equivocation is provable (`EquivocatingHeaders`). The gadget is *not* a mitigation for a Coda-input stall, because no correct validator may halt; `PullRoot` is the mitigation.
- **R5.** The four hosts that run monad-bft and bind 8002 (README) constrain pq exactly as they constrain mcp.
- **R6.** `decode_grace` shifts the worst-case timely bounds by g: adoption D+2Δ+g, Coda view 1 D+4Δ+g, e2e ≈ 1045 ms on mcp_test at g = 75. It applies only when ≥ weak positive voters withhold chunks. Measure; lower g if Byzantine-free runs never hit it.
- **R7.** After halt, Finalized lives only `certificate_retention` slots past the cap. A node lagging by more than that, but by less than `lag_threshold`, would depend on the paper's never-abandon rule. The config check enforces `certificate_retention ≥ lag_threshold`.
- **R8.** n ≡ 0 (mod 3) weakens the decode guarantee from weak holders (section 4.3 item 4). Config load warns.

Open questions (human decision):
- **O1.** Should `release_chunks = "receipt"` (paper §7 evaluation) be the mcp_test default? It lowers decode latency but drops the deadline-gated forwarding Chorus uses.
- **O2.** How should Gadget-path finalizations appear in explorer stats: as Fallback (current choice) or as Fast? A third ledger variant needs explorer and rpc code changes.
- **O3.** Should `PathVote` evidence (`ledger_proof = "path-votes"`) stay on once ML-DSA sizes apply (about 16 KB per block)?

## 18. Rollback

Everything is additive: new crates and a new deploy dir. On mcp_test, `netctl.sh handback` stops the `monad-pq-*` units and restarts `monad-mcp-*` with their untouched configs. Local rollback is deleting `monad-pq-*` and reverting `Cargo.lock`.
