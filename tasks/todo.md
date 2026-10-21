# Quantum Cadence client — plan

Source of truth: `~/tmp/mcp-paper/pq-cadence/quantum-cadence.tex` (v0.9; App. B and §6 Conductor marked not-ready).
Alg. 1 = Quantum Chorus (sections/4, algorithms/quantum-chorus.tex), Alg. 2 = Coda (sections/5, algorithms/coda.tex).

## Constraints
- No edits to any `monad-mcp-*` crate, script or config. Reuse via crate deps; duplicate where an API must change.
- Own binary, config, deploy scripts, systemd units, remote dir, port.
- Targets: local single node, local swarm, mcp_test (8 hosts); tx via explorer or direct; e2e tx latency < 1 s.

## Architecture (reuse vs duplicate)
Reused unchanged (crate deps on `monad-mcp-chorus` stub variant):
`CadenceRuntime`, `SlotManager`, `Timers`, `MonadConductor<NopAcs>`, `ProposalPlanner`,
`ProposerSchedule`, `ledger::*` (block/tx format), `monad-mcp-explorer` binary, `monad-sim(-swarm)`.
`SlotConsensus` is a trait, so Quantum Chorus plugs into the existing runtime; its PQ keys live in
its own `Context` (stub `NodeId`/`ValidatorData` used only for identity + stake).

User decisions (2026-10-21): stub signatures now behind a forward-compatible PQ signature interface;
DA forwarding of chunks (not chunks-in-votes); mcp_test = replace monad-mcp-node while testing (stop it,
run PQ on 8002); defer signature-free ACS, crash persistence, tx signatures.

New crates:
- `monad-pq-chorus` — `pq_sig` module: `PqSigner`/`PqVerifier` traits sized for non-recoverable,
  non-aggregatable signatures (verify by NodeId pubkey lookup, variable-length bytes), stub impl now,
  ML-DSA-44 drop-in later. `QuantumChorus: SlotConsensus` (Alg. 1: Vote / PathVote / Finalized,
  clean sets, adoption, halting gadget, early vote, D+Δ / D+2Δ guards) + `Coda` (Alg. 2: leaderless
  view 1, prepare/vote-1/vote-2, 5Δ timeout, per-proposer echoes, Bracha amplification, catch-up,
  two-view memory window) + accountability evidence store (equivocating headers/votes).
- DA: reuse `monad-mcp-da` unchanged if `ChorusDAEvent`/`ChorusDACommand` give what Alg. 1 needs
  (own valid chunk before D, decoded + re-encoded root check, holders for retrieval). Duplicate as
  `monad-pq-da` only if the design step proves a gap.
- `monad-pq-node` — duplicate of `monad-mcp-node` wiring: Quantum Chorus component, repeater =
  forever-alive retransmit until conductor closes the slot, mempool, ledger writer (explorer-compatible).
  Transport: frame-size check sized for PQ (fragmentation hook; needed once real PQ sigs land).
- RPC: reuse `monad-mcp-rpc` unchanged if `monad-pq-node`'s config stays parseable as its `NodeConfig`;
  otherwise thin duplicate `monad-pq-rpc`.
- `monad-pq-chorus-sim` tests — deterministic swarm tests (fast path, split proposer → Coda view 1,
  faulty leaders → later views, loss, partition, equivocation evidence, halting).

## Paper → implementation decisions (flag for review)
1. Vote chunks: Alg. 1 attaches the voter's chunk to each positive entry. Following §7's deployment
   note, chunks travel via DA forwarding instead; "f+1 votes positive for r with chunks" ⇒ count
   f+1 positive votes AND DA holds f+1 valid chunks under r (decode + re-encode check). Keeps votes small.
2. Slot completion: runtime closes a slot on `SlotOutput::Finalize`. Paper (§6) completes at *halt*
   and finalization must keep running Coda/gadget until then. Emit ledger finalization via
   `CommitOptimistic`-style observer at finalize; emit `Finalize` to the runtime at halt. Verify this
   fits `CadenceRuntime` without changes; otherwise duplicate `runtime.rs` into `monad-pq-chorus`.
3. Conductor: keep `MonadConductor<NopAcs>` (fixed schedule, same as current MCP). Signature-free ACS
   (§6, BKR + binary Coda) deferred — paper itself leaves it unspecified.
4. Persistent memory / crash recovery: in-memory only in v1 (current MCP has none either); keep state
   layout matching the paper's persistent variables so a WAL can be added.
5. Δ tuned per environment: local Δ=50 ms, mcp_test Δ=150 ms (RTT up to 240 ms; fallback via Coda view 1
   ≈ D+2Δ+2 hops).

## Tasks
- [x] 0. Confirm plan + open questions with user
- [x] 1. Design doc (written; adversarial review done: B1 → duplicate DA as monad-pq-da; revised). Worktree: /home/mchen/github/pq-cadence-wt (michael/pq-cadence)
- [x] 1b. monad-pq-da (WP-DA) — 118 tests pass, uncommitted (vouched-root cap = const 3) `docs/pq-cadence.md`: message formats, sizes, state machines, mapping table paper line → code, DA/RPC reuse verdicts
- [x] 2. `pq_sig` interface + stub impl + unit tests
- [x] 3. `monad-pq-chorus`: Coda (standalone, sim-tested against Alg. 2 properties)
- [x] 4. `monad-pq-chorus`: Quantum Chorus slot consensus + halting gadget + accountability
- [x] 5. Sim swarm tests (101 tests pass; verified independently) — faithfulness review: no critical; I1/I2/M1-M3 fixed
- [x] 6. DA integration → monad-pq-da (see 1b)
  - Committed 1caaa6c2 (main checkout, jj, bookmark mcp-pq-dev) after scoped precommit-pq gate (224 tests). Work moved to main checkout per user.
- [x] 7. `monad-pq-node` — committed 22b40eb1 (251 tests pass, scoped gate); worktree removed
- [x] 8. RPC + explorer reused unchanged (rpc parses pq node.toml; explorer tails pq ledger)
- [x] 9. Local single node: tx e2e p99 explorer 481 / rpc 497 / udp 490 ms (tail = 5-slot lane vacancy per 100 slots)
- [x] 10. Local swarm: n=4 p99 ≤172 ms, n=8 p99 ≤159 ms; full agreement, 96–99% fast path
- [ ] 11. Adversarial review pass: chorus/da done (fixed); node review running
- [x] 12. mcp_test deploy scripts (`monad-pq-node/deploy/`, units `monad-pq-*`, `~/monad-pq/`, UDP 8002, rpc 8090, explorer 8091) — preflight clean, uncommitted
- [ ] 13. mcp_test deploy — user approved takeover 2026-10-21 ("go. no need to restore MCP"). Commits b87123b8 (fixes) + c883a067 (deploy); built c883a067; deployed; takeover done; started, genesis 2026-10-21T21:12:33Z.
  - [x] Result: 8/8 live, 100% fast path (2168 slots), finalize D+56 p50 / D+58 p99, halt p99 D+109; agreement 0 conflicts, 0 gaps, 0 warn/err/evidence. Tx probe 500/500 via rpc: p50 221 / p90 261 / p99 410 / max 495 ms. Explorer tx (ewr-002): 258 ms. MCP left stopped per user.
  - Explorer moved to 127.0.0.1:8081 (sgp-008 nginx proxy target, same as mcp) — 213b54e1
- [x] 13b. Single-node compare (same load/params): PQ p50 ~50 / p99 ~470 ms vs MCP p50 ~150 / p99 ~600 ms. Tail = shared proposer-schedule 5-slot rotation vacancy (n=1 can't cover). local-config.sh rotation_slack 995 for n=1 → PQ p99 100 ms (f3ed6c8f). Full fix needs monad-mcp-chorus schedule change (not done).
- [ ] 14. `bash -ic precommit`, commits (no Claude co-author trailer)

## Review
- Commits (jj, bookmark mcp-pq-dev): 1caaa6c2 chorus+da, 22b40eb1 node, b87123b8 node fixes, c883a067 deploy, 213b54e1 explorer port, f3ed6c8f single-node slack, 5dd3fa5d live genesis.
- No monad-mcp-* file modified; rpc + explorer binaries reused unchanged.
- Reviews: design critique (2 blockers fixed in design), chorus/da faithfulness review (no critical; fixed), node review (no critical; fixed).
- Deviations from paper: listed in docs/pq-cadence.md (chunks via DA instead of in votes; Decoded counts as holding chunk; Finalize at halt; stub signatures; ACS/persistence deferred).
- mcp_test fault test (vin-002 stopped 21:23:07–21:25:18 UTC): 84% fast (p50 D+149), 16% Coda view 1 (D+449), ltu-010 all via gadget, all halted; after rejoin 100% fast, lag 0; 2881 slots, 0 conflicts.
- e46146d5 (early_vote default off, rpc resend 500 ms) live-upgraded on mcp_test: normal tx p99 373 ms (finalize D+121 p50 / D+224 p99); lax-001 down: tx p99 363 ms. vin-002 down: tx p99 1157 ms (rpc routes to dead nearest proposer) — user: leave rpc as is, test with other nodes down.
- lax-001 down 21:40:18–21:42:09 UTC with 2000-tx probe in same window: p50 405 / p99 485 / max 1261 ms, 2000/2000 landed.
- Submitted from this machine (ssh tunnels; local clock NTP-synced): rpc ewr-002 500/500 p50 350 / p99 461 / max 612 ms; explorer sgp-008 300/300 p50 684 / p99 774 / max 780 ms.
- 788b2491: opt-in ML-DSA-44 + fragmentation (stub default). Local: 8-node swarm 100% fast, finalize p99 +0.8 ms vs stub; 4-node e2e tx p99 ≈ stub (≈230–250 ms). Not deployed. Gaps: DA proposer-header sig still 65-byte stub; no real-key distribution in deploy.
- Pre-D lane decode on PQ vs MCP explained: sgp-008 ran old mcp build 1ea23eeba (decode at > k chunks); upstream ae636e20 changed to ≥ k, which pq-da inherits. Not a PQ bug.
- 2026-10-21 user: "defer everything" — ML-DSA on mcp_test, PQ proposer-header signature, signature-free ACS, crash persistence all deferred.
- Open (superseded): ML-DSA + fragmentation (WP8); single-node rotation vacancy needs shared schedule change.
