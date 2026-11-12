# Baseline native transaction development profile

Snapshot: 2026-11-12. Branch: `feat/monad-type-40`.

## Matched revisions

- Consensus/RPC base: `e844f6086acac664f4bd2a8ffcc99f3b5ce4b9af`.
- Execution base: `f1a93dd450ec99d52b70b677cf8fb356a333277e`, checked out on its own `feat/monad-type-40` branch under `monad-execution/`.
- Execution implementation: `90cbb7139a7a7366abbd8f000d266339486fccf8`, recorded by this branch's `monad-execution` submodule pin.
- Viem vector source: `40e3a58c1c8c9c0326558eee8991992173059ea4`.
- Protocol source: the supplied universal transaction draft's main injection design, SHA-256 `79ce4f8c4910a71e3a2932c1083312577e2ef99774a98ddba081bf20face1cb9`.

The execution implementation was committed first, and the BFT gitlink records that exact revision. Build this branch with its pinned execution submodule. Do not mix these BFT changes with an unmodified execution checkout or public prebuilt images.

## Scope and commitments

This profile accepts ordinary sender-paid, single-call or contract-creation transactions on devnet chain **20143** after Prague. It supports access lists. It rejects authorization-list feature `0x05`, sponsorship, batching, unknown features, nonempty auxiliary data and nonempty receipt extensions.

Confirmed milestone decisions (2026-11-12): reject feature `0x05` even when its authorization list is empty, and retain the [pre-NEXT devnet configuration](#required-devnet-fork-setting). Native EIP-7702 execution and reconciliation of NEXT's gas rules remain outside this milestone.

```text
canonical = 0x40 || RLP([chainId, signedFeatures])
signDigest = keccak256(0x40 || RLP([chainId, features with 0x06 = []]))
txHash = keccak256(canonical)
submission = 0x40 || RLP([chainId, signedFeatures, []])
receipt = 0x40 || RLP([status, cumulativeGasUsed, logsBloom, logs, []])
```

Required feature keys: `0x01` nonce, `0x02` gas, `0x03` CALL, `0x06` signature and `0x07` inclusion gas bid. Optional `0x04` is an access list. CALL contains exactly keys 1 destination, 2 value and 3 calldata. All maps have unique ascending single-byte keys. All RLP containers are bounded before nested decoding; integers are minimal unsigned encodings. Empty destination means contract creation.

Native chain IDs, values and fee caps are uint256; nonce/gas are uint64. Admission requires the actual development chain ID without truncation. Received signatures are never normalized: recovery and validity reject high-s/invalid signatures. The maximum encoded uint64 nonce is invalid under EIP-2681, and gas × fee overflow is rejected before pool arithmetic.

Omitted and explicitly empty access lists execute equivalently but have different commitments. Presence survives Rust decoding, C++ decoding, storage and execution-event reconstruction. RPC returns the canonical transaction hash, not the submission hash. Internal ledger, gossip and trie objects use canonical bytes.

Block and IPC items add RLP framing around that identity: typed envelopes are byte strings containing the raw canonical bytes, while legacy Ethereum envelopes remain lists. Transaction and receipt readers bound the item before decoding and require exact consumption. The wrapper does not enter the transaction hash or signing digest.

Database adapters explicitly distinguish two existing stored representations: C++ TrieDB stores raw EIP-2718 bytes inside the record's byte-string field, whereas Rust archives store network RLP bytes there. `TxEnvelopeWithSender` and `ReceiptWithLogIndex` bound both the outer record and that field, decode the appropriate representation exactly, then consume only the required metadata. Archive writers retain their existing representation; no storage migration is introduced. This compatibility is confined to stored records.

## Implementation boundaries

The shared [`monad-transaction`](../monad-execution/rust/crates/monad-transaction/) crate owns native Rust transaction/receipt codecs and full-width RPC representations. Existing Ethereum variants continue to use Alloy signed payloads and encoders. The wrapper intentionally does not implement Alloy's transaction trait, whose fee methods narrow to uint128. Both workspaces use the shared source through a path dependency; no Alloy fork is required.

Native envelopes now flow through RPC submission, sender recovery, pool ingress and forwarding, proposal generation, block validation, block persistence, state readers, archive adapters and RPC buffers. Fee sorting and fee-history rewards retain uint256 values. Transaction-generator readers accept mixed native/Ethereum blocks and receipts; its raw-send helper applies native submission framing. Execution dispatch uses the existing single-call EVM, sender-paid fees and normal nonce/account rules.

`eth_call` and `eth_estimateGas` preserve explicit `type: "0x40"` and use canonical bytes through the execution bridge. Transaction, block and receipt responses retain standard RPC fields. Both raw-submission methods use the three-field decoder. Node-managed signing and explicit native `eth_fillTransaction` are outside this milestone.

The draft's separate execution/consensus input objects and new header input roots are deferred: this profile produces no such inputs. No voting/finality algorithm or EVM opcode changes are included.

## Required devnet fork setting

Execution's default devnet is `MONAD_NEXT`, while the matched BFT source models Prague-era transaction gas. NEXT additionally charges access-list bytes under EIP-7981. For this baseline set:

```sh
export MONAD_DEVNET_FORK_TS=18446744073709551615
```

Apply that environment to **execution and RPC** (which hosts an execution engine), and genesis initialization. It keeps execution on `MONAD_NINE`, whose relevant transaction gas rules match the BFT baseline. Supporting NEXT requires separately reconciling the fork rules; do not use the default NEXT configuration for this milestone.

The optional [Compose overlay](../docker/single-node/nets/compose.type40.yaml) supplies the setting. From `docker/single-node/`, the existing source-build launcher can use it with:

```sh
COMPOSE_FILE=compose.yaml:compose.type40.yaml ./nets/run.sh --toolchain gcc-avx2
```

The launcher requires the project's Docker/toolchain setup and builds the local checkouts. Use a fresh data directory. This launch command has not been verified as a running network in this workspace.

The execution event header gains a uint8 presence flag in existing padding and has a new schema fingerprint. Build producers and readers together; use a fresh event ring. Production schema validation remains strict. The Rust `test-utils` feature reframes only historical Ethereum test snapshots, allowing their existing payloads to remain in regression coverage without accepting old live rings.

## Verification

Run from the repository root unless noted. Linux checks used x86-64 Debian, Rust 1.97.1 and Clang 19. The host was Apple Silicon; Linux checks ran through an amd64 container.

```sh
cargo test -p monad-eth-block-policy -p monad-eth-block-validator -p monad-eth-txpool
cargo test -p monad-eth-types -p monad-eth-txpool-types -p monad-eth-txpool-ipc
cargo test --manifest-path monad-execution/rust/Cargo.toml -p monad-transaction
CC=clang-19 CXX=clang++-19 LIBCLANG_PATH=/usr/lib/llvm-19/lib \
  cargo test --manifest-path monad-execution/rust/Cargo.toml \
  -p monad-exec-events --features alloy
monad-execution/scripts/test-monad-type40.sh
CC=clang-19 CXX=clang++-19 LIBCLANG_PATH=/usr/lib/llvm-19/lib \
  cargo check -p monad-node -p monad-rpc -p monad-archive \
  -p monad-eth-txpool-executor -p monad-updaters -p monad-triedb-utils --all-targets
# With the execution/TrieDB libraries built and linked:
cargo test -p monad-rpc --lib native_logs_tests
```

The standalone C++ script compiles the actual transaction/receipt codecs, recovery, gas functions and static validators. It uses Linux x86-64 AVX2 and initialized source submodules, plus Clang 19, Boost, GoogleTest, libsecp256k1 and TBB headers. It does not build the complete EVM or a node process.

Recorded results:

| Check | Result |
| --- | --- |
| Consensus policy, block validator and pool suites | 170 tests passed |
| Block/database adapters and IPC types | 10 tests passed, including 7 new framing/storage regressions |
| Shared Rust codec/RPC/trie-root regressions | 14 tests passed, including 4 new framing/stream regressions |
| C++ native plus existing Ethereum transaction/receipt RLP and validity tests | 29 tests passed, including the new transaction-list framing regression |
| RPC receipt/log regression source harness | 3 tests passed; all 3 fail with the previous helper |
| Execution-event tests | 4 passed; 2 existing tests ignored; documentation test passed |
| Linux `cargo check --all-targets` for node, RPC, archive, pool executor, updaters and TrieDB utilities | Passed, including RPC test and benchmark compilation |
| Linux `cargo check --all-targets` for test utilities, mock swarm, consensus state, secp, TrieDB utilities and system calls | Passed, including transaction generator examples |
| C++ event writer syntax check with execution ctypes definitions | Passed |
| Compose overlay configuration | Confirmed setting reaches genesis, execution and RPC |

Coverage includes:

- SDK golden vectors in independent Rust/C++ codecs: exact canonical and submission bytes, signing preimages/digests, sender recovery and canonical hashes.
- RLP truncation/overflow, nonminimal quantities, missing/duplicate/out-of-order/unknown keys, unsupported feature combinations, high-s and invalid scalars.
- Five-field native receipts and empty auxiliary/receipt-extension boundaries.
- Exact transaction/receipt network framing, actual block-body and IPC/event decoding, and mixed Ethereum/native streams.
- Raw TrieDB and existing archive record decoding, unchanged archive writes, metadata boundaries and rejection of extra/truncated fields.
- Native/empty receipt log extraction, address/topic filters and mixed-block log metadata/ordering without panics.
- Full-width RPC transaction/fee-history round trips and unchanged Ethereum effective-fee behavior.
- Submission → pool → proposal → block-validation regression for owned/forwarded transactions, including canonical block-body round trip, wrong-chain and fee-overflow rejection.
- Native execution-event reconstruction, presence fidelity and legacy Ethereum snapshot replay.
- C++ activation, dynamic pricing, fee/nonce bounds and existing intrinsic/calldata/access-list gas.

A complete linked `monad-node`/`monad-rpc`/execution build, full RPC runtime tests and a live Rabby submit/include/execute/receipt transaction are still required. A Rust `cargo check` does not link the C++ execution/TrieDB libraries or prove live interoperability. The three checked-in RPC log tests were run in an isolated harness containing the current helper bodies and required error types; the full crate's test targets were also checked. This verifies extraction/filtering and mixed-block scans, not the MongoDB-backed indexed query or a running RPC process. Existing ignored event snapshot tests remain ignored.

## Review fixes (2026-11-12)

Both findings are fixed without changing canonical encoding, signatures or transaction identity:

1. **P1 — native log queries:** `receipt_envelope_to_logs_iter` uses the shared envelope's exhaustive `into_receipt()` accessor. It handles native and Ethereum receipts, including empty logs, through the same filtering path. Three regressions fail with the previous helper and pass with the fix.
2. **P2 — block/IPC framing:** both shared envelopes override the permissive Alloy network decoder with bounded, exact RLP decoding. Actual block-body, IPC admission/event and database adapter regressions cover the boundary. Raw canonical parsing remains separate from network parsing. Existing archive writes are preserved, with explicit support for both archive and TrieDB record payloads.

Independent Rust/C++ comparisons of 14,112 raw transaction/receipt cases found no differences in acceptance, re-encoding, hashes, signing digests or recovery. After the framing fix, both implementations reject the unwrapped, undersized and split-payload transaction-list probes; the correctly wrapped list is accepted and re-encodes identically.
