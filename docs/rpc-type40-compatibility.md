# Ethereum RPC compatibility review

Review date: 2026-11-12; fixes: 2026-11-13. Compared [PR #3352](https://github.com/category-labs/monad-bft/pull/3352) at `d1861e6c3fbf5bbf2a57d65724fcc04958d4e831` with master `e844f6086acac664f4bd2a8ffcc99f3b5ce4b9af`, using execution `75800de50c3983f536afa0ef1ee2966abfe2368c` from [PR #2840](https://github.com/category-labs/monad/pull/2840). Both BFT revisions use Alloy 2.0.4. Both findings are corrected on this branch. The execution fix is committed as `d2fadff7c960e655ff7d3e76ea244f4dc8346aa4`, which this branch now pins. The committed implementation and test files match the paired source snapshot validated on category-box.

## Finding 1: wrong-chain submission error code

**P2 — fixed: restore the existing error code for Ethereum chain mismatches.** The native implementation replaced `JsonRpcError::invalid_chain_id` with an inline `InvalidParams` error in [`validate_and_decode_tx`](../monad-rpc/src/handlers/eth/txn.rs). A valid Ethereum transaction signed for another chain returned **`-32602`**, whereas master returned **`-32003`** (`TransactionRejected`). The message and rejection behavior were unchanged. Both `eth_sendRawTransaction` and `eth_sendRawTransactionSync` use this helper, so clients that classify failures by code observed a wire-level compatibility change.

The fix restores `TransactionRejected` in the shared chain-mismatch branch and retains full-width chain-ID formatting. For example, a protected legacy transaction with chain ID 20143 submitted to chain 143 again returns `-32003` with `Invalid chain ID: expected 143, got 20143`. The 384-case admission regression covers existing Ethereum types and both methods' error factories. An additional native regression checks a `2^200` chain ID and the same rejection category, while native submissions on a disabled network still return the separate `InvalidParams` activation error.

## Finding 2: nullable receipt gas prices

**P2 — fixed: preserve the previous receipt JSON reader's null handling.** The shared [`TransactionReceipt`](../monad-execution/rust/crates/monad-transaction/src/rpc.rs) originally used a non-optional `U256` with `serde(default)`. An omitted field defaulted to zero, but explicit `"effectiveGasPrice": null` failed with `invalid type: null, expected a 32 byte hex string`. Master's Alloy reader accepted both forms and returned zero. The `gasPrice` alias had the same null-handling issue.

The affected surface is Rust code reading receipts returned by an RPC: the transaction generator's [`eth_json_rpc`](../monad-eth-testutil/examples/txgen/shared/eth_json_rpc.rs) and [`committed_tx_watcher`](../monad-eth-testutil/examples/txgen/workers/committed_tx_watcher.rs) now use this reader for transaction and block receipts. This does not demonstrate a changed response from this server: its receipt construction supplies a numeric price, which still serializes as a hex quantity. RPC request parameters do not use this receipt type.

The fix deserializes the price through `Option<U256>` and defaults only missing/null values to zero. The stored field remains `U256`; serialization emits the canonical `effectiveGasPrice` hex quantity. Malformed quantities, overflow and duplicate canonical/alias fields remain errors. The original `receipt_reader_accepts_null_effective_gas_price_like_master` regression is retained, and execution owns three additional receipt-reader tests across types 0–4 and `0x40`. Execution was committed before updating BFT's pin; no wire-format or feature-scope decision was needed.

## RPC surface compared

| Surface | Result and coverage |
| --- | --- |
| `eth_sendRawTransaction`, `eth_sendRawTransactionSync` | Parameter shapes are unchanged. Existing Ethereum raw envelopes retain their framing, hashes and recovery. The admission helper is compared with master for chain checks, unprotected legacy policy and each method's distinct decode-error code. The fix restores master's wrong-chain rejection code. |
| `eth_call`, `eth_estimateGas` and shared call-request consumers | Request fields are unchanged. The new branch applies only to explicit `type: "0x40"`. A frozen master converter compares existing fee/type/access-list/authorization combinations, defaults, integer boundaries and creation-size errors. |
| `eth_fillTransaction` | Request/response structures and Ethereum type selection remain unchanged. The existing fill-transaction unit cases run as part of the full RPC suite. Explicit native fill remains outside the milestone. |
| `eth_getTransaction*`, full/hash-only block responses | Existing transaction fields, pending/mined metadata and JSON quantities match master's representations for types 0–4. |
| `eth_getTransactionReceipt`, `eth_getBlockReceipts`, synchronous send receipts | Success/revert, call/creation, optional blob fields, logs and historical legacy receipt roots retain their representation. The null-price finding concerns reading these responses. |
| `eth_feeHistory` | Optional/empty rewards and Ethereum uint128 fee boundaries serialize identically; effective prices and tips match Alloy at fee caps and base-fee boundaries. |
| Debug raw transaction/receipt/block paths | Existing raw and network-RLP envelope bytes match Alloy. No extra auxiliary field is added to Ethereum envelopes. |
| WebSocket event payloads | Transaction and receipt aliases use the same shared serializers covered above. Subscription names and filters are unchanged. |

The method registration and request parameter structs were inspected against master. `eth_simulateV1` continues to reject `validation: false`; the execution bridge's new argument forwards the existing validated mode. This is not an additional simulation capability.

Master's call converter infers Ethereum encoding from fee and authorization fields even when a type is supplied. The regression preserves this behavior, including its existing treatment of access lists with legacy gas pricing; this review does not redesign request inference.

## Regression tests and validation

- [`ethereum_wire_compat.rs`](../monad-rpc/tests/ethereum_wire_compat.rs): protected/unprotected legacy, EIP-2930, EIP-1559, EIP-4844 and EIP-7702; pending/mined transactions, nonempty access/authorization lists, a blob-sidecar framing control, malformed/truncated submissions, hashes, signing digests and recovery, receipts, blocks, fee history and reader compatibility.
- [`call/ethereum_rpc_compat.rs`](../monad-rpc/src/handlers/eth/call/ethereum_rpc_compat.rs): 252 request combinations and 19 default/boundary/error cases against the frozen master converter.
- [`txn/ethereum_rpc_compat.rs`](../monad-rpc/src/handlers/eth/txn/ethereum_rpc_compat.rs): 384 combinations across existing Ethereum types, valid/truncated/trailing/empty bytes, chain IDs, unprotected policy and both methods' error factories.
- [`monad-transaction/tests/rpc.rs`](../monad-execution/rust/crates/monad-transaction/tests/rpc.rs): missing/null prices, both field names, exact uint256 round trips, malformed/overflowing quantities and duplicate-alias rejection across Ethereum and native receipts.

Run with the execution/TrieDB libraries and repository toolchain available:

```sh
cargo test --locked --release -p monad-rpc --test ethereum_wire_compat
cargo test --locked --release -p monad-rpc --lib ethereum_rpc_compat
# Include the existing RPC regressions as well:
cargo test --locked --release -p monad-rpc --lib
cargo test --locked --release --manifest-path monad-execution/rust/Cargo.toml -p monad-transaction
cargo +nightly-2025-12-09 fmt --all --check
```

Before the fixes, the full repository run on Linux x86-64 category-box using the integration toolchain and Rust 1.97.1 recorded **201 passes and two failing regressions**. The wire test reproduced the null-price issue for both field names across types 0–4. Both call-conversion regressions passed. The admission matrix completed all 384 cases and found 56 mismatches, all due to the wrong-chain error code; valid input and other rejection cases matched.

Running the entire RPC unit suite also found three old event-snapshot fixtures that failed at schema validation before exercising their assertions. The two event-server tests and synchronous-send test now use the existing `test-utils` snapshot adapter, matching the WebSocket tests already adapted in this PR. The historical JSON assertion compares parsed objects, retaining its typed decode and raw-value leakage checks: transaction object member order changes (`from` moves), while fields and values are preserved. Object order is not part of the RPC contract. Production event-ring validation remains strict.

Fix validation (2026-11-13), using the paired working-tree changes on category-box:

| Check | Result |
| --- | --- |
| Shared transaction crate: codec, framing and receipt-reader tests | 17 passed |
| Wire compatibility integration target | 13 passed |
| Full RPC unit suite, including 4 handler compatibility tests | 191 passed |
| Rust formatting in both workspaces (`nightly-2025-12-09`) and whitespace checks | Passed |

**All 221 tests pass, with no failures or ignored tests.** The two original regressions now pass, all 384 Ethereum admission cases match master, and the native full-width chain-ID control passes. The shared crate also passes its 17 tests locally. The full RPC suite includes the historical event/receipt and WebSocket assertions restored during review.

The category-box run used tmux session `rpc-compat-fixes-3352`. Logs and the exact source snapshot are retained under `.build/type40/category-box-rpc-compat-fixes-3352/` in the local and remote integration checkouts: `check-0.log` for the shared crate, `check-1.log` for wire compatibility, `check-2.log` for the RPC unit suite, and `session.log` for commands and exit codes. The remote node source overlays were restored after validation. The review's failing baseline remains recorded in `.build/type40/category-box-rpc-compat-3352/`.

## Compatibility boundaries

The identified error-code and receipt-reader regressions are corrected; no additional changed Ethereum RPC field shape, valid encoding or hash was found in the review matrix. This is not a complete live endpoint or state-transition equivalence proof: blob cases test representation only, and do not imply Monad admits blob transactions. Restart/replay, archive/MongoDB queries and a live wallet session are outside this review.

The PR changes Rust source APIs: Alloy response/envelope types are replaced, fee/chain quantities widen, and the native-aware envelope does not implement Alloy's uint128 transaction trait. External Rust callers may need source changes. On a native-enabled devnet, clients with exhaustive Ethereum-only transaction-type decoders also need `0x40` support when reading mixed blocks. Those extension/integration requirements are separate from compatibility of existing Ethereum transaction values.
