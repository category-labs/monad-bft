# RPC surface changes and compatibility

[PR #3352](https://github.com/category-labs/monad-bft/pull/3352) adds native `0x40` support to existing RPC methods. Method names and parameter structures remain unchanged. No breaking JSON-RPC change is intended for existing Ethereum transaction types `0x0`–`0x4`, and none remains identified in the comparison below after the compatibility fixes.

Native transactions extend the response types and numeric ranges clients can encounter. Strict Ethereum-only decoders need updating on native-enabled networks, and direct Rust integrations have source API changes. These boundaries are detailed below.

The native scope is sender-paid calls, contract creation and optional access lists on devnet chain **20143** after Prague. Authorization lists, sponsorship, batching, nonempty auxiliary data and node-managed signing are outside this milestone. See the [development profile](monad-type-40.md) for the paired execution implementation and runtime setup.

## RPC surface compared

| Surface | Change for native `0x40` | Compatibility for existing Ethereum transactions |
| --- | --- | --- |
| `eth_sendRawTransaction`, `eth_sendRawTransactionSync` | Accept the `0x40` prefix followed by `RLP([chainId, signedFeatures, []])`. Use the two-field canonical transaction's hash for the hash result or synchronous receipt's `transactionHash`. Native submission is restricted to devnet. | Existing envelope bytes, hashes, signer recovery, unprotected-legacy policy and method-specific decode errors are preserved. Wrong-chain submissions retain `-32003` (`TransactionRejected`). |
| `eth_call`, `eth_estimateGas` and shared call-request consumers | Explicit `type: "0x40"` selects native conversion, preserving 256-bit fees, contract creation and omitted versus empty access lists. Unsupported authorization/blob fields and invalid fee combinations are rejected. | Existing fee/type inference, defaults, integer boundaries and creation-size errors match the baseline converter. |
| `eth_fillTransaction` | Explicit native fill remains unsupported. | Existing request/response structures and Ethereum type selection are preserved. |
| Transaction lookups and full/hash-only block responses | Return native transaction objects or canonical hashes, including mixed Ethereum/native blocks. Native objects retain `type: "0x40"` and full-width quantities. | Existing transaction fields, pending/mined metadata and JSON quantities match the baseline representations for types `0x0`–`0x4`. |
| `eth_getTransactionReceipt`, `eth_getBlockReceipts`, synchronous send receipts | Return native receipt types with standard receipt fields and full-width effective gas prices. | Success/revert, call/creation, optional blob fields, logs and historical legacy receipt roots retain their representation. |
| `eth_getLogs` and receipt log extraction | Handle native receipts through the common log filtering path, preserving canonical transaction hashes, ordering and log metadata. | Existing log fields and address/topic filters are preserved. Native and Ethereum variants share regression coverage. |
| `eth_feeHistory` | Include native transactions in reward calculations without narrowing their fee quantities to `u128`. | JSON quantities remain hex strings. Optional/empty rewards and Ethereum fee-cap/base-fee boundaries match Alloy. |
| Debug raw transaction/receipt/block paths | Encode canonical native transactions and five-field native receipts. The submission wrapper is not part of raw canonical transaction output. | Existing raw and network-RLP bytes match Alloy. Ethereum envelopes gain no auxiliary field. |
| WebSocket event payloads | Transaction and receipt events use the shared serializers that support native types. | Subscription names, filters and existing Ethereum payload fields are preserved. |
| `eth_simulateV1` | Uses the shared transaction envelope and forwards the existing validation flag to the updated execution API. | No new simulation mode is enabled: `validation: false` remains rejected. |

Implementation: [submission](../monad-rpc/src/handlers/eth/txn.rs), [call conversion](../monad-rpc/src/handlers/eth/call.rs), [fees](../monad-rpc/src/handlers/eth/gas.rs), [data and logs](../monad-rpc/src/data/mod.rs), and [shared RPC representations](https://github.com/haythemsellami/monad/blob/d2fadff7c960e655ff7d3e76ea244f4dc8346aa4/rust/crates/monad-transaction/src/rpc.rs).

The existing Ethereum call converter infers encoding from fee and authorization fields even when a type is supplied. That behavior, including access lists with legacy gas pricing, is preserved; only explicit `type: "0x40"` takes the new conversion branch.

## Breaking changes and consumer impact

| Consumer | Impact and required action |
| --- | --- |
| JSON-RPC clients processing existing Ethereum transactions | **No breaking wire change identified in the compared cases.** Existing requests, response field meanings, encodings, hashes and error categories are retained. The validation scope below bounds this conclusion. |
| Clients with exhaustive Ethereum-only transaction or receipt decoders | **Can break on native-enabled devnets.** Add `0x40` decoding before reading native transactions, receipts, mixed blocks or transaction/receipt events. A client can encounter these responses even if it only submits Ethereum transactions. Raw-byte consumers also need the native transaction and receipt formats. |
| Clients assuming fees or fee-history rewards fit in `u128` | **Can overflow or reject native values.** Parse native fee quantities, effective prices and rewards as 256-bit values. Their JSON representation remains a hex quantity; existing Ethereum value ranges are unchanged. |
| Rust code depending directly on RPC/envelope types | **Source API changes.** Alloy envelope and response types are replaced by shared `monad-transaction` types, and fee/chain quantities widen. The shared envelope does not implement Alloy's transaction trait with `u128` fee methods. Update imports, type assumptions and conversions. |
| Deployments sharing execution event rings | **Internal schema change.** Build execution producers and RPC readers from the paired revisions and use a fresh event ring. Old live rings are rejected by production schema validation; see the [development profile](monad-type-40.md). |

Transaction JSON object member order can change. Fields and values are preserved for existing Ethereum transactions, but byte-for-byte JSON comparisons must normalize object order. Member order is not part of the JSON-RPC contract.

## Compatibility fixes included

- **Wrong-chain errors:** Both raw-send methods retain `-32003` (`TransactionRejected`) for a transaction chain mismatch, including full-width native chain IDs in the message. Native submission on a network where the type is disabled has a separate `-32602` (`InvalidParams`) error. The earlier error-code regression is fixed.
- **Nullable receipt prices:** The shared Rust receipt reader accepts missing/null `effectiveGasPrice` and its `gasPrice` alias as zero, preserving the previous Alloy behavior. Valid values retain 256-bit precision; malformed, overflowing or duplicate fields remain errors. This fix is in the pinned execution crate. It affects receipt deserialization by Rust consumers; the server continues to emit a numeric hex quantity.

## Regression tests and validation

Comparison baseline: master `e844f6086acac664f4bd2a8ffcc99f3b5ce4b9af`. Reviewed implementation: BFT `6dab54f1efcc6ade9bb2f3225b3c31dfcf13874f`, with execution `d2fadff7c960e655ff7d3e76ea244f4dc8346aa4` from [PR #2840](https://github.com/category-labs/monad/pull/2840). Both BFT revisions use Alloy 2.0.4.

Recorded compatibility-fix validation on Linux x86-64 category-box, **2026-11-13**, passed all **221 tests**, with no failures or ignored tests:

| Check | Coverage | Result |
| --- | --- | --- |
| [Shared transaction crate](https://github.com/haythemsellami/monad/blob/d2fadff7c960e655ff7d3e76ea244f4dc8346aa4/rust/crates/monad-transaction/tests/rpc.rs) | Codec/framing coverage and receipt-reader defaults, aliases, null handling and 256-bit boundaries | 17 passed across the crate |
| [Ethereum wire compatibility](../monad-rpc/tests/ethereum_wire_compat.rs) | Types `0x0`–`0x4`: encodings, hashes, recovery, transaction/receipt/block/fee-history JSON and reader behavior | 13 passed |
| Full RPC unit suite | [252 call-conversion combinations plus 19 boundary cases](../monad-rpc/src/handlers/eth/call/ethereum_rpc_compat.rs), [384 raw-admission combinations and native chain checks](../monad-rpc/src/handlers/eth/txn/ethereum_rpc_compat.rs), [native log handling](../monad-rpc/src/data/native_logs_tests.rs), existing fill-transaction, event and WebSocket tests | 191 passed |

Historical event fixtures use a test-only snapshot adapter; production event-ring schema validation remains strict. Formatting and whitespace checks passed in both Rust workspaces. Logs and the validated source snapshot are retained under `.build/type40/category-box-rpc-compat-fixes-3352/` in the integration checkout.

To reproduce with the execution/TrieDB libraries and repository toolchain available:

```sh
cargo test --locked --release -p monad-rpc --test ethereum_wire_compat
cargo test --locked --release -p monad-rpc --lib
cargo test --locked --release --manifest-path monad-execution/rust/Cargo.toml -p monad-transaction
cargo +nightly-2025-12-09 fmt --all --check
```

This comparison covers the listed representations and handler cases, not complete live endpoint or state-transition equivalence. Blob cases establish representation compatibility, not blob-transaction admission. Restart/replay, archive/MongoDB queries and a live wallet session remain outside this review.
