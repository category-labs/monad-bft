# Native Batch Calls

Native type `0x40` accepts either CALL `0x03` or CALLS `0x08`, with exactly one present. CALLS is a nonempty ordered list of DEST/VALUE/DATA feature maps; only entry zero may create a contract. A one-entry CALLS retains its feature identity. Canonical bytes remain `0x40 || rlp([chainId, features])`; submissions append an empty auxiliary list. Native batching follows the existing Monad Prague activation rule on all configured chain IDs.

One sender nonce, one shared gas budget, and one aggregate receipt cover the entire batch. Entries stop on the first failure and roll back all user effects. Gas and nonce effects persist. Each additional entry costs 21,000 intrinsic gas. The access-list byte charge and data floor cover the entire signed batch. Values are checked when entries run; consensus does not require the sum of values to be affordable upfront.

## Requests

`eth_call`, `eth_estimateGas`, `eth_createAccessList`, `eth_fillTransaction`, and `debug_traceCall` accept:

```json
{"jsonrpc":"2.0","id":1,"method":"eth_estimateGas","params":[{
  "type":"0x40",
  "from":"0x1111111111111111111111111111111111111111",
  "calls":[
    {"to":"0x2222222222222222222222222222222222222222","value":"0x1","input":"0x"},
    {"to":"0x3333333333333333333333333333333333333333","value":"0x2","input":"0x"}
  ]
}]}
```

Omitting `type` with `calls` selects native type `0x40`. Do not combine `calls` with scalar `to`, `value`, `input`, or `data`, including explicit null fields. Omitted entry value and input default to zero and empty data. A missing or null destination selects creation. `eth_call` returns the last attempted entry's output or the failing revert data. Estimation covers the entire batch. `eth_fillTransaction` returns `raw` canonical signing bytes plus `tx` containing the complete filled request.

For simulation, batch CREATE uses the sender nonce from the selected block state after applying state overrides, matching scalar CREATE. Omitting the request's `nonce` does not imply creation at nonce zero. This applies to call, estimation, access-list generation, and call tracing; simulation does not consume a nonce or persist the created contract.

## Execution Views

Native transaction queries and full blocks expose `calls` as the attempted prefix, including a failing entry and earlier rolled-back entries. Pending transactions expose `calls: []`. Exactly one attempted entry populates scalar `to`, `value`, and `input`; all other counts project null, zero, and empty data. Access and authorization lists normalize to empty arrays when absent.

RPC retains accepted native pool payloads for pending transaction-by-hash and raw lookups. Mined data takes precedence; commit, drop, eviction, and snapshot reconciliation remove stale pending payloads. A fresh RPC connection sees hash-only pool snapshots, so pending transactions whose payload it has not observed can still return null until execution or a subsequent pool insertion event.

Transaction JSON therefore does not reconstruct signed intent. Use `debug_getRawTransaction` and verify the canonical hash. The complete signed payload remains in consensus, execution events, and storage; the attempted count travels independently of optional call tracing. Historical single-CALL receipts without the count remain readable. A missing batch count is an error, never a reason to guess the signed list was fully executed.

Receipts retain their aggregate shape and canonical five-field envelope with empty extensions. `to` follows the same execution projection. `contractAddress` is derived from an actual first-entry creation using the original sender nonce. No sponsorship or fee-payer field is introduced.

`eth_simulateV1` requires the RPC flag `--enable-eth-simulate-v1`. The existing implementation supports validation mode only, so requests must supply fee caps sufficient for the simulated block. It includes nested entry results:

```json
{"status":"0x0","returnData":"0x","calls":[
  {"status":"0x1","returnData":"0x"},
  {"status":"0x0","returnData":"0x"},
  {"status":null,"returnData":"0x"}
]}
```

The third entry was not attempted. Traces use a synthetic CALL from sender to itself, with attempted entries as ordered children; this root is not an EVM call and adds no execution depth. Reverted logs do not survive in the aggregate receipt or committed trace logs.

## Compatibility

Ethereum transaction formats and behavior are unchanged. Consumers of native transaction JSON must adopt the execution-view semantics; adding fields alone does not make old canonical reconstruction logic correct. The execution-owned Rust RPC model guards normal conversions from a received projection into signing requests. Viem uses canonical bytes for replacement classification and keeps query `input` entries distinct from signing `data` entries. Native feature `0x05` remains disabled; existing Ethereum delegation and already delegated accounts remain supported.
