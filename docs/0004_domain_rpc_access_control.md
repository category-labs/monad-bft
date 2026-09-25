# Domain RPC access control

The `/domain/<chain id>` routes of `monad-rpc` serve the state of a private domain. The
execution client already gates every call frame of a domain simulation through the spoke's
`canCall`, but that check only sees the `from` the caller chose, and the plain read methods
(blocks, logs, storage, transactions, receipts) never reach it. This document describes the
layer the RPC adds in front: who a request is served as, and what that account may ask for.

## Access tokens

Every request on a domain route carries an `X-Authorization-Token` header proving control of
an account. The token is an EIP-712 signature over the domain chain id:

```text
domain:  { name: "Monad Domain RPC", version: "1", chainId: <domain chain id> }
message: DomainAccessToken { chainId: <domain chain id> }
header:  <domain chain id>.<0x-prefixed 65-byte signature>
```

`monad-rpc` recovers the signer and treats it as the authenticated account for every JSON-RPC
call in that HTTP request, including each call of a batch. Requests without a valid token are
answered with JSON-RPC error `-32006` (`Unauthorized: ...`). A token signed for one domain is
not valid on any other. The root route ignores the header.

The token carries no expiry: it authenticates its signer on that domain for as long as the key
remains in use, so it must be handled like a password. It is a bearer credential that is not
bound to the request body, so domain RPC must be served over TLS. Only externally owned
accounts can authenticate; EIP-1271 contract signatures are not supported.

## Per-method rules

The authenticated account is then held to the rules below. A request that breaks one is
answered with JSON-RPC error `-32007` (`Forbidden: ...`).

| Methods | Rule |
| --- | --- |
| `eth_getBalance`, `eth_getTransactionCount`, `txpool_statusByAddress` | the requested account must be the signer |
| `eth_getTransactionByHash`, `eth_getTransactionReceipt` | the transaction's sender must be the signer |
| `eth_call`, `eth_estimateGas`, `eth_createAccessList`, `eth_fillTransaction` | `from` is replaced by the signer regardless of the request; state overrides are rejected |
| `eth_getLogs` | the filter must pin the signer, as a single value, in at least one topic position |
| `eth_getBlockByNumber`, `eth_getBlockByHash`, `eth_getBlockReceipts`, `eth_getBlockTransactionCountBy*`, `eth_getTransactionByBlock*AndIndex`, `eth_getCode`, `eth_getStorageAt`, `monad_getDomainHeader`, `eth_simulateV1`, `debug_*`, `admin_*`, `txpool_statusByHash` | domain operators only |
| `eth_chainId`, `eth_blockNumber`, `eth_syncing`, `eth_gasPrice`, `eth_maxPriorityFeePerGas`, `eth_feeHistory`, `eth_sendRawTransaction`, `eth_sendRawTransactionSync`, `net_version`, `web3_clientVersion` | any authenticated account |

Replacing `from` by the signer is what ties this layer to the spoke's access control: the
execution client evaluates `canCall(caller, contract, data)` with the caller of each frame,
and on a domain route that caller is now the account that proved it holds the key, not an
address named in the request.

Domain operators are the accounts given with repeatable `--domain-operator <chain id>=<address>`
flags. An operator is exempt from every rule above: it may read any account's state,
enumerate blocks, and simulate calls from any `from` with state overrides. A node operator
holds the domain's viewing key and sees everything anyway; the flag names the accounts that
may read through the RPC what the operator could read from disk. Validators attesting
checkpoints (`monad_getDomainHeader`, the spoke's message ids) and the relayer that rebuilds
batches from spoke logs run as operators of the node they query.

## Known limitations

- The token never expires and is not bound to a node or a request, so a leaked token grants
  its signer's view of the domain on every node serving it until the key is retired.
- `eth_getLogs` is gated by topic pinning alone. An account sees every log that names it in
  an indexed field, and none that do not, whatever the spoke's rules for the emitting
  contract say. Applications keyed by an id rather than an address (an order id, a message
  id) cannot be followed through logs by the accounts involved; they must read state.
  The spoke's access rules cover calls, not events.
- Transactions and receipts are served to their sender only. A system transaction delivering
  an L1 message to an account is visible to operators alone; the recipient learns of it from
  logs that name it or from state.
- Historical reads are limited to what the triedb retains; the rules do not change that.
- Statesync and the node's own database are outside this layer: a peer the node serves state
  to, or anyone with the disk, reads the domain without a token.
