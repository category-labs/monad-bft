# Hive RPC compatibility harness

This directory contains the Monad setup helper used with the
`category-labs/hive-runner` RPC compatibility tests. It imports the Hive test
chain RLP into Monad execution, starts `monad-rpc` against the resulting state,
and exposes JSON-RPC on `http://localhost:8080`.

This is not a full upstream Ethereum Hive client wrapper. It does not start a
consensus node or txpool, so write-path methods such as `eth_sendRawTransaction`
are not expected to work through this harness.

## Usage

From this repository, run:

```bash
docker/single-node/hive/run.sh \
  --chain-rlp /path/to/hive-runner/tests/chain.rlp \
  --nblocks <block-count>
```

`--chain-rlp` must point at the Hive runner test chain RLP. `--nblocks` is
passed to the execution import command and should match the number of blocks to
import from that file.

The helper writes its generated compose workspace under
`docker/single-node/hive/logs/`, including the generated node config with Hive
chain ID `3503995874084926` (`0xc72dd9d5e883e`).

After `monad-rpc` is running, run the compatibility tests from the
`category-labs/hive-runner` checkout:

```bash
RPC_URL=http://localhost:8080 ./rpc-compat
```

To verify the chain ID manually:

```bash
curl -s http://localhost:8080 \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
```
