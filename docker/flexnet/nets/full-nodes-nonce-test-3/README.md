## Full nodes nonce test 3

A network of 4 full nodes, each running a consensus client, execution client, and a RPC server

### Script usage
```
scripts/net-run.sh --output-dir <dir_path> --net-dir <net_path> --flexnet-root <flexnet_root> --monad-bft-root <monad_bft_root>
```

The test includes sending 10 transactions from 4 different sender addresses to the 4 nodes. It sends 50 random invalid transactions
with duplicate nonces to a random node.
It asserts that the first 10 transactions from each sender address run to completion and the rest of the invalid transactions
are not run.
