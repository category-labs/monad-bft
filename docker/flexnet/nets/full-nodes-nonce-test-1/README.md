## Full nodes nonce test 1

A network of 4 full nodes, each running a consensus client, execution client, and a RPC server

### Script usage
```
scripts/net-run.sh --output-dir <dir_path> --net-dir <net_path> --flexnet-root <flexnet_root> --monad-bft-root <monad_bft_root>
```

The test submits 10 valid transactions with sequential nonces to a single node, then submits 20 invalid transactions
with non-sequential nonces.
It asserts that the 10 valid transactions run to completion and the transaction count for the sender account is only 10.
