## Consensus nodes nonce test 1

A single node running a consensus client and a RPC server

### Script usage
```
scripts/net-run.sh --output-dir <dir_path> --net-dir <net_path> --flexnet-root <flexnet_root> --monad-bft-root <monad_bft_root>
```

The test submits 10 valid transactions with sequential nonces to a single node, then submits 20 invalid transactions
with non-sequential nonces.
It asserts that the 10 valid transactions are seen in the ledger blocks and the rest of the transactions are not.

