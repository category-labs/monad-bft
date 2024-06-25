## Full nodes

A network of 4 full nodes, each running a consensus client, execution client, and a RPC server

### Script usage
```
scripts/net-run.sh <mode> --output-dir <dir_path> --net-dir <net_path> --flexnet-root <flexnet_root> --monad-bft-root <monad_bft_root>
```
**Mode 1: Run** 
Runs the network. RPC ports are forwarded to 8080-8083 on localhost

**Mode 2: Test**
The test submits transactions to different RPC endpoints and assert that transactions land in everyone's ledger, and the account balance is consistent across all nodes
