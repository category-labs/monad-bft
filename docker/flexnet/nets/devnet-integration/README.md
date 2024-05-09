## Devnet integration

A local devnet consists of running a Monad node and a RPC server (which is the interface to send requests to the node).

### Script usage
```
scripts/net-run.sh <mode> --output-dir <output_path> --net-dir <net_path> --image-root <image_root> --monad-bft-root <monad_bft_root>
```
where `mode` can be `run` or `test`.

The script creates a unique directory in `output_path` and spins up the services from there

**Mode 1: Run** 

Spin up `monad-node` and `monad_rpc` services. `monad_rpc` server can be accessed at `http://localhost:8080`. The service root path is printed at the end of the script. 

**Mode 2: Test**

Runs an integration test to sanity check the devnet setup. 

The RPC calls involved are
- eth_sendRawTransaction
- eth_getTransactionReceipt
- eth_getTransactionCount
- eth_getBalance
