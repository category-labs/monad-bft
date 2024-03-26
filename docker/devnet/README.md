## Devnet

A local devnet consists of running a Monad node and a RPC server (which is the interface to send requests to the node). 

**Option 1: Docker Compose (Recommended)**

Run the docker compose script to start a local instance:
1. Navigate to the current directory (`docker/devnet`)
2. Run `bash clean.sh && docker compose up`

**Option 2: Run docker containers manually**

It is also possible to run the node and RPC server manually. To start the instance, run the following commands in the root directory of `monad-bft`:
1. Run `bash docker/devnet/clean.sh` to clean all previous generated files
2. Run `docker build -t devnet -f docker/devnet/Dockerfile .` to build the docker image for the node
3. Run `docker run -d devnet` to run the docker container
4. In a separate terminal, run `docker build -t rpc -f docker/rpc/Dockerfile .` to build the docker image for the rpc server
5. Run `docker run rpc` to run the docker container