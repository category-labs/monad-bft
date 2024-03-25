## Devnet

To run a local devnet node, run the following commands in the root directory of `monad-bft`
1. To build the docker image, run `docker build -t devnet -f docker/devnet/Dockerfile .`
2. To run the docker container, run `docker run -d devnet`