### Docker commands

Command to build from the `monad-bft` folder:
```
docker build -f docker/testground/Dockerfile . -t <image_name>
```

Command to run the image:
```
docker run --cap-add=NET_ADMIN <image_name>
```

### Default behavior

- The logs are written in the directory `/usr/src/monad-bft/testground_logs` within the container. To access the logs, `--mount type=bind,src=<directory_in_host>,dst=/usr/src/monad-bft/testground_logs` can be added to the `docker run` command.
- The command `./run_testground.sh -l` is run on container initialization. This can be overridden by passing `./run_testground.sh <custom_flags>` at the end of the `docker run` command.

### Custom flags

The wrapper script `./run_testground.sh` takes the following arguments:
- `-l` to set the network interface to loopback. There will be added latency between nodes.
- `-t` to generate pcap files for packets sent/received by each node (1 pcap file per node).
- `-n` to set number of nodes per region. Default is `5,0,0,0,0` which is 5 nodes in region 1 and no other nodes. The latency between each region is in `topology_gen.py`.
- `-s` to set simulation length (in seconds). Default value is 60.
- `-p` to set proposal size (in bytes). Default value is 2000000 (2MB).
- `-d` to set rust log level. Default is INFO.
