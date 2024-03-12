### Usage
To spin up `net0`, from `flexnet`, run
```
nets/net0/scripts/net-run.sh --output-dir logs --net-dir nets/net0/ --image-root images --monad-bft-root ../..
```

Rerun tc-gen.py if topology changes

### Config gen
```
docker run -it --rm -v ./:/flexnet monad-python-dev:latest bash -c "cd /flexnet/nets/net0 && python3 ../../common/config-gen.py -c 4 -s ''"
```
