## How to use

### Install pprof

```bash
go install github.com/google/pprof@latest
```

Or follow guidelines from [pprof](https://github.com/google/pprof)

### Collect profile

```bash
pprof -http=0.0.0.0:8080 http://0.0.0.0:32808/debug/pprof/heap
```

When collecting profile pprof will try to find binaries and static libs
for symbolization. If you built and running application in docker container,
copy them and point to the directory with `export PPROF_BINARY_PATH=~/monad-binaries`.
