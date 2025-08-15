services:
  indexer:
    image: archiver
    init: true
    environment:
      - RUST_LOG=info
    command: |
      monad-indexer
      --block-data-source "aws testnet-ltu-032-0 50"
      --archive-sink "mongodb mongodb://admin:pwd@archive-db:27017 archive-db 50"
      --skip-connectivity-check
      --max-concurrent-blocks 50
      --max-blocks-per-iteration 10000
      --start-block 0
    restart: no

  mainnet-checker:
    image: archiver
    init: true
    volumes:
      - ~/.aws:/root/.aws:ro
    environment:
      - RUST_LOG=info
    command: |
      monad-archive-checker --bucket mainnet-checker-3 checker --init-replicas "aws mainnet-deu-009-0 50,aws mainnet-deu-010-0 50"
    restart: on-failure

  testnet-checker:
    image: archiver
    init: true
    volumes:
      - ~/.aws:/root/.aws:ro
    environment:
      - RUST_LOG=info
    command: |
      monad-archive-checker --bucket testnet-checker-2 checker --disable-rechecker
    restart: on-failure

  testnet-checker-fixer:
    image: archiver
    init: true
    volumes:
      - ~/.aws:/root/.aws:ro
    environment:
      - RUST_LOG=info
    command: |
      monad-archive-checker --bucket testnet-checker-2 fault-fixer  --end 7489000   --commit-changes --verify
    restart: on-failure

  archive-db:
    image: mongo:latest
    command: mongod --bind_ip 0.0.0.0
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: pwd
    ports:
      - '27017:27017'
