# blockdb

Blockdb stores information of produced blocks. There are five tables in blockdb, with their schema documented below.

### Txn Hash Table

Maps a transaction hash to its associated block hash and transaction index in the block.
```
transactionHash => {
    blockHash,
    transactionIndex
}
```

### Block Number Table

Maps a block number to its associated block hash.
```
blockNumber => {
    blockHash
}
```

### Block Tag Table

Maps a block tag (*latest* or *finalized*) to its associated block hash. Since Monad BFT has single slot finality, the *latest* block is the same as the *finalized* block.
```
blockTag => {
    blockHash,
    blockNumber
}
```

### Block Table

Maps the block hash to the full block information. A block contains the block header which includes fields such as state root and fee parameters, the block body which is a list of all signed transactions included in the block, and a list of validator withdrawals.
```
blockHash => {
    blockHeader,
    Vec<TransactionSigned>,
    Vec<Withdrawals>
}
```

### BFT Ledger Table

Maps the block hash to the consensus block, which includes information on the quorum certificate. 
```
blockHash => {
    blockProposerId
    round,
    payload,
    quorumCertificate
}
