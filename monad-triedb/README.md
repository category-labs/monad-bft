# monad-triedb

Triedb stores the blockchain state (i.e. account balance, contract bytecode, value at a particular storage slot etc). The schema are documented as follows.

**Account**
```
key: <block number: 6 bytes><state nibble (0): 1 nibble><keccak256(address): 32 bytes>
value: rlp([nonce, balance, code_hash])
```

**Storage**
```
key:  <block number: 6 bytes><state nibble (0): 1 nibble><keccak256(address): 32 bytes><keccak256(key): 32 bytes>
value: rlp(zeroless_view(storage_data)) | rlp(zeroless_view(storage_slot))
```

**Code**
```
key: <block number: 6 bytes><code nibble (1): 1 nibble><code hash: 32 bytes>
value: code
```

**Receipt**
```
key: <block number: 6 bytes><code nibble (2): 1 nibble><transaction index: 8 bytes>
value: rlp([tx_type, status, culmulative_gas_used, logs_bloom, logs])
```