# queryX demo curl commands
#
# Assumes monad-rpc queryX-only server is listening on:
#   http://127.0.0.1:18080
#
# These queries use ERC20 Transfer logs over a large historical range and keep
# response fields bounded for demo readability.

URL=http://127.0.0.1:18080
FROM_BLOCK=0x3500000
TRANSFER_TOPIC=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
TOKEN=0x29f9c424803ac7b09d97ee218adbbb9329703946
FROM_WALLET_TOPIC=0x00000000000000000000000037c4e0a368fda5d6946a77ec4bb016921b9c6cfd
TO_WALLET_TOPIC=0x000000000000000000000000b90c064f6bdcea0137274814c5f5f859a90fa251

# 1. Sparse wallet history: all ERC20 Transfers from one wallet.
curl -sS -H 'content-type: application/json' --data "{
  \"jsonrpc\":\"2.0\",
  \"id\":1,
  \"method\":\"eth_queryLogs\",
  \"params\":[{
    \"fromBlock\":\"$FROM_BLOCK\",
    \"toBlock\":\"finalized\",
    \"limit\":\"0x64\",
    \"filter\":{
      \"topics\":[\"$TRANSFER_TOPIC\",\"$FROM_WALLET_TOPIC\"]
    },
    \"fields\":{
      \"logs\":[\"blockNumber\",\"transactionIndex\",\"logIndex\",\"address\",\"topics\",\"data\"]
    }
  }]
}" "$URL" | jq .
# 2. Token-specific Transfer logs.
curl -sS -H 'content-type: application/json' --data "{
  \"jsonrpc\":\"2.0\",
  \"id\":2,
  \"method\":\"eth_queryLogs\",
  \"params\":[{
    \"fromBlock\":\"$FROM_BLOCK\",
    \"toBlock\":\"finalized\",
    \"limit\":\"0x64\",
    \"filter\":{
      \"address\":\"$TOKEN\",
      \"topics\":[\"$TRANSFER_TOPIC\"]
    },
    \"fields\":{
      \"logs\":[\"blockNumber\",\"transactionIndex\",\"logIndex\",\"address\",\"topics\",\"data\"]
    }
  }]
}" "$URL" | jq .

# 3. Token-specific Transfers from one wallet.
curl -sS -H 'content-type: application/json' --data "{
  \"jsonrpc\":\"2.0\",
  \"id\":3,
  \"method\":\"eth_queryLogs\",
  \"params\":[{
    \"fromBlock\":\"$FROM_BLOCK\",
    \"toBlock\":\"finalized\",
    \"limit\":\"0x64\",
    \"filter\":{
      \"address\":\"$TOKEN\",
      \"topics\":[\"$TRANSFER_TOPIC\",\"$FROM_WALLET_TOPIC\"]
    },
    \"fields\":{
      \"logs\":[\"blockNumber\",\"transactionIndex\",\"logIndex\",\"address\",\"topics\",\"data\"]
    }
  }]
}" "$URL" | jq .

# 4. Token-specific Transfer from one wallet to another wallet.
curl -sS -H 'content-type: application/json' --data "{
  \"jsonrpc\":\"2.0\",
  \"id\":4,
  \"method\":\"eth_queryLogs\",
  \"params\":[{
    \"fromBlock\":\"$FROM_BLOCK\",
    \"toBlock\":\"finalized\",
    \"limit\":\"0x64\",
    \"filter\":{
      \"address\":\"$TOKEN\",
      \"topics\":[\"$TRANSFER_TOPIC\",\"$FROM_WALLET_TOPIC\",\"$TO_WALLET_TOPIC\"]
    },
    \"fields\":{
      \"logs\":[\"blockNumber\",\"transactionIndex\",\"logIndex\",\"address\",\"topics\",\"data\"]
    }
  }]
}" "$URL" | jq .

# 5. Pagination-shaped token query with a small page size.
# Note: queryX completes the current block after reaching limit, so this may
# return more than exactly 10 logs.
curl -sS -H 'content-type: application/json' --data "{
  \"jsonrpc\":\"2.0\",
  \"id\":5,
  \"method\":\"eth_queryLogs\",
  \"params\":[{
    \"fromBlock\":\"$FROM_BLOCK\",
    \"toBlock\":\"finalized\",
    \"limit\":\"0xa\",
    \"filter\":{
      \"address\":\"$TOKEN\",
      \"topics\":[\"$TRANSFER_TOPIC\"]
    },
    \"fields\":{
      \"logs\":[\"blockNumber\",\"transactionIndex\",\"logIndex\",\"address\",\"topics\",\"data\"]
    }
  }]
}" "$URL" | jq .
