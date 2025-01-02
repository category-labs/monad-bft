#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <replica-name>"
    exit 1
fi

# Source the replicas list
source /home/ubuntu/.env

# Convert REPLICAS to array
IFS=',' read -ra REPLICA_ARRAY <<< "$REPLICAS"

# Find the index of the replica
index=-1
for i in "${!REPLICA_ARRAY[@]}"; do
    if [ "${REPLICA_ARRAY[$i]}" = "$1" ]; then
        index=$i
        break
    fi
done

if [ $index -eq -1 ]; then
    echo "Error: Replica '$1' not found in REPLICAS list"
    exit 1
fi

# Follow the journal for this service instance
sudo journalctl -fu monad-indexer@$index.service -o cat --no-pager 
