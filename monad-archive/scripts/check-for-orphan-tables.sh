#!/bin/bash

# Source the replicas list
source /home/ubuntu/.env

# Convert REPLICAS to array for easier checking
IFS=',' read -ra REPLICA_ARRAY <<< "$REPLICAS"

echo "Known replicas:"
for replica in "${REPLICA_ARRAY[@]}"; do
    echo "  - $replica"
done
echo

echo "Checking for orphaned tables..."
aws dynamodb list-tables --output text --query 'TableNames[]' | tr '\t' '\n' | while read -r table; do
    # Check if table is in our replicas list
    found=0
    for replica in "${REPLICA_ARRAY[@]}"; do
        if [ "$table" = "$replica" ]; then
            found=1
            break
        fi
    done
    
    if [ "$found" -eq 0 ]; then
        echo "  - $table"
    fi
done
