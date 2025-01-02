#!/bin/bash

# Source the replicas list
source /home/ubuntu/.env

# Constants for table creation
MIN_RCU=100
MAX_RCU=40000
MIN_WCU=100
MAX_WCU=400000

# Function to check if table exists
check_table_exists() {
    local table_name=$1
    aws dynamodb describe-table --table-name "$table_name" >/dev/null 2>&1
    return $?
}

# Process each replica
echo "$REPLICAS" | tr ',' '\n' | while read -r replica; do
    if [ -n "$replica" ]; then
        echo "Checking table for replica: $replica"
        if ! check_table_exists "$replica"; then
            echo "Table doesn't exist, creating..."
            bash create_table.sh "$replica" $MIN_RCU $MAX_RCU $MIN_WCU $MAX_WCU
        else
            echo "Table $replica already exists"
        fi
    fi
done
