#!/bin/bash

# Check for --stop flag
STOP_MODE=0
if [ "$1" = "--stop" ]; then
    STOP_MODE=1
fi

# Get number of desired replicas from .env
source /home/ubuntu/.env
desired_count=$(echo "${REPLICAS:-}" | grep -v '^$' | tr ',' '\n' | wc -l)
echo "Desired count: $desired_count"

# Get current instance count
current_count=$(systemctl list-units "monad-indexer@*.service" --all --full --no-legend | awk '{print $1}' | sed -n 's/monad-indexer@\([0-9]*\).service/\1/p' | wc -l)
echo "Current count: $current_count"

if [ $STOP_MODE -eq 1 ]; then
    echo "Stopping all instances..."
    for i in $(seq 0 $((desired_count-1))); do
        echo "Stopping instance $i..."
        sudo systemctl stop monad-indexer@$i.service
    done
    exit 0
fi

# Create and start instances up to desired count
for i in $(seq 0 $((desired_count-1))); do
    echo "Ensuring instance $i exists and is running..."
    sudo systemctl enable monad-indexer@$i.service
    sudo systemctl start monad-indexer@$i.service
done

# Clean up excess instances
for i in $(systemctl list-units "monad-indexer@*.service" --all --full --no-legend | awk '{print $1}' | sed -n 's/monad-indexer@\([0-9]*\).service/\1/p'); do
    if [ "$i" -ge "$desired_count" ]; then
        echo "Removing excess instance $i..."
        sudo systemctl stop monad-indexer@$i.service
        sudo systemctl disable monad-indexer@$i.service
    fi
done
