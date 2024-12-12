#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <table-name>"
    exit 1
fi

TABLE_NAME=$1

echo "Removing auto-scaling settings for table $TABLE_NAME..."

# Remove write capacity scaling
echo "Removing write capacity scaling policy..."
aws application-autoscaling delete-scaling-policy \
    --service-namespace dynamodb \
    --resource-id "table/$TABLE_NAME" \
    --scalable-dimension dynamodb:table:WriteCapacityUnits \
    --policy-name "${TABLE_NAME}_WriteScalingPolicy"

echo "Deregistering write capacity scaling target..."
aws application-autoscaling deregister-scalable-target \
    --service-namespace dynamodb \
    --resource-id "table/$TABLE_NAME" \
    --scalable-dimension dynamodb:table:WriteCapacityUnits

# Remove read capacity scaling
echo "Removing read capacity scaling policy..."
aws application-autoscaling delete-scaling-policy \
    --service-namespace dynamodb \
    --resource-id "table/$TABLE_NAME" \
    --scalable-dimension dynamodb:table:ReadCapacityUnits \
    --policy-name "${TABLE_NAME}_ReadScalingPolicy"

echo "Deregistering read capacity scaling target..."
aws application-autoscaling deregister-scalable-target \
    --service-namespace dynamodb \
    --resource-id "table/$TABLE_NAME" \
    --scalable-dimension dynamodb:table:ReadCapacityUnits

# Delete the table
echo "Deleting table $TABLE_NAME..."
aws dynamodb delete-table --table-name "$TABLE_NAME"

echo "Waiting for table deletion to complete..."
aws dynamodb wait table-not-exists --table-name "$TABLE_NAME"

echo "Table $TABLE_NAME has been removed."
