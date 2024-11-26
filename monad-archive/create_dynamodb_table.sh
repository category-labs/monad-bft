#!/bin/bash

# Input parameters
TABLE_NAME=$1
MIN_RCU=$2
MAX_RCU=$3
MIN_WCU=$4
MAX_WCU=$5

if [ -z "$TABLE_NAME" ] || [ -z "$MIN_RCU" ] || [ -z "$MAX_RCU" ] || [ -z "$MIN_WCU" ] || [ -z "$MAX_WCU" ]; then
  echo "Usage: $0 <table-name> <min-rcu> <max-rcu> <min-wcu> <max-wcu>"
  exit 1
fi

# Step 1: Create the table
echo "Creating DynamoDB table $TABLE_NAME..."
aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions AttributeName=tx_hash,AttributeType=S \
  --key-schema AttributeName=tx_hash,KeyType=HASH \
  --provisioned-throughput ReadCapacityUnits="$MIN_RCU",WriteCapacityUnits="$MIN_WCU"

echo "Waiting for the table $TABLE_NAME to become ACTIVE..."
aws dynamodb wait table-exists --table-name "$TABLE_NAME"
echo "Table $TABLE_NAME is now ACTIVE."

# Step 2: Configure auto-scaling for ReadCapacityUnits
echo "Configuring auto-scaling for ReadCapacityUnits..."
aws application-autoscaling register-scalable-target \
  --service-namespace dynamodb \
  --resource-id table/"$TABLE_NAME" \
  --scalable-dimension dynamodb:table:ReadCapacityUnits \
  --min-capacity "$MIN_RCU" \
  --max-capacity "$MAX_RCU"

aws application-autoscaling put-scaling-policy \
  --service-namespace dynamodb \
  --resource-id table/"$TABLE_NAME" \
  --scalable-dimension dynamodb:table:ReadCapacityUnits \
  --policy-name "${TABLE_NAME}_ReadScalingPolicy" \
  --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
    "TargetValue": 70.0,
    "PredefinedMetricSpecification": {
      "PredefinedMetricType": "DynamoDBReadCapacityUtilization"
    },
    "ScaleInCooldown": 60,
    "ScaleOutCooldown": 60
  }'

# Step 3: Configure auto-scaling for WriteCapacityUnits
echo "Configuring auto-scaling for WriteCapacityUnits..."
aws application-autoscaling register-scalable-target \
  --service-namespace dynamodb \
  --resource-id table/"$TABLE_NAME" \
  --scalable-dimension dynamodb:table:WriteCapacityUnits \
  --min-capacity "$MIN_WCU" \
  --max-capacity "$MAX_WCU"

aws application-autoscaling put-scaling-policy \
  --service-namespace dynamodb \
  --resource-id table/"$TABLE_NAME" \
  --scalable-dimension dynamodb:table:WriteCapacityUnits \
  --policy-name "${TABLE_NAME}_WriteScalingPolicy" \
  --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
    "TargetValue": 70.0,
    "PredefinedMetricSpecification": {
      "PredefinedMetricType": "DynamoDBWriteCapacityUtilization"
    },
    "ScaleInCooldown": 60,
    "ScaleOutCooldown": 60
  }'

echo "Auto-scaling policies configured for table $TABLE_NAME."
