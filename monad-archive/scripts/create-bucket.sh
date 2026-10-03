#!/bin/bash

# Check if bucket name is provided
if [ $# -eq 0 ]; then
    echo "Error: Bucket name must be provided"
    echo "Usage: $0 <bucket-name>"
    exit 1
fi

S3_BUCKET_NAME=$1
REGION="us-east-2"

echo "Creating bucket: $S3_BUCKET_NAME in region: $REGION"

# Create the bucket
aws s3api create-bucket \
    --bucket "$S3_BUCKET_NAME" \
    --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"

if [ $? -ne 0 ]; then
    echo "Failed to create bucket"
    exit 1
fi

echo "Disabling block public access settings..."

# Disable block public access
aws s3api put-public-access-block \
    --bucket "$S3_BUCKET_NAME" \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

if [ $? -ne 0 ]; then
    echo "Failed to configure public access settings"
    exit 1
fi

echo "Creating bucket policy..."

# Create bucket policy JSON
cat > bucket-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::$S3_BUCKET_NAME/*"
        },
        {
            "Sid": "PublicListBucket",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::$S3_BUCKET_NAME"
        }
    ]
}
EOF

# Apply the bucket policy
aws s3api put-bucket-policy --bucket "$S3_BUCKET_NAME" --policy file://bucket-policy.json

if [ $? -ne 0 ]; then
    echo "Failed to apply bucket policy"
    rm bucket-policy.json
    exit 1
fi

# Clean up
rm bucket-policy.json

echo "Successfully created and configured public S3 bucket: $S3_BUCKET_NAME"