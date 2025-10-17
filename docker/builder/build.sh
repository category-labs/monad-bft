#!/bin/bash

# Build script for rust-builder Docker image
# This script builds and pushes the builder image to the registry

set -e

# Configuration
IMAGE_NAME="registry.devcore4.com/category-labs/builder"
DOCKERFILE_PATH="docker/builder/Dockerfile"

echo "Building Docker image..."

# Build the Docker image
docker build \
  -t "$IMAGE_NAME" \
  -f "$DOCKERFILE_PATH" \
  .

echo "Successfully built image: $IMAGE_NAME"

# Push the image to registry
echo "Pushing image to registry..."
docker push "$IMAGE_NAME"

echo "Successfully pushed image: $IMAGE_NAME"
