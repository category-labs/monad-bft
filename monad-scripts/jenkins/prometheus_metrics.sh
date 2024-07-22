#!/bin/bash
set -ex
DIR=$1

function normalize_metric() {
  local input_string="$1"
  # Replace all forbidden characters with an underscore
  cleaned_string=$(echo "$input_string" | sed 's/[^a-zA-Z0-9_:]/_/g')
  # Replace multiple consecutive underscores with a single underscore
  cleaned_string=$(echo "$cleaned_string" | tr -s '_')
  # Remove trailing underscores
  cleaned_string=$(echo "$cleaned_string" | sed 's/_$//')

  echo "$cleaned_string"
}

function output() {
  dir="$1"
  original_metric_name=$(jq -r '.full_id' "$dir"/benchmark.json)
  normalized_metric_name=$(normalize_metric "$original_metric_name")
  measurement=$(jq -r '.mean.point_estimate' "$dir"/estimates.json)
  printf "%s_ns_per_iter{commit=\"%s\"} %s\n" "$normalized_metric_name" "$(git rev-parse --short HEAD)" "$measurement"
}

export -f output
export -f normalize_metric

if [ -z "$1" ]; then
  echo "Error: Path to 'target/criterion' not provided."
  echo "Usage: $0 /path/to/target/criterion"
  exit 1
fi

find $DIR -type d -name "new" -exec bash -c 'output "$1"' - {} \;
