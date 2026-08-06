#!/bin/sh
# Operator tool: discard all data on the TrieDB block device.
# DESTRUCTIVE — this was previously an interactive prompt in the package's
# postrm; it now has to be run explicitly.
set -eu

DEVICE=${1:-/dev/triedb}

if [ ! -b "$DEVICE" ]; then
  echo "Not a block device: $DEVICE" >&2
  exit 1
fi

printf 'This will destroy all TrieDB data on %s. Type "yes" to continue: ' "$DEVICE"
read -r answer
if [ "$answer" != "yes" ]; then
  echo "Aborted, TrieDB remains intact."
  exit 1
fi

blkdiscard "$DEVICE"
echo "TrieDB wiped."
