#!/bin/bash

LEDGER_DIR=/home/monad/monad-bft/ledger
RETENTION_FORKPOINT_MINS=${RETENTION_FORKPOINT_MINS:-300}
RETENTION_VALIDATORS_MINS=${RETENTION_VALIDATORS_MINS:-43200}
RETENTION_LEDGER_MINS=${RETENTION_LEDGER_MINS:-600}

printf 'Cleanup started: ledger=%sm, forkpoint=%sm, validators=%sm\n' \
  "$RETENTION_LEDGER_MINS" "$RETENTION_FORKPOINT_MINS" "$RETENTION_VALIDATORS_MINS"

RECENT_LEDGER_FILE=$(find "$LEDGER_DIR" -type f -mmin -20 -print -quit)
if [ -n "$RECENT_LEDGER_FILE" ]; then
    find /home/monad/monad-bft/config/forkpoint/ -type f -name 'forkpoint.rlp.*' \
      -mmin +"$RETENTION_FORKPOINT_MINS" -delete 2>/dev/null
    find /home/monad/monad-bft/config/forkpoint/ -type f -name 'forkpoint.toml.*' \
      -mmin +"$RETENTION_FORKPOINT_MINS" -delete 2>/dev/null
    find /home/monad/monad-bft/config/validators/ -type f -name 'validators.toml.*' \
      -mmin +"$RETENTION_VALIDATORS_MINS" -delete 2>/dev/null
    find "$LEDGER_DIR/headers" -type f -mmin +"$RETENTION_LEDGER_MINS" -delete 2>/dev/null
    find "$LEDGER_DIR/bodies" -type f -mmin +"$RETENTION_LEDGER_MINS" -delete 2>/dev/null
fi

if [ -d "${BLOCKCAPD_ROOT_DIR:-}" ] && [ "${BLOCKCAPD_MAX_GB:-0}" -ne 0 ]; then
  BLOCKCAPD_MAX_BYTES=$((BLOCKCAPD_MAX_GB * 1024 * 1024 * 1024))

  while [ "$(du -sb "$BLOCKCAPD_ROOT_DIR" | awk '{print $1}')" -gt "$BLOCKCAPD_MAX_BYTES" ]; do
    OLDEST_BLOCKCAPD_DIR=$(find "$BLOCKCAPD_ROOT_DIR" -mindepth 1 -maxdepth 1 -type d \
      -printf '%T@ %p\n' | sort -n | head -1 | cut -d' ' -f2-)
    [ -n "$OLDEST_BLOCKCAPD_DIR" ] || break
    rm -rf "$OLDEST_BLOCKCAPD_DIR"
  done
fi

exit 0
