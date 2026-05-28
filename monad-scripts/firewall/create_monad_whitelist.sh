#!/bin/bash
set -e # exit on error
set -x # echo commands

PORT="${PORT:-8000}"
WHITELIST_NAME="${WHITELIST_NAME:-monad_whitelist}"

sudo ipset create ${WHITELIST_NAME} hash:ip family inet
IP_LIST=$(sudo -i -u monad monad-debug-node -c monad-bft/controlpanel.sock get-peers | grep address | cut -d\" -f 2 | cut -d: -f 1)
IP_COUNT=$(echo "${IP_LIST}" | wc -w)
if [ "$IP_COUNT" -lt 150 ]; then
	echo "Warning: Was expecting to fetch at least 150 IP addresses, but got ${IP_COUNT}"
fi

for IP in ${IP_LIST}; do
	sudo ipset add ${WHITELIST_NAME} ${IP}
done

echo "Show whitelist"
sudo ipset list ${WHITELIST_NAME}
