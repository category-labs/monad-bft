#!/bin/bash
set -x # echo commands

PORT="${PORT:=8000}"
WHITELIST_NAME="${WHITELIST_NAME:=monad_whitelist}"

# Destroy old rules and whitelist
sudo iptables -D INPUT -p udp --dport ${PORT} -m set --match-set ${WHITELIST_NAME}  src -j ACCEPT
sudo iptables -D INPUT -p udp --dport ${PORT} -j DROP
sudo ipset destroy ${WHITELIST_NAME}
