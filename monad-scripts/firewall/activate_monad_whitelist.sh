#!/bin/bash
set -e # exit on error
set -x # echo commands

PORT="${PORT:-8000}"
WHITELIST_NAME="${WHITELIST_NAME:-monad_whitelist}"

echo "Show whitelist"
sudo ipset list ${WHITELIST_NAME}

read -p "Do you wish to activate the whitelist? [y/n] " ans
case $ans in
	[Yy]* )
		sudo iptables -I INPUT -p udp --dport ${PORT} -m set --match-set ${WHITELIST_NAME} src -j ACCEPT
		sudo iptables -A INPUT -p udp --dport ${PORT} -j DROP
		exit;;
	[Nn]* ) exit;;
	* ) echo "Unexpected answer. Aborting"; exit 1;;
esac
