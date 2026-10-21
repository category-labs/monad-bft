#!/usr/bin/env bash
# deploy.sh [--stage] [host...]: ships the pq node, rpc and mcp-tx binaries, the
# pruner and the user units to every host in hosts.txt or to the hosts given;
# the explorer too where config/<host>/explorer.env exists, and
# config/latency.toml when present. Touches only ~/monad-pq and the monad-pq-*
# units: the monad-mcp-* stack is left as it is.
# The node, rpc and explorer units are installed but not enabled, so a reboot
# brings back the mcp stack, not two nodes racing for the port; only the
# pruner timer is enabled.
# --stage leaves bin/{current,rpc-current,explorer-current,mcp-tx} alone;
# netctl.sh live-upgrade swaps them per host.
# Starting is netctl.sh's job: a start mints a genesis.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

swap=yes
if [ "${1:-}" = --stage ]; then
    swap=no
    shift
fi
[ $# = 0 ] || set_targets "$@"

read -r binary rpc_binary explorer_binary tx_binary <<< "$(dist_binaries)"

# a local mirror of the remote layout, shipped in one rsync per host.
# cruft.env travels as cruft.env.dist so an existing one is never clobbered.
stage=$(mktemp -d)
explorer_stage=$(mktemp -d)
trap 'rm -rf "$stage" "$explorer_stage"' EXIT
systemd_dir=.config/systemd/user
mkdir -p "$stage/$remote_rel"/{bin,config,run,logs,ledger/blocks,ledger/evidence} "$stage/$systemd_dir"
cp "$dist_dir/$binary" "$dist_dir/$rpc_binary" "$dist_dir/$tx_binary" "$stage/$remote_rel/bin/"
cp "$deploy_dir"/{cruft.sh,run.sh} "$stage/$remote_rel/"
cp "$deploy_dir/cruft.env" "$stage/$remote_rel/cruft.env.dist"
if have_latency; then
    cp "$latency_file" "$stage/$remote_rel/config/latency.toml"
else
    echo "no $latency_file: latency.toml not shipped"
fi
cp "$deploy_dir/$unit.service" "$deploy_dir/$rpc_unit.service" "$deploy_dir/$cruft_unit".{service,timer} \
    "$deploy_dir/monad-pq.slice" "$stage/$systemd_dir/"
chmod -R u=rwX,go=rX "$stage"
chmod 755 "$stage/$remote_rel"/{bin/"$binary",bin/"$rpc_binary",bin/"$tx_binary",cruft.sh,run.sh}

# the explorer ships only to hosts with config/<host>/explorer.env
mkdir -p "$explorer_stage/$remote_rel/bin" "$explorer_stage/$systemd_dir"
cp "$dist_dir/$explorer_binary" "$explorer_stage/$remote_rel/bin/"
cp "$deploy_dir/$explorer_unit.service" "$explorer_stage/$systemd_dir/"
chmod -R u=rwX,go=rX "$explorer_stage"
chmod 755 "$explorer_stage/$remote_rel/bin/$explorer_binary"

deploy_host() {
    local host=$1 explorer=no
    has_explorer "$host" && explorer=yes
    rsync_tree_to "$host" "$stage" "$remote_rel" "$systemd_dir"
    [ "$explorer" = no ] || rsync_tree_to "$host" "$explorer_stage" "$remote_rel" "$systemd_dir"

    local post="set -e
        cd $remote_root
        [ -f cruft.env ] || cp cruft.env.dist cruft.env"
    if [ "$swap" = yes ]; then
        post+="
        $(swap_cmd "$binary")
        $(swap_cmd "$rpc_binary" rpc-current)
        $(swap_cmd "$tx_binary" mcp-tx)"
        [ "$explorer" = no ] || post+="
        $(swap_cmd "$explorer_binary" explorer-current)"
    else
        post+="
        echo \"staged $binary $rpc_binary $tx_binary, current -> \$(readlink bin/current || echo none)\""
    fi

    if [ "$supervisor" = systemd ]; then
        rssh_user_systemd "$host" "$post
            loginctl enable-linger \$(id -un)
            systemctl --user daemon-reload
            systemctl --user enable --now $cruft_unit.timer
            echo \"unit=\$(systemctl --user is-enabled $unit.service || true) cruft-timer=\$(systemctl --user is-active $cruft_unit.timer)\""
    else
        rssh "$host" "$post"
        echo "supervisor=setsid: units shipped but not enabled, launcher is $remote_root/run.sh; no rpc"
    fi
}

echo "deploying $binary + $rpc_binary + $tx_binary to $(targets | xargs), $explorer_binary to those with config/<host>/explorer.env (supervisor=$supervisor, swap=$swap)"
fanout deploy_host
