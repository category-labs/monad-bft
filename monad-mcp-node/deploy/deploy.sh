#!/usr/bin/env bash
# deploy.sh [--stage] [host...]: ships the node and rpc binaries, the pruner
# and the user units, and enables them, to every host in hosts.txt or to the
# hosts given. --stage leaves bin/{current,rpc-current} alone; netctl.sh
# live-upgrade swaps them per host.
# Starting is netctl.sh's job: a start mints a genesis.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

swap=yes
if [ "${1:-}" = --stage ]; then
    swap=no
    shift
fi
[ $# = 0 ] || set_targets "$@"

read -r binary rpc_binary <<< "$(dist_binaries)"

# a local mirror of the remote layout, shipped in one rsync per host.
# cruft.env travels as cruft.env.dist so an existing one is never clobbered.
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
systemd_dir=.config/systemd/user
mkdir -p "$stage/$remote_rel"/{bin,config,run,logs,ledger/blocks} "$stage/$systemd_dir"
cp "$dist_dir/$binary" "$dist_dir/$rpc_binary" "$stage/$remote_rel/bin/"
cp "$deploy_dir"/{cruft.sh,run.sh} "$stage/$remote_rel/"
cp "$deploy_dir/cruft.env" "$stage/$remote_rel/cruft.env.dist"
cp "$deploy_dir/$unit.service" "$deploy_dir/$rpc_unit.service" "$deploy_dir/$cruft_unit".{service,timer} \
    "$stage/$systemd_dir/"
chmod -R u=rwX,go=rX "$stage"
chmod 755 "$stage/$remote_rel"/{bin/"$binary",bin/"$rpc_binary",cruft.sh,run.sh}

deploy_host() {
    local host=$1
    rsync_tree_to "$host" "$stage" "$remote_rel" "$systemd_dir"

    local post="set -e
        cd $remote_root
        [ -f cruft.env ] || cp cruft.env.dist cruft.env"
    if [ "$swap" = yes ]; then
        post+="
        $(swap_cmd "$binary")
        $(swap_cmd "$rpc_binary" rpc-current)"
    else
        post+="
        echo \"staged $binary $rpc_binary, current -> \$(readlink bin/current || echo none)\""
    fi

    if [ "$supervisor" = systemd ]; then
        rssh_user_systemd "$host" "$post
            loginctl enable-linger \$(id -un)
            systemctl --user daemon-reload
            systemctl --user enable $unit.service $rpc_unit.service
            systemctl --user enable --now $cruft_unit.timer
            echo \"unit=\$(systemctl --user is-enabled $unit.service) rpc=\$(systemctl --user is-enabled $rpc_unit.service) cruft-timer=\$(systemctl --user is-active $cruft_unit.timer)\""
    else
        rssh "$host" "$post"
        echo "supervisor=setsid: units shipped but not enabled, launcher is $remote_root/run.sh; no rpc"
    fi
}

echo "deploying $binary + $rpc_binary to $(targets | xargs) (supervisor=$supervisor, swap=$swap)"
fanout deploy_host
