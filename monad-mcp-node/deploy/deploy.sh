#!/usr/bin/env bash
# deploy.sh [host]: ships the binary, the pruner and the user units, and
# enables them, to every host in hosts.txt or to the one host given.
# Starting is netctl.sh's job: a start mints a genesis.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[ $# -le 1 ] || die "usage: deploy.sh [host]"
only_host=${1:-}
[ -z "$only_host" ] || node_id_of "$only_host" > /dev/null

[ -f "$dist_dir/VERSION" ] || die "no $dist_dir/VERSION; run build.sh first"
binary=$(sed -n 's/^binary=//p' "$dist_dir/VERSION")
[ -n "$binary" ] && [ -f "$dist_dir/$binary" ] || die "no $dist_dir/$binary; run build.sh first"

# a local mirror of the remote layout, shipped in one rsync per host.
# cruft.env travels as cruft.env.dist so an existing one is never clobbered.
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
systemd_dir=.config/systemd/user
mkdir -p "$stage/$remote_rel"/{bin,config,run,logs,ledger/blocks} "$stage/$systemd_dir"
cp "$dist_dir/$binary" "$stage/$remote_rel/bin/"
cp "$deploy_dir"/{cruft.sh,run.sh} "$stage/$remote_rel/"
cp "$deploy_dir/cruft.env" "$stage/$remote_rel/cruft.env.dist"
cp "$deploy_dir/$unit.service" "$deploy_dir/$cruft_unit".{service,timer} "$stage/$systemd_dir/"
chmod -R u=rwX,go=rX "$stage"
chmod 755 "$stage/$remote_rel"/{bin/"$binary",cruft.sh,run.sh}

deploy_host() {
    local host=$1
    rsync_tree_to "$host" "$stage" "$remote_rel" "$systemd_dir"

    # the symlink swap is the only step the node would notice mid-flight
    local post="set -e
        cd $remote_root
        [ -f cruft.env ] || cp cruft.env.dist cruft.env
        ln -sfn $binary bin/.current.new
        mv -T bin/.current.new bin/current
        echo \"current -> \$(readlink bin/current)\""

    if [ "$supervisor" = systemd ]; then
        rssh_user_systemd "$host" "$post
            loginctl enable-linger \$(id -un)
            systemctl --user daemon-reload
            systemctl --user enable $unit.service
            systemctl --user enable --now $cruft_unit.timer
            echo \"unit=\$(systemctl --user is-enabled $unit.service) cruft-timer=\$(systemctl --user is-active $cruft_unit.timer)\""
    else
        rssh "$host" "$post"
        echo "supervisor=setsid: units shipped but not enabled, launcher is $remote_root/run.sh"
    fi
}

if [ -n "$only_host" ]; then
    echo "deploying $binary to $only_host (supervisor=$supervisor)"
    deploy_host "$only_host"
else
    echo "deploying $binary to $(host_count) hosts (supervisor=$supervisor)"
    fanout deploy_host
fi
