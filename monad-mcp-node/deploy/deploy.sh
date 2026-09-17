#!/usr/bin/env bash
# deploy.sh: ships the binary, the pruner and the user units, and
# enables them. Starting is netctl.sh's job: a start mints a genesis.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[ -f "$dist_dir/VERSION" ] || die "no $dist_dir/VERSION; run build.sh first"
binary=$(sed -n 's/^binary=//p' "$dist_dir/VERSION")
[ -n "$binary" ] && [ -f "$dist_dir/$binary" ] || die "no $dist_dir/$binary; run build.sh first"

deploy_host() {
    local host=$1
    rssh "$host" "mkdir -p $remote_root/bin $remote_root/config $remote_root/run \
        $remote_root/logs $remote_root/ledger/blocks \$HOME/.config/systemd/user"

    rsync_to "$host" "$dist_dir/$binary" "$remote_rel/bin/$binary"
    rsync_to "$host" "$deploy_dir/cruft.sh" "$remote_rel/cruft.sh"
    rsync_to "$host" "$deploy_dir/run.sh" "$remote_rel/run.sh"
    rssh "$host" "test -f $remote_root/cruft.env" \
        || rsync_to "$host" "$deploy_dir/cruft.env" "$remote_rel/cruft.env"

    local systemd_dir=.config/systemd/user
    rsync_to "$host" "$deploy_dir/$unit.service" "$systemd_dir/$unit.service"
    rsync_to "$host" "$deploy_dir/$cruft_unit.service" "$systemd_dir/$cruft_unit.service"
    rsync_to "$host" "$deploy_dir/$cruft_unit.timer" "$systemd_dir/$cruft_unit.timer"

    # the symlink swap is the only step the node would notice mid-flight
    rssh "$host" "set -e
        chmod 755 $remote_root/bin/$binary $remote_root/cruft.sh $remote_root/run.sh
        ln -sfn $binary $remote_root/bin/.current.new
        mv -T $remote_root/bin/.current.new $remote_root/bin/current
        echo \"current -> \$(readlink $remote_root/bin/current)\""

    if [ "$supervisor" = systemd ]; then
        rssh "$host" "loginctl enable-linger \$(id -un)"
        rssh_user_systemd "$host" "set -e
            systemctl --user daemon-reload
            systemctl --user enable $unit.service
            systemctl --user enable --now $cruft_unit.timer
            echo \"unit=\$(systemctl --user is-enabled $unit.service) cruft-timer=\$(systemctl --user is-active $cruft_unit.timer)\""
    else
        echo "supervisor=setsid: units shipped but not enabled, launcher is $remote_root/run.sh"
    fi
}

echo "deploying $binary to $(host_count) hosts (supervisor=$supervisor)"
fanout deploy_host
