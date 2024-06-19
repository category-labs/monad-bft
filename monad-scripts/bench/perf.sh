#!/bin/bash
set +x

if [ -f "ctl_fd.fifo" ]; then
  rm "ctl_fd.fifo"
fi

if [ -f "ctl_fd_ack.fifo" ]; then
  rm "ctl_fd_ack.fifo"
fi

mkfifo ctl_fd.fifo
exec {ctl_fd}<>ctl_fd.fifo
mkfifo ctl_fd_ack.fifo
exec {ctl_fd_ack}<>ctl_fd_ack.fifo


PERF_CTL_FD=$ctl_fd PERF_CTL_FD_ACK=$ctl_fd_ack RUSTFLAGS="-C force-frame-pointers=yes" perf record --delay=-1 -F 12000 --control fd:${ctl_fd},${ctl_fd_ack} --call-graph dwarf,16384 -- cargo bench --bench $1 -- --profile-time 10

unlink ctl_fd.fifo
unlink ctl_fd_ack.fifo
