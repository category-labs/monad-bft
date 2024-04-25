#!/bin/bash
set +x
mkfifo ctl_fd.fifo
exec {ctl_fd}<>ctl_fd.fifo
mkfifo ctl_fd_ack.fifo
exec {ctl_fd_ack}<>ctl_fd_ack.fifo


PERF_CTL_FD=$ctl_fd PERF_CTL_FD_ACK=$ctl_fd_ack RUSTFLAGS="-C force-frame-pointers=yes" perf record --delay=-1 -F 12000 --control fd:${ctl_fd},${ctl_fd_ack} --call-graph dwarf,16384 -- cargo bench --bench proposal_bench -- --profile-time 10

unlink ctl_fd.fifo
unlink ctl_fd_ack.fifo
