#!/bin/bash
# NOTE: logs are written to testground_logs/all_nodes.log

set -ex

usage() {
    echo "USAGE:"
    echo "-l to set network interface to loopback"
    echo "-t to generate pcap files for packets sent/received by each node"
    echo "-n to set number of nodes per region. Default is 5,0,0,0,0"
    echo "-s to set simulation length (in seconds). Default value is 60"
    echo "-p to set proposal size (in bytes). Default value is 2000000 (2MB)"
    echo "-d to set rust log level. Default is INFO"
    exit 1;
}

interface='ifb'
run_tcpdump=false
node_counts='5,0,0,0,0'
sim_len=60 # seconds
prop_size=2000000 # bytes
log_level=INFO
while getopts 'ltn:s:p:d:' flag; do
    case $flag in
        l)
            interface='lo' ;;
        t)
            run_tcpdump=true ;;
        n)
            node_counts=$OPTARG ;;
        s)
            sim_len=$OPTARG ;;
        p)
            prop_size=$OPTARG ;;
        d)
            log_level=$OPTARG ;;
        *)
            usage ;;
    esac
done

node_counts_array=($(echo $node_counts | tr "," "\n"))
if [ ${#node_counts_array[@]} -ne 5 ]; then
    echo "expected 5 regions"; exit 1;
fi

python3 topology-gen.py topo.json $node_counts
python3 tc-gen.py topo.json tc.sh addresses

mkdir -p testground_logs

if [ "$interface" = "ifb" ]; then
    source tc.sh
fi

if [ "$run_tcpdump" = true ]; then
    addrs=$(cat addresses | tr " " "\n")
    index=0
    for addr in $addrs
    do
        # needed for dissecting packets with tshark
        interface=ifb${index}
        ethtool -K $interface tx-udp-segmentation off

        ip=${addr%:*}
        echo "starting tcpdump for address" $addr
        # buffer size is 16384 for 4MB proposal blocks
        tcpdump -i $interface -w testground_logs/node_${index + 1}.pcap -s 100000 -B 16384 host ${ip} &
        tcpdump_pids+=($!)
        ((index++))
    done

    # wait for tcpdump to start
    sleep 5
fi

echo "starting testground"
RUST_LOG=$log_level monad-testground --simulation-length $sim_len --addresses $(<addresses) --proposal-size $prop_size > testground_logs/all_nodes.log
echo "testground ended"

if [ "$run_tcpdump" = true ]; then
    # to avoid packet capture loss
    sleep 5

    for tcpdump_pid in ${tcpdump_pids[@]}
    do
        echo "killing" $tcpdump_pid
        kill $tcpdump_pid
    done

    # to avoid packet capture loss
    sleep 5
fi
