set -x
# IFB
ip link set dev eth0 mtu 1500
ip link add ifb0 type ifb
ip link set dev ifb0 up
tc qdisc add dev eth0 handle ffff: ingress
tc filter add dev eth0 parent ffff: u32 match ip src 0/0 action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root handle 1: htb

# RESOLVE HOSTS
node0=$(getent hosts node0 | awk '{ print $1 }' )
node1=$(getent hosts node1 | awk '{ print $1 }' )
node2=$(getent hosts node2 | awk '{ print $1 }' )
node3=$(getent hosts node3 | awk '{ print $1 }' )

# INBOUND
tc class add dev ifb0 parent 1: classid 1:1 htb rate 100mbit
tc filter add dev ifb0 protocol ip parent 1: prio 0 u32 match ip src 0/0 flowid 1:1
tc class add dev ifb0 parent 1:1 classid 1:2 htb rate 33.33mbit ceil 100mbit
tc filter add dev ifb0 protocol ip parent 1:1 prio 1 u32 match ip src $node0 flowid 1:2
tc qdisc add dev ifb0 parent 1:2 handle 2: netem delay 50ms limit 2343
tc class add dev ifb0 parent 1:1 classid 1:3 htb rate 33.33mbit ceil 100mbit
tc filter add dev ifb0 protocol ip parent 1:1 prio 1 u32 match ip src $node1 flowid 1:3
tc qdisc add dev ifb0 parent 1:3 handle 3: netem delay 50ms limit 2343
tc class add dev ifb0 parent 1:1 classid 1:4 htb rate 33.33mbit ceil 100mbit
tc filter add dev ifb0 protocol ip parent 1:1 prio 1 u32 match ip src $node2 flowid 1:4
tc qdisc add dev ifb0 parent 1:4 handle 4: netem delay 10ms limit 468

# OUTBOUND
tc qdisc add dev eth0 root handle 1: htb
tc class add dev eth0 parent 1: classid 1:1 htb rate 100mbit
tc filter add dev eth0 protocol ip parent 1: prio 0 u32 match ip dst 0/0 flowid 1:1
