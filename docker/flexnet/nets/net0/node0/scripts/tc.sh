set -x
# IFB
ip link set dev eth0 mtu 1500
ip link add ifb0 type ifb
ip link set dev ifb0 up
tc qdisc add dev eth0 handle ffff: ingress
tc filter add dev eth0 parent ffff: u32 match ip src 0/0 action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root handle 1: htb

# RESOLVE HOSTS
node0_us=$(getent hosts node0_us | awk '{ print $1 }' )
node1_eu=$(getent hosts node1_eu | awk '{ print $1 }' )
node2_eu=$(getent hosts node2_eu | awk '{ print $1 }' )

# INBOUND
tc class add dev ifb0 parent 1: classid 1:1 htb rate 100mbit
tc filter add dev ifb0 protocol ip parent 1: prio 0 u32 match ip src 0/0 flowid 1:1
tc class add dev ifb0 parent 1:1 classid 1:2 htb rate 50.00mbit ceil 100mbit
tc filter add dev ifb0 protocol ip parent 1:1 prio 1 u32 match ip src $node1_eu flowid 1:2
tc qdisc add dev ifb0 parent 1:2 handle 2: netem delay 50ms limit 2343
tc class add dev ifb0 parent 1:1 classid 1:3 htb rate 50.00mbit ceil 100mbit
tc filter add dev ifb0 protocol ip parent 1:1 prio 1 u32 match ip src $node2_eu flowid 1:3
tc qdisc add dev ifb0 parent 1:3 handle 3: netem delay 50ms limit 2343

# OUTBOUND
tc qdisc add dev eth0 root handle 1: htb
tc class add dev eth0 parent 1: classid 1:1 htb rate 100mbit
tc filter add dev eth0 protocol ip parent 1: prio 0 u32 match ip dst 0/0 flowid 1:1
