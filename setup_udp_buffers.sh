#!/bin/bash

# Set UDP buffer sizes to 64MB (67108864 bytes)
BUFFER_SIZE=67108864

echo "Setting UDP buffer sizes to 64MB..."

# Set the maximum receive buffer size
sudo sysctl -w net.core.rmem_max=$BUFFER_SIZE
sudo sysctl -w net.core.rmem_default=$BUFFER_SIZE

# Set the maximum send buffer size
sudo sysctl -w net.core.wmem_max=$BUFFER_SIZE
sudo sysctl -w net.core.wmem_default=$BUFFER_SIZE

# Also increase netdev_max_backlog for high throughput
sudo sysctl -w net.core.netdev_max_backlog=5000

# Optional: Set UDP specific memory limits (min, default, max)
sudo sysctl -w net.ipv4.udp_mem="12582912 16777216 25165824"

# Verify the settings
echo ""
echo "Current UDP buffer settings:"
echo "rmem_max: $(sysctl -n net.core.rmem_max)"
echo "rmem_default: $(sysctl -n net.core.rmem_default)"
echo "wmem_max: $(sysctl -n net.core.wmem_max)"
echo "wmem_default: $(sysctl -n net.core.wmem_default)"
echo "netdev_max_backlog: $(sysctl -n net.core.netdev_max_backlog)"

# Make settings persistent across reboots
echo ""
echo "To make these settings persistent, add the following to /etc/sysctl.conf:"
echo "net.core.rmem_max = $BUFFER_SIZE"
echo "net.core.rmem_default = $BUFFER_SIZE"
echo "net.core.wmem_max = $BUFFER_SIZE"
echo "net.core.wmem_default = $BUFFER_SIZE"
echo "net.core.netdev_max_backlog = 5000"
echo "net.ipv4.udp_mem = 12582912 16777216 25165824"