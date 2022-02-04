#!/bin/sh

set -eu

# Remove keepalived pid files if one exists on container startup.
rm -rf /run/keepalived.pid
rm -rf /run/keepalived_vrrp.pid

# In the background
keepalived --dont-fork --dump-conf --log-console --log-detail --log-facility 7 --vrrp -f /usr/local/etc/keepalived/keepalived.conf --pid /run/keepalived.pid --vrrp_pid /run/keepalived_vrrp.pid &

# -W master-worker mode (similar to the old "haproxy-systemd-wrapper"; allows for reload via "SIGUSR2")
# -db disables background mode
haproxy -W -db -f /usr/local/etc/haproxy/haproxy.cfg

