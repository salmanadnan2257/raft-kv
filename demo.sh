#!/usr/bin/env bash
# raftkv demo: start a 3-node cluster, write data, kill -9 the leader,
# watch failover, verify no data loss, stop the cluster.
#
# Requires `raftkv` on PATH (pip install -e .).

set -euo pipefail

HOME_DIR="$(mktemp -d /tmp/raftkv-demo.XXXXXX)"
export RAFTKV_HOME="$HOME_DIR"
trap 'raftkv cluster stop >/dev/null 2>&1 || true; rm -rf "$HOME_DIR"' EXIT

step() { printf '\n$ %s\n' "$*"; "$@"; }

echo "=== raftkv demo (cluster state in $HOME_DIR) ==="

step raftkv cluster start -n 3
step raftkv cluster status

step raftkv put city lahore
step raftkv put team raptors
step raftkv cas team raptors dinos
step raftkv get city
step raftkv get team

LEADER_ID=$(raftkv cluster status | awk '/role=leader/ {print $1}')
LEADER_PID=$(raftkv cluster status | awk '/role=leader/ {sub("pid=","",$2); print $2}')
echo
echo "=== killing leader $LEADER_ID (pid $LEADER_PID) with SIGKILL ==="
kill -9 "$LEADER_PID"
sleep 2

step raftkv cluster status
echo
echo "=== data survives, writes keep working ==="
step raftkv get city
step raftkv get team
step raftkv put after-failover yes
step raftkv get after-failover

step raftkv cluster stop
echo
echo "=== demo complete: leader killed, failover elected, no data lost ==="
