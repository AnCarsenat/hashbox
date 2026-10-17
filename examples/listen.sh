#!/usr/bin/env bash
# Minimal webhook listener: runs a command for every event on a channel.
# Usage: ./listen.sh http://192.168.1.150:8080 my-channel
# Needs curl and jq. Reconnects automatically if the server restarts.
set -u
SERVER="${1:?server URL}"; CHANNEL="${2:?channel}"

while true; do
  curl -sN "$SERVER/listen/$CHANNEL" | while IFS= read -r line; do
    [[ "$line" == data:* ]] || continue
    json="${line#data: }"
    [[ "$json" == "{}" ]] && { echo "connected to $CHANNEL"; continue; }
    # Replace this with whatever should happen on each webhook:
    echo "$json" | jq -c '{time: (.time/1000 | todate), method, path, body}'
  done
  echo "disconnected, retrying in 3s" >&2; sleep 3
done
