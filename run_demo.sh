#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROTO_DIR="$ROOT_DIR/protos"
OUT_DIR="$ROOT_DIR"

gen() {
  python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/distsync.proto"
  echo "Generated gRPC stubs into $OUT_DIR"
}

up() {
  # Start master
  (python master/master_server.py --bind 0.0.0.0:50050 --replicas 2 & echo $! > .master.pid) || true
  sleep 0.5
  # Start three nodes on different ports
  (python node/node_server.py --node-id node-a --bind 127.0.0.1:50051 --master 127.0.0.1:50050 & echo $! > .node-a.pid) || true
  (python node/node_server.py --node-id node-b --bind 127.0.0.1:50052 --master 127.0.0.1:50050 & echo $! > .node-b.pid) || true
  (python node/node_server.py --node-id node-c --bind 127.0.0.1:50053 --master 127.0.0.1:50050 & echo $! > .node-c.pid) || true
  echo "Master and nodes started. PIDs recorded."
}

down() {
  for f in .master.pid .node-a.pid .node-b.pid .node-c.pid; do
    if [ -f "$f" ]; then
      kill "$(cat "$f")" 2>/dev/null || true
      rm -f "$f"
    fi
  done
  echo "Stopped master and nodes."
}

case "${1:-}" in
  gen) gen ;;
  up) gen; up ;;
  down) down ;;
  *) echo "Usage: $0 {gen|up|down}" && exit 1 ;;
 esac

