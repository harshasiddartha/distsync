import argparse
import logging
import os
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

# Ensure project root is importable for generated stubs
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import grpc

import distsync_pb2 as pb
import distsync_pb2_grpc as pbg


class NodeServicer(pbg.NodeServiceServicer):
    def __init__(self, storage_dir: Path) -> None:
        self._storage_dir = storage_dir
        self._lock = threading.Lock()
        self._storage_dir.mkdir(parents=True, exist_ok=True)

    def _chunk_path(self, filename: str, chunk_id: int) -> Path:
        safe_name = filename.replace("/", "_")
        return self._storage_dir / f"{safe_name}.{chunk_id}.chunk"

    def StoreChunk(self, request: pb.FileChunk, context) -> pb.Ack:
        p = self._chunk_path(request.filename, request.chunk_id)
        with self._lock:
            with open(p, "wb") as f:
                f.write(request.data)
        return pb.Ack(success=True, message="stored")

    def GetChunk(self, request: pb.ChunkRequest, context) -> pb.FileChunk:
        p = self._chunk_path(request.filename, request.chunk_id)
        if not p.exists():
            context.abort(grpc.StatusCode.NOT_FOUND, "chunk not found")
        data = p.read_bytes()
        return pb.FileChunk(filename=request.filename, data=data, chunk_id=request.chunk_id, total_chunks=0)


def start_heartbeat(master_addr: str, node_id: str, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            with grpc.insecure_channel(master_addr) as ch:
                stub = pbg.DistSyncStub(ch)
                stub.Heartbeat(pb.NodeHeartbeat(node_id=node_id))
        except Exception:
            pass
        stop_event.wait(3.0)


def serve(node_id: str, bind: str, master_addr: str) -> None:
    logging.basicConfig(level=logging.INFO, format=f"[node {node_id}] %(asctime)s %(levelname)s %(message)s")

    storage_dir = ROOT / "node" / "storage" / node_id
    storage_dir.mkdir(parents=True, exist_ok=True)

    # Register with master (retry to handle master startup races)
    register_deadline = time.time() + 30.0
    last_error: Exception | None = None
    while time.time() < register_deadline:
        try:
            with grpc.insecure_channel(master_addr) as ch:
                stub = pbg.DistSyncStub(ch)
                ack = stub.RegisterNode(pb.NodeInfo(node_id=node_id, address=bind))
                if ack.success:
                    break
                last_error = RuntimeError(f"Failed to register node: {ack.message}")
        except Exception as e:
            last_error = e
        time.sleep(1.0)
    if time.time() >= register_deadline and last_error is not None:
        raise last_error

    # Serve node service
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pbg.add_NodeServiceServicer_to_server(NodeServicer(storage_dir=storage_dir), server)
    server.add_insecure_port(bind)
    server.start()
    logging.info("Node listening on %s (master=%s)", bind, master_addr)

    # Heartbeat thread
    stop_event = threading.Event()
    hb_thread = threading.Thread(target=start_heartbeat, args=(master_addr, node_id, stop_event), daemon=True)
    hb_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        server.stop(0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--bind", default="127.0.0.1:50051")
    parser.add_argument("--master", default="127.0.0.1:50050")
    args = parser.parse_args()
    serve(args.node_id, args.bind, args.master)


if __name__ == "__main__":
    main()

