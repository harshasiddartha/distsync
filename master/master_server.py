import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Dict, List, Tuple

# Ensure project root is importable for generated stubs
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import grpc

import distsync_pb2 as pb
import distsync_pb2_grpc as pbg
from master.hash_ring import ConsistentHashRing


DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB


class Coordinator(pbg.DistSyncServicer):
    def __init__(self, replication: int, metadata_path: Path) -> None:
        self._replication = replication
        self._metadata_path = metadata_path
        self._lock = threading.Lock()
        self._ring = ConsistentHashRing(replication_factor=128)
        self._node_address: Dict[str, str] = {}
        self._node_last_heartbeat: Dict[str, float] = {}
        self._file_index: Dict[str, Dict[str, int]] = {}
        self._load_metadata()

    # Metadata persistence
    def _load_metadata(self) -> None:
        if not self._metadata_path.exists():
            self._save_metadata()
            return
        try:
            data = json.loads(self._metadata_path.read_text())
        except Exception:
            logging.warning("Failed to read metadata, starting fresh")
            data = {}
        nodes = data.get("nodes", {})
        self._file_index = data.get("files", {})
        for node_id, address in nodes.items():
            self._node_address[node_id] = address
            self._ring.add_node(node_id)
            self._node_last_heartbeat[node_id] = time.time()

    def _save_metadata(self) -> None:
        data = {
            "nodes": self._node_address,
            "files": self._file_index,
        }
        self._metadata_path.write_text(json.dumps(data, indent=2))

    # DistSync RPCs
    def RegisterNode(self, request: pb.NodeInfo, context) -> pb.Ack:
        with self._lock:
            self._node_address[request.node_id] = request.address
            self._node_last_heartbeat[request.node_id] = time.time()
            if request.node_id not in self._ring.nodes():
                self._ring.add_node(request.node_id)
            self._save_metadata()
        logging.info("Node registered: %s @ %s", request.node_id, request.address)
        return pb.Ack(success=True, message="registered")

    def Heartbeat(self, request: pb.NodeHeartbeat, context) -> pb.Ack:
        with self._lock:
            if request.node_id in self._node_address:
                self._node_last_heartbeat[request.node_id] = time.time()
                return pb.Ack(success=True, message="ok")
        return pb.Ack(success=False, message="unknown node")

    def UploadFile(self, request_iterator, context) -> pb.UploadStatus:
        # Buffer incoming stream into chunks
        first: pb.FileChunk | None = None
        chunks: List[pb.FileChunk] = []
        for part in request_iterator:
            if first is None:
                first = part
            chunks.append(part)
        if first is None:
            return pb.UploadStatus(message="no data")

        filename = first.filename
        total_chunks = first.total_chunks or len(chunks)

        # Determine placement via consistent hashing with replication
        with self._lock:
            live_nodes = self._live_nodes()
            if not live_nodes:
                return pb.UploadStatus(message="no live nodes")
            chunk_map: Dict[str, int] = {}
            placements: Dict[int, List[str]] = {}
            for chunk in chunks:
                key = f"{filename}:{chunk.chunk_id}"
                node_ids = self._ring.get_nodes_for_key(key, count=self._replication)
                # filter to live
                node_ids = [n for n in node_ids if n in live_nodes]
                if not node_ids:
                    return pb.UploadStatus(message="no live nodes for chunk")
                placements[chunk.chunk_id] = node_ids
                for nid in node_ids:
                    chunk_map_key = f"{nid}:{chunk.chunk_id}"
                    chunk_map[chunk_map_key] = 1

        # Fanout to nodes concurrently
        def store_to_node(node_id: str, chunk: pb.FileChunk) -> Tuple[str, int]:
            address = self._node_address[node_id]
            with grpc.insecure_channel(address) as ch:
                stub = pbg.NodeServiceStub(ch)
                stub.StoreChunk(chunk)
            return (node_id, chunk.chunk_id)

        with futures.ThreadPoolExecutor(max_workers=16) as pool:
            futs = []
            for chunk in chunks:
                node_ids = placements[chunk.chunk_id]
                for nid in node_ids:
                    futs.append(pool.submit(store_to_node, nid, chunk))
            for f in futures.as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    logging.exception("StoreChunk failed: %s", e)

        with self._lock:
            # track minimal file index (store total_chunks under filename)
            self._file_index[filename] = {"total_chunks": total_chunks}
            self._save_metadata()

        return pb.UploadStatus(message="ok")

    def DownloadFile(self, request: pb.FileRequest, context):
        filename = request.filename
        with self._lock:
            info = self._file_index.get(filename)
            if not info:
                context.abort(grpc.StatusCode.NOT_FOUND, "file not found")
            total_chunks = int(info.get("total_chunks", 0))
            live_nodes = self._live_nodes()
            if not live_nodes:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "no live nodes")

        # For each chunk, pick first available replica by ring order
        for cid in range(total_chunks):
            key = f"{filename}:{cid}"
            node_ids = self._ring.get_nodes_for_key(key, count=self._replication)
            sent = False
            for nid in node_ids:
                with self._lock:
                    if nid not in live_nodes:
                        continue
                    address = self._node_address[nid]
                try:
                    with grpc.insecure_channel(address) as ch:
                        stub = pbg.NodeServiceStub(ch)
                        chunk = stub.GetChunk(pb.ChunkRequest(filename=filename, chunk_id=cid))
                        yield chunk
                        sent = True
                        break
                except Exception:
                    logging.warning("Chunk %s:%d unavailable on %s", filename, cid, nid)
                    continue
            if not sent:
                context.abort(grpc.StatusCode.UNAVAILABLE, f"chunk {cid} unavailable")

    # Utilities
    def _live_nodes(self) -> List[str]:
        now = time.time()
        live: List[str] = []
        for nid, ts in self._node_last_heartbeat.items():
            if now - ts <= 10.0:  # 10s heartbeat window
                live.append(nid)
        return live


def chunk_bytes(filename: str, data: bytes, chunk_size: int = DEFAULT_CHUNK_SIZE) -> List[Tuple[int, bytes]]:
    chunks: List[Tuple[int, bytes]] = []
    i = 0
    for off in range(0, len(data), chunk_size):
        chunks.append((i, data[off: off + chunk_size]))
        i += 1
    return chunks


def serve(bind: str, replication: int) -> None:
    logging.basicConfig(level=logging.INFO, format="[master] %(asctime)s %(levelname)s %(message)s")
    meta_path = ROOT / "master" / "metadata.json"
    if not meta_path.exists():
        meta_path.write_text(json.dumps({"nodes": {}, "files": {}}, indent=2))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pbg.add_DistSyncServicer_to_server(Coordinator(replication=replication, metadata_path=meta_path), server)
    server.add_insecure_port(bind)
    server.start()
    logging.info("Master listening on %s (replicas=%d)", bind, replication)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0:50050")
    parser.add_argument("--replicas", type=int, default=2)
    args = parser.parse_args()
    serve(args.bind, args.replicas)


if __name__ == "__main__":
    main()

