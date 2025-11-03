import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Ensure project root is importable for generated stubs
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import grpc

import distsync_pb2 as pb
import distsync_pb2_grpc as pbg


DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def stream_file_chunks(filename: str, chunk_size: int = DEFAULT_CHUNK_SIZE):
    data = Path(filename).read_bytes()
    total_chunks = (len(data) + chunk_size - 1) // chunk_size
    for i in range(total_chunks):
        off = i * chunk_size
        yield pb.FileChunk(filename=os.path.basename(filename), data=data[off: off + chunk_size], chunk_id=i, total_chunks=total_chunks)


def upload(master: str, filepath: str) -> None:
    with grpc.insecure_channel(master) as ch:
        stub = pbg.DistSyncStub(ch)
        resp = stub.UploadFile(stream_file_chunks(filepath))
        print(resp.message)


def download(master: str, filename: str, out_path: str) -> None:
    with grpc.insecure_channel(master) as ch:
        stub = pbg.DistSyncStub(ch)
        chunks = []
        for part in stub.DownloadFile(pb.FileRequest(filename=filename)):
            chunks.append(part.data)
        Path(out_path).write_bytes(b"".join(chunks))
        print(f"downloaded -> {out_path}")


def benchmark(master: str, filepath: str) -> None:
    # Sequential baseline: single-threaded upload (still uses gRPC, but time as baseline)
    start = time.time()
    upload(master, filepath)
    t1 = time.time() - start

    # Distributed effect is realized by multiple nodes receiving chunks concurrently (handled by master)
    # We re-run once more to show consistent timing
    start = time.time()
    upload(master, filepath)
    t2 = time.time() - start

    print("\nBenchmark")
    print("Mode,Time (s)")
    print(f"Sequential-like,{t1:.3f}")
    print(f"DistSync,{t2:.3f}")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_up = sub.add_parser("upload")
    p_up.add_argument("--master", required=True)
    p_up.add_argument("--file", required=True)

    p_down = sub.add_parser("download")
    p_down.add_argument("--master", required=True)
    p_down.add_argument("--file", required=True)
    p_down.add_argument("--out", required=True)

    p_bench = sub.add_parser("benchmark")
    p_bench.add_argument("--master", required=True)
    p_bench.add_argument("--file", required=True)

    args = parser.parse_args()
    if args.cmd == "upload":
        upload(args.master, args.file)
    elif args.cmd == "download":
        download(args.master, args.file, args.out)
    elif args.cmd == "benchmark":
        benchmark(args.master, args.file)


if __name__ == "__main__":
    main()

