## DistSync – Distributed File Synchronization System

DistSync is a lightweight distributed file synchronization prototype showcasing a coordinator (master) and multiple worker nodes using gRPC, consistent hashing, multithreading, replication, and simple fault tolerance.

### Features
- File upload/download via gRPC streaming
- Consistent hashing for chunk placement
- Replication (N=2 by default)
- Simple heartbeat and node liveness tracking
- Multithreaded replication fanout
- Basic benchmarking for sequential vs distributed

### Project Structure

```
DistSync/
├── master/
│   ├── master_server.py
│   ├── hash_ring.py
│   ├── metadata.json
├── node/
│   ├── node_server.py
│   ├── storage/
├── client/
│   ├── client.py
├── protos/
│   ├── distsync.proto
├── tests/
│   ├── test_hash_ring.py
│   ├── test_integration.py
├── requirements.txt
├── README.md
└── run_demo.sh
```

### Quickstart

> Important: Use Python 3.10–3.12. grpcio/grpcio-tools wheels for Python 3.14 on macOS ARM may not be available yet and will try to compile from source (slow and often fails).

1. Install dependencies:
```bash
# Recommended: use pyenv to install Python 3.12 (or 3.11):
# brew install pyenv
# pyenv install 3.12.6
# pyenv local 3.12.6

python -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
```

2. Generate gRPC stubs:
```bash
bash run_demo.sh gen
```

3. Start master and nodes (in separate shells) or use demo:
```bash
bash run_demo.sh up
```

4. Upload a file:
```bash
python client/client.py upload --master localhost:50050 --file sample.bin
```

5. Download the file:
```bash
python client/client.py download --master localhost:50050 --file sample.bin --out downloaded.bin
```

### Benchmarks
Use the built-in benchmark mode in the client to compare sequential vs distributed for a given file:
```bash
python client/client.py benchmark --master localhost:50050 --file sample.bin
```

### Sample run: Inputs and Outputs

1) Create a sample file and upload
```bash
python - <<'PY'
with open("sample.bin","wb") as f: f.write(b"A"*10_000_000)
PY
python client/client.py upload --master localhost:50050 --file sample.bin
# Output
ok
```

2) Download and verify integrity
```bash
python client/client.py download --master localhost:50050 --file sample.bin --out downloaded.bin
# Output
downloaded -> downloaded.bin

python - <<'PY'
import hashlib, pathlib as p
h=lambda x: hashlib.sha256(p.Path(x).read_bytes()).hexdigest()
print("orig:", h("sample.bin"))
print("down:", h("downloaded.bin"))
PY
# Example Output
orig: 2e9d76efe0bae3ce8ff4f8d7da83aef7203b65759c11d547f8718e32d9a22269
down: 2e9d76efe0bae3ce8ff4f8d7da83aef7203b65759c11d547f8718e32d9a22269
```

3) Benchmark
```bash
python client/client.py benchmark --master localhost:50050 --file sample.bin
# Example Output
ok
ok

Benchmark
Mode,Time (s)
Sequential-like,0.018
DistSync,0.017
```

4) Simulate node failure (replication works)
```bash
kill $(cat .node-a.pid)
python client/client.py download --master localhost:50050 --file sample.bin --out downloaded_after_failure.bin
# Output
downloaded -> downloaded_after_failure.bin
```

### Troubleshooting (macOS arm64, Python 3.14)
- If you see grpcio/grpcio-tools failing to build wheels (long clang++ logs), switch to Python 3.11 or 3.12 using pyenv (see Quickstart). Then reinstall deps in a fresh virtualenv.
- If you must stay on Python 3.14, you can try pre-releases (not guaranteed):
```bash
python -m pip install --pre grpcio grpcio-tools
```
But the recommended fix is to use Python 3.10–3.12.

- Protobuf runtime warning: If you see a runtime/gencode mismatch warning, align to 5.27.2 and regenerate stubs:
```bash
pip install --force-reinstall 'protobuf==5.27.2'
bash run_demo.sh gen
```

### Notes
- This is a local, educational prototype. Data is stored in `node/storage/` folders per node.
- Metadata is persisted in `master/metadata.json`.
- Heartbeats mark node liveness; master avoids routing to non-live nodes.

### License
MIT


