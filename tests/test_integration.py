import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_proto_exists():
    assert (ROOT / "protos" / "distsync.proto").exists()

