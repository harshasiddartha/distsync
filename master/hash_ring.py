import hashlib
from bisect import bisect_right
from typing import Dict, List, Tuple


class ConsistentHashRing:
    def __init__(self, replication_factor: int = 100) -> None:
        self._replication_factor = replication_factor
        self._ring: List[Tuple[int, str]] = []
        self._nodes: Dict[str, List[int]] = {}

    @staticmethod
    def _hash_key(key: str) -> int:
        return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        points: List[int] = []
        for i in range(self._replication_factor):
            h = self._hash_key(f"{node_id}:{i}")
            points.append(h)
            self._ring.append((h, node_id))
        self._ring.sort(key=lambda x: x[0])
        self._nodes[node_id] = points

    def remove_node(self, node_id: str) -> None:
        points = self._nodes.pop(node_id, None)
        if points is None:
            return
        point_set = set(points)
        self._ring = [(h, n) for (h, n) in self._ring if not (n == node_id and h in point_set)]

    def get_nodes_for_key(self, key: str, count: int = 1) -> List[str]:
        if not self._ring:
            return []
        key_hash = self._hash_key(key)
        idx = bisect_right(self._ring, (key_hash, ""))
        selected: List[str] = []
        seen = set()
        i = idx
        while len(selected) < min(count, len(self._nodes)):
            pos = i % len(self._ring)
            node_id = self._ring[pos][1]
            if node_id not in seen:
                seen.add(node_id)
                selected.append(node_id)
            i += 1
        return selected

    def nodes(self) -> List[str]:
        return list(self._nodes.keys())

