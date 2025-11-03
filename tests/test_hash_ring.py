from master.hash_ring import ConsistentHashRing


def test_hash_ring_basic_distribution():
    ring = ConsistentHashRing(replication_factor=16)
    ring.add_node("a")
    ring.add_node("b")
    ring.add_node("c")

    keys = [f"file-{i}" for i in range(100)]
    placements = [tuple(ring.get_nodes_for_key(k, count=2)) for k in keys]

    # Ensure we always get 2 nodes and they are among added nodes
    for p in placements:
        assert len(p) == 2
        for nid in p:
            assert nid in set(["a", "b", "c"])  # noqa: PLR2004

