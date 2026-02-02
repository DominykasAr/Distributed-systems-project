from dynamo.hashing import ConsistentHashRing

def test_owner_is_stable():
    nodes = ["n1", "n2", "n3"]
    ring = ConsistentHashRing(nodes, vnodes=10)

    key = "example-key"
    owner1 = ring.owner(key)
    owner2 = ring.owner(key)

    assert owner1 == owner2


def test_replicas_unique_and_limited():
    nodes = ["n1", "n2", "n3"]
    ring = ConsistentHashRing(nodes, vnodes=10)

    reps = ring.replicas("k", r=2)

    assert len(reps) == 2
    assert len(set(reps)) == 2
    assert all(r in nodes for r in reps)


def test_rebalance_moves_subset_only():
    nodes1 = ["n1", "n2", "n3"]
    nodes2 = ["n1", "n2", "n3", "n4"]

    ring1 = ConsistentHashRing(nodes1, vnodes=20)
    ring2 = ConsistentHashRing(nodes2, vnodes=20)

    keys = [f"k{i}" for i in range(200)]

    moved = 0
    for k in keys:
        if ring1.owner(k) != ring2.owner(k):
            moved += 1

    assert moved < len(keys)
    assert moved > 0
