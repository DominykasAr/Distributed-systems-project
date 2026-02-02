import hashlib
from bisect import bisect_right
from dataclasses import dataclass
from typing import Dict, List, Tuple

def _h(s: str) -> int:
    # Return a 32-bit hash of the input string.
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:8], 16)

@dataclass(frozen=True)
class RingNode:
    base_url: str

# Consistent hashing ring implementation with virtual nodes
class ConsistentHashRing:
    def __init__(self, nodes: List[str], vnodes: int = 50):
        self.vnodes = max(1, vnodes)
        self._ring: List[Tuple[int, str]] = []
        self._nodes = sorted(set(nodes))
        self._build()

    def _build(self):
        ring: List[Tuple[int, str]] = []
        for n in self._nodes:
            for i in range(self.vnodes):
                ring.append((_h(f"{n}#{i}"), n))
        ring.sort(key=lambda x: x[0])
        self._ring = ring

    @property
    def nodes(self) -> List[str]:
        return list(self._nodes)

    def set_nodes(self, nodes: List[str]) -> None:
        self._nodes = sorted(set(nodes))
        self._build()

    def owner(self, key: str) -> str:
        if not self._ring:
            raise RuntimeError("Ring has no nodes")
        key_h = _h(key)
        idx = bisect_right(self._ring, (key_h, chr(0x10FFFF)))
        if idx == len(self._ring):
            idx = 0
        return self._ring[idx][1]

    # Return up to r distinct node URLs (clockwise) starting at owner.
    def replicas(self, key: str, r: int) -> List[str]:
        if not self._ring:
            raise RuntimeError("Ring has no nodes")
        r = max(1, r)
        key_h = _h(key)
        idx = bisect_right(self._ring, (key_h, chr(0x10FFFF)))
        if idx == len(self._ring):
            idx = 0

        seen = set()
        out = []
        i = idx
        while len(out) < min(r, len(self._nodes)):
            node = self._ring[i][1]
            if node not in seen:
                seen.add(node)
                out.append(node)
            i = (i + 1) % len(self._ring)
        return out