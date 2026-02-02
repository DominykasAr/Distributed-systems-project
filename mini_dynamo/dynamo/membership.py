import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, List

import httpx

log = logging.getLogger("membership")

@dataclass
class PeerState:
    base_url: str
    last_seen: float
    alive: bool = True

class Membership:
    def __init__(self, self_url: str, peers: List[str], timeout_s: float, dead_after_s: float):
        self.self_url = self_url
        self.timeout_s = timeout_s
        self.dead_after_s = dead_after_s
        now = time.time()
        self._peers: Dict[str, PeerState] = {p: PeerState(p, last_seen=now, alive=True) for p in set(peers) if p != self_url}

    # include self + alive peers
    def all_nodes(self) -> List[str]:
        alive_peers = [p.base_url for p in self._peers.values() if p.alive]
        return sorted(set([self.self_url] + alive_peers))

    def peer_snapshot(self) -> Dict[str, dict]:
        out = {}
        for url, st in self._peers.items():
            out[url] = {"alive": st.alive, "last_seen": st.last_seen}
        return out

    def mark_seen(self, peer_url: str) -> None:
        if peer_url == self.self_url:
            return
        st = self._peers.get(peer_url)
        if st is None:
            st = PeerState(peer_url, last_seen=time.time(), alive=True)
            self._peers[peer_url] = st
        st.last_seen = time.time()
        st.alive = True

    def tick_dead(self) -> None:
        now = time.time()
        for st in self._peers.values():
            if (now - st.last_seen) > self.dead_after_s:
                st.alive = False

    async def heartbeat_loop(self, interval_s: float, self_id: str):
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            while True:
                await asyncio.sleep(interval_s)
                # send heartbeat to all known peers
                for peer_url in list(self._peers.keys()):
                    try:
                        r = await client.post(f"{peer_url}/internal/heartbeat", json={"from": self.self_url, "node_id": self_id})
                        if r.status_code == 200:
                            self.mark_seen(peer_url)
                    except Exception:
                        pass
                self.tick_dead()
