import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from .store import Record, InMemoryStore

log = logging.getLogger("quorum")

class QuorumClient:
    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s

    async def _post(self, client: httpx.AsyncClient, url: str, path: str, payload: dict) -> Tuple[str, bool, Optional[dict]]:
        try:
            r = await client.post(f"{url}{path}", json=payload)
            if r.status_code == 200:
                return (url, True, r.json())
            return (url, False, None)
        except Exception:
            return (url, False, None)

    async def _get(self, client: httpx.AsyncClient, url: str, path: str, params: dict) -> Tuple[str, bool, Optional[dict]]:
        try:
            r = await client.get(f"{url}{path}", params=params)
            if r.status_code == 200:
                return (url, True, r.json())
            return (url, False, None)
        except Exception:
            return (url, False, None)

    async def replicate_put(self, replicas: List[str], key: str, value: str, ts: float, w: int) -> Dict[str, Any]:
        w = max(1, w)
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            tasks = [
                self._post(client, url, "/internal/replica/put", {"key": key, "value": value, "ts": ts})
                for url in replicas
            ]
            acks = 0
            results = {}
            for coro in asyncio.as_completed(tasks):
                url, ok, data = await coro
                results[url] = ok
                if ok:
                    acks += 1
                if acks >= w:
                    break
            return {"acks": acks, "results": results, "needed": w}

    async def replicate_delete(self, replicas: List[str], key: str, ts: float, w: int) -> Dict[str, Any]:
        w = max(1, w)
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            tasks = [
                self._post(client, url, "/internal/replica/delete", {"key": key, "ts": ts})
                for url in replicas
            ]
            acks = 0
            results = {}
            for coro in asyncio.as_completed(tasks):
                url, ok, data = await coro
                results[url] = ok
                if ok:
                    acks += 1
                if acks >= w:
                    break
            return {"acks": acks, "results": results, "needed": w}

    async def quorum_get(self, replicas: List[str], key: str, q: int) -> Dict[str, Any]:
        q = max(1, q)
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            tasks = [
                self._get(client, url, "/internal/replica/get", {"key": key})
                for url in replicas
            ]
            oks = 0
            best: Optional[Record] = None
            responses = {}
            for coro in asyncio.as_completed(tasks):
                url, ok, data = await coro
                responses[url] = data if ok else None
                if ok and data is not None:
                    oks += 1
                    rec = Record(
                        value=data.get("value"),
                        ts=float(data.get("ts")),
                        tombstone=bool(data.get("tombstone")),
                    )
                    best = InMemoryStore.newer(best, rec)
                if oks >= q:
                    break

            if best is None:
                return {"ok": False, "reason": "no_quorum", "responses": responses}

            if best.tombstone:
                return {"ok": True, "found": False, "record": {"value": None, "ts": best.ts, "tombstone": True}, "responses": responses}

            return {"ok": True, "found": True, "record": {"value": best.value, "ts": best.ts, "tombstone": False}, "responses": responses}
