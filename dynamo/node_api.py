import asyncio
import logging
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from .config import NodeConfig
from .logging_setup import setup_logging
from .hashing import ConsistentHashRing
from .membership import Membership
from .store import InMemoryStore
from .quorum import QuorumClient

log = logging.getLogger("node")

class PutReq(BaseModel):
    key: str
    value: str

class DelReq(BaseModel):
    key: str

class HeartbeatReq(BaseModel):
    from_: str | None = None
    from_url: Optional[str] = None
    from_url_alt: Optional[str] = None
    node_id: Optional[str] = None

class ReplicaPutReq(BaseModel):
    key: str
    value: str
    ts: float

class ReplicaDelReq(BaseModel):
    key: str
    ts: float

def create_app(node_id: str, base_url: str, peers: List[str], replication: int, w: int, q: int, debug: bool) -> FastAPI:
    cfg = NodeConfig(
        node_id=node_id,
        base_url=base_url,
        peers=peers,
        replication=replication,
        w=w,
        q=q,
        debug=debug,
    )
    setup_logging(cfg.debug)
    app = FastAPI(title=f"Mini-Dynamo Node {cfg.node_id}")

    store = InMemoryStore()
    membership = Membership(cfg.base_url, cfg.peers, timeout_s=cfg.request_timeout_s, dead_after_s=cfg.peer_dead_after_s)
    ring = ConsistentHashRing(membership.all_nodes(), vnodes=cfg.virtual_nodes)
    qc = QuorumClient(timeout_s=cfg.request_timeout_s)

    async def refresh_ring_periodically():
        while True:
            await asyncio.sleep(0.5)
            ring.set_nodes(membership.all_nodes())

    @app.on_event("startup")
    async def _startup():
        log.info("Starting node %s at %s, peers=%s", cfg.node_id, cfg.base_url, cfg.peers)
        asyncio.create_task(membership.heartbeat_loop(cfg.heartbeat_interval_s, self_id=cfg.node_id))
        asyncio.create_task(refresh_ring_periodically())

    @app.get("/health")
    def health():
        return {"ok": True, "node_id": cfg.node_id, "base_url": cfg.base_url}

    @app.get("/debug/state")
    def debug_state():
        return {
            "node_id": cfg.node_id,
            "base_url": cfg.base_url,
            "ring_nodes": ring.nodes,
            "peers": membership.peer_snapshot(),
            "replication": cfg.replication,
            "w": cfg.w,
            "q": cfg.q,
        }

    # Public client endpoints
    @app.post("/kv/put")
    async def kv_put(req: PutReq):
        ring.set_nodes(membership.all_nodes())
        replicas = ring.replicas(req.key, cfg.replication)
        ts = time.time()

        # Write to local store if this node is a replica
        if cfg.base_url in replicas:
            store.put(req.key, req.value, ts=ts)

        info = await qc.replicate_put(replicas, req.key, req.value, ts=ts, w=cfg.w)
        if info["acks"] < info["needed"]:
            raise HTTPException(status_code=503, detail={"error": "write_quorum_not_met", **info, "replicas": replicas})

        return {"ok": True, "key": req.key, "ts": ts, "replicas": replicas, "quorum": info}

    @app.get("/kv/get")
    async def kv_get(key: str):
        ring.set_nodes(membership.all_nodes())
        replicas = ring.replicas(key, cfg.replication)
        res = await qc.quorum_get(replicas, key, q=cfg.q)
        if not res["ok"]:
            raise HTTPException(status_code=503, detail={"error": "read_quorum_not_met", "replicas": replicas, **res})
        return {"ok": True, "key": key, "replicas": replicas, **res}

    @app.post("/kv/delete")
    async def kv_delete(req: DelReq):
        ring.set_nodes(membership.all_nodes())
        replicas = ring.replicas(req.key, cfg.replication)
        ts = time.time()

        if cfg.base_url in replicas:
            store.delete(req.key, ts=ts)

        info = await qc.replicate_delete(replicas, req.key, ts=ts, w=cfg.w)
        if info["acks"] < info["needed"]:
            raise HTTPException(status_code=503, detail={"error": "delete_quorum_not_met", **info, "replicas": replicas})

        return {"ok": True, "key": req.key, "ts": ts, "replicas": replicas, "quorum": info}

    # Internal replica endpoints
    @app.post("/internal/replica/put")
    def replica_put(req: ReplicaPutReq):
        store.put(req.key, req.value, ts=req.ts)
        return {"ok": True}

    @app.post("/internal/replica/delete")
    def replica_delete(req: ReplicaDelReq):
        store.delete(req.key, ts=req.ts)
        return {"ok": True}

    @app.get("/internal/replica/get")
    def replica_get(key: str):
        rec = store.get(key)
        if rec is None:
            # Return a "not found" record response, but still OK.
            return {"ok": True, "value": None, "ts": 0.0, "tombstone": True}
        return {"ok": True, "value": rec.value, "ts": rec.ts, "tombstone": rec.tombstone}

    # Internal membership endpoints
    @app.post("/internal/heartbeat")
    def heartbeat(payload: Dict[str, Any]):
        from_url = payload.get("from") or payload.get("from_url") or payload.get("from_url_alt")
        if isinstance(from_url, str) and from_url:
            membership.mark_seen(from_url)
        return {"ok": True}

    return app
