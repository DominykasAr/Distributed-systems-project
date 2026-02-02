from dataclasses import dataclass
from typing import List

@dataclass
class NodeConfig:
    node_id: str
    base_url: str
    peers: List[str]
    replication: int = 2
    w: int = 1
    q: int = 1
    debug: bool = False

    request_timeout_s: float = 1.5
    heartbeat_interval_s: float = 1.0
    peer_dead_after_s: float = 3.5
    virtual_nodes: int = 50