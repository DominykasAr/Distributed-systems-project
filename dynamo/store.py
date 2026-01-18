from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import time

@dataclass
class Record:
    value: Optional[str]
    ts: float
    tombstone: bool = False

class InMemoryStore:
    def __init__(self):
        self._data: Dict[str, Record] = {}

    def put(self, key: str, value: str, ts: Optional[float] = None) -> Record:
        ts = ts if ts is not None else time.time()
        rec = Record(value=value, ts=ts, tombstone=False)
        self._data[key] = rec
        return rec

    def delete(self, key: str, ts: Optional[float] = None) -> Record:
        ts = ts if ts is not None else time.time()
        rec = Record(value=None, ts=ts, tombstone=True)
        self._data[key] = rec
        return rec

    def get(self, key: str) -> Optional[Record]:
        return self._data.get(key)

    @staticmethod
    def newer(a: Optional[Record], b: Optional[Record]) -> Optional[Record]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a.ts >= b.ts else b