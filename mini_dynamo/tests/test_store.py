from dynamo.store import InMemoryStore, Record
import time

def test_put_and_get():
    store = InMemoryStore()
    store.put("a", "1", ts=1.0)

    rec = store.get("a")
    assert rec is not None
    assert rec.value == "1"
    assert rec.tombstone is False


def test_last_write_wins():
    store = InMemoryStore()

    store.put("a", "old", ts=1.0)
    store.put("a", "new", ts=2.0)

    rec = store.get("a")
    assert rec.value == "new"


def test_delete_creates_tombstone():
    store = InMemoryStore()

    store.put("a", "1", ts=1.0)
    store.delete("a", ts=2.0)

    rec = store.get("a")
    assert rec.tombstone is True
    assert rec.value is None


def test_newer_record_selection():
    r1 = Record(value="v1", ts=1.0)
    r2 = Record(value="v2", ts=2.0)

    newer = InMemoryStore.newer(r1, r2)
    assert newer.value == "v2"
