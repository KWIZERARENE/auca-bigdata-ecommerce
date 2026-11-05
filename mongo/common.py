"""Shared MongoDB helpers (connection with clear errors, JSONL reader, date parsing)."""
import json, sys
from datetime import datetime

def get_db(uri="mongodb://localhost:27017", name="ecom"):
    """Connect and ping. Use uri 'mongomock://' ONLY for offline unit tests (no server needed)."""
    if uri.startswith("mongomock"):
        import mongomock
        return mongomock.MongoClient()[name]
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except ServerSelectionTimeoutError as e:
        sys.exit(f"Cannot reach MongoDB at {uri}. Is the container running? (docker compose up -d)\n{str(e)[:200]}")
    return client[name]

def jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def dt(s):
    """ISO string -> datetime (None-safe). Stored as real dates so they can be grouped/range-queried."""
    return datetime.fromisoformat(s) if s else None

def batches(it, n=5000):
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf
