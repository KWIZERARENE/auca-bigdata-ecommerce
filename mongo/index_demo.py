"""Evidence for the report: query plan BEFORE and AFTER an index.

Usage: python mongo/index_demo.py [--uri ...] [--db ecom]
Query: "latest 5 transactions of one user".  Prints stage, docs examined, time for both cases.
(Needs a real MongoDB server; mongomock does not implement explain.)
"""
import argparse
from common import get_db

ap = argparse.ArgumentParser()
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecom")
a = ap.parse_args()
db = get_db(a.uri, a.db)
t = db.transactions
user = t.find_one({}, {"user_id": 1})["user_id"]
IDX = "user_id_1_timestamp_-1"

def plan(label):
    ex = t.find({"user_id": user}).sort("timestamp", -1).limit(5).explain("executionStats")
    s = ex["executionStats"]
    stage = ex["queryPlanner"]["winningPlan"]
    names = []
    while stage:                                    # walk the plan tree to list stages
        names.append(stage["stage"])
        stage = stage.get("inputStage")
    print(f"{label:<14} stages={'>'.join(names):<28} docsExamined={s['totalDocsExamined']:>7} "
          f"keysExamined={s['totalKeysExamined']:>5} returned={s['nReturned']} time={s['executionTimeMillis']} ms")

print("transactions in collection:", t.estimated_document_count(), "| user:", user)
try:
    t.drop_index(IDX)
except Exception:
    pass
plan("NO INDEX")
t.create_index([("user_id", 1), ("timestamp", -1)])
plan("WITH INDEX")
