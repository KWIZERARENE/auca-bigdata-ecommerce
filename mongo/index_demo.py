"""Evidence for the report: query plan BEFORE and AFTER an index.

Usage: python mongo/index_demo.py [--uri ...] [--db ecom]
Query: "latest 5 transactions of one user".  Prints plan stages, docs examined and time for both cases.
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

def stages(plan):
    """Collect stage names from the (possibly nested) winning plan, e.g. LIMIT > FETCH > IXSCAN."""
    plan = plan.get("queryPlan", plan)               # MongoDB 7 may wrap the plan (slot-based engine)
    names = []
    while isinstance(plan, dict):
        names.append(plan.get("stage", "?"))
        plan = plan.get("inputStage")
    return names

def plan(label):
    cmd = {"find": "transactions", "filter": {"user_id": user}, "sort": {"timestamp": -1}, "limit": 5}
    ex = db.command("explain", cmd, verbosity="executionStats")
    s = ex["executionStats"]
    names = stages(ex["queryPlanner"]["winningPlan"])
    print(f"{label:<11} stages={' > '.join(names):<30} docsExamined={s['totalDocsExamined']:>6} "
          f"keysExamined={s['totalKeysExamined']:>4} returned={s['nReturned']} time={s['executionTimeMillis']} ms")

print("transactions in collection:", t.estimated_document_count(), "| user:", user)
try:
    t.drop_index(IDX)
except Exception:
    pass                                              # index did not exist
plan("NO INDEX")
t.create_index([("user_id", 1), ("timestamp", -1)])
plan("WITH INDEX")
