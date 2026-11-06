"""Load the generated dataset into MongoDB with the document model described in the report.

Usage:  python mongo/load.py [--data data] [--uri mongodb://localhost:27017] [--db ecom] [--session-files 1]

Collections (see report section 'Data modeling'):
  products     category + subcategory EMBEDDED (denormalized), price_history EMBEDDED array
  users        profile + purchase_summary EMBEDDED (computed after transactions are loaded)
  transactions items[] EMBEDDED (a transaction is always read with its items)
  sessions     page_views[] EMBEDDED, cart_contents dict converted to a cart[] array (indexable)
Only --session-files x 100,000 sessions are loaded into MongoDB (a significant subset).
"""
import argparse, glob, os, sys
from pymongo import ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import BulkWriteError
from common import get_db, jsonl, dt, batches

ap = argparse.ArgumentParser()
ap.add_argument("--data", default="data")
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecom")
ap.add_argument("--session-files", type=int, default=1, help="number of 100k-session chunks to load")
a = ap.parse_args()
db = get_db(a.uri, a.db)

def insert_all(coll, docs, n=5000):
    total = 0
    for b in batches(docs, n):
        try:
            total += len(coll.insert_many(b, ordered=False).inserted_ids)
        except BulkWriteError as e:            # report and continue: one bad batch must not stop the load
            total += e.details.get("nInserted", 0)
            print(f"  ! batch error in {coll.name}: {str(e.details.get('writeErrors', [])[:1])[:200]}")
    return total

# ---- categories -> lookup dicts used to embed category info in each product ----
cats = {c["category_id"]: c for c in jsonl(os.path.join(a.data, "categories.jsonl"))}
subs = {s["subcategory_id"]: s for c in cats.values() for s in c["subcategories"]}

def product_docs():
    for p in jsonl(os.path.join(a.data, "products.jsonl")):
        c, s = cats[p["category_id"]], subs.get(p.get("subcategory_id"))
        p["_id"] = p.pop("product_id")
        p["category"] = {"id": c["category_id"], "name": c["name"]}
        p["subcategory"] = ({"id": s["subcategory_id"], "name": s["name"], "profit_margin": s["profit_margin"]}
                            if s else None)
        p["creation_date"] = dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = dt(ph["date"])
        yield p

def user_docs():
    for u in jsonl(os.path.join(a.data, "users.jsonl")):
        u["_id"] = u.pop("user_id")
        u["registration_date"] = dt(u["registration_date"])
        u["last_active"] = dt(u["last_active"])
        u["purchase_summary"] = {"total_orders": 0, "total_spend": 0.0, "last_purchase": None}
        yield u

def txn_docs():
    for t in jsonl(os.path.join(a.data, "transactions.jsonl")):
        t["_id"] = t.pop("transaction_id")
        t["timestamp"] = dt(t["timestamp"])
        yield t

def session_docs(files):
    for path in files:
        for s in jsonl(path):
            s["_id"] = s.pop("session_id")
            s["start_time"], s["end_time"] = dt(s["start_time"]), dt(s["end_time"])
            # dict keyed by product id is not indexable/queryable -> array of {product_id, quantity, price}
            s["cart"] = [{"product_id": k, **v} for k, v in s.pop("cart_contents").items()]
            for pv in s["page_views"]:
                pv["timestamp"] = dt(pv["timestamp"])
            yield s

for name in ("products", "users", "transactions", "sessions"):
    db[name].drop()

print("products     :", insert_all(db.products, product_docs()))
print("users        :", insert_all(db.users, user_docs()))
print("transactions :", insert_all(db.transactions, txn_docs()))
files = sorted(glob.glob(os.path.join(a.data, "sessions_*.jsonl")))[: a.session_files]
if not files:
    sys.exit("No sessions_*.jsonl files found in " + a.data)
print("sessions     :", insert_all(db.sessions, session_docs(files), 2000), f"(from {len(files)} file(s))")

# ---- embed a purchase summary into each user (computed from transactions) ----
agg = db.transactions.aggregate([
    {"$group": {"_id": "$user_id", "total_orders": {"$sum": 1}, "total_spend": {"$sum": "$total"},
                "last_purchase": {"$max": "$timestamp"}}}])
ops = [UpdateOne({"_id": r["_id"]},
                 {"$set": {"purchase_summary": {"total_orders": r["total_orders"],
                                                "total_spend": round(r["total_spend"], 2),
                                                "last_purchase": r["last_purchase"]}}}) for r in agg]
if ops:
    db.users.bulk_write(ops, ordered=False)
print("user purchase_summary updated for", len(ops), "users")

# ---- indexes (justified in the report; see mongo/index_demo.py for before/after evidence) ----
db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
db.transactions.create_index("timestamp")
db.transactions.create_index("items.product_id")            # multikey index on the embedded array
db.transactions.create_index("session_id")
db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
db.sessions.create_index("device_profile.type")
db.sessions.create_index("page_views.product_id")           # multikey: which sessions viewed a product
db.products.create_index("category.id")
db.users.create_index("geo_data.country")
print("indexes created. Done.")
