"""MongoDB aggregation pipelines (analytics for the report). Run after mongo/load.py.

Usage: python mongo/aggregations.py [--uri mongodb://localhost:27017] [--db ecom] [--out outputs]

  A  Revenue by category per month        ($match, $unwind, $lookup, $group, $dateToString, $project)
  B  User segmentation by purchase freq.  ($group, $bucket)
  C  Top products by units and revenue    ($unwind, $group, $lookup)
  D  Most-viewed products (sessions)      ($unwind page_views, $match, $group) -> joined with C in Python
Revenue = sum of line-item subtotals BEFORE the order-level discount (state this in the report).
"""
import argparse, csv, os
from common import get_db

ap = argparse.ArgumentParser()
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecom")
ap.add_argument("--out", default="outputs")
a = ap.parse_args()
db = get_db(a.uri, a.db)
os.makedirs(a.out, exist_ok=True)

def save(name, rows, fields):
    path = os.path.join(a.out, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  saved {path} ({len(rows)} rows)")

REVENUE = ["completed", "shipped", "delivered", "processing"]   # every status except cancelled/refunded (none here)

# ---------------- A: revenue by category and month ----------------
pipeline_a = [
    {"$match": {"status": {"$in": REVENUE}}},
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id", "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$group": {"_id": {"category": "$p.category.name",
                        "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
                "revenue": {"$sum": "$items.subtotal"},
                "units": {"$sum": "$items.quantity"},
                "orders": {"$addToSet": "$_id"}}},
    {"$project": {"_id": 0, "category": "$_id.category", "month": "$_id.month",
                  "revenue": 1, "units": 1, "orders": {"$size": "$orders"}}},
    {"$sort": {"month": 1, "revenue": -1}},
]
print("A. revenue by category and month")
rows = list(db.transactions.aggregate(pipeline_a, allowDiskUse=True))
for r in rows:
    r["revenue"] = round(r["revenue"], 2)
save("mongo_revenue_by_category_month.csv", rows, ["category", "month", "revenue", "units", "orders"])
for r in rows[:5]:
    print("   ", r["month"], r["category"], r["revenue"], r["units"])

# ---------------- B: segmentation by purchase frequency ----------------
pipeline_b = [
    {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "spend": {"$sum": "$total"}}},
    {"$bucket": {"groupBy": "$orders", "boundaries": [1, 2, 4, 7, 11, 1000], "default": "other",
                 "output": {"users": {"$sum": 1}, "avg_spend": {"$avg": "$spend"},
                            "total_spend": {"$sum": "$spend"}, "avg_orders": {"$avg": "$orders"}}}},
]
LABEL = {1: "1 order", 2: "2-3 orders", 4: "4-6 orders", 7: "7-10 orders", 11: "11+ orders"}
print("B. user segmentation by purchase frequency")
seg = []
for r in db.transactions.aggregate(pipeline_b):
    seg.append({"segment": LABEL.get(r["_id"], str(r["_id"])), "lower_bound": r["_id"] if isinstance(r["_id"], int) else 0,
                "users": r["users"], "avg_spend": round(r["avg_spend"], 2),
                "total_spend": round(r["total_spend"], 2), "avg_orders": round(r["avg_orders"], 2)})
seg.sort(key=lambda x: x["lower_bound"])
tot = sum(s["total_spend"] for s in seg) or 1
for s in seg:
    s["share_of_revenue_pct"] = round(100 * s["total_spend"] / tot, 1)
    print("   ", s["segment"], s["users"], "users, share of revenue", s["share_of_revenue_pct"], "%")
save("mongo_user_segments.csv", seg, ["segment", "users", "avg_orders", "avg_spend", "total_spend", "share_of_revenue_pct"])

# ---------------- C: units and revenue per product ----------------
pipeline_c = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "units": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}}},
    {"$sort": {"units": -1}},
]
print("C. units sold per product")
units = {r["_id"]: r for r in db.transactions.aggregate(pipeline_c, allowDiskUse=True)}
names = {p["_id"]: (p["name"], p["category"]["name"]) for p in db.products.find({}, {"name": 1, "category.name": 1})}
for pid, r in list(units.items())[:5]:
    print("   ", pid, names.get(pid, ("?",))[0], r["units"], round(r["revenue"], 2))

# ---------------- D: views per product (from sessions subset) ----------------
pipeline_d = [
    {"$unwind": "$page_views"},
    {"$match": {"page_views.page_type": "product_detail"}},
    {"$group": {"_id": "$page_views.product_id", "views": {"$sum": 1}}},
]
print("D. views per product (sessions subset)")
views = {r["_id"]: r["views"] for r in db.sessions.aggregate(pipeline_d, allowDiskUse=True)}
rows = []
for pid, (nm, cat) in names.items():
    u = units.get(pid, {"units": 0, "revenue": 0})
    v = views.get(pid, 0)
    rows.append({"product_id": pid, "name": nm, "category": cat, "views": v, "units": u["units"],
                 "revenue": round(u["revenue"], 2), "units_per_100_views": round(100 * u["units"] / v, 2) if v else None})
rows.sort(key=lambda r: -r["views"])
for r in rows[:5]:
    print("   ", r["product_id"], r["views"], "views", r["units"], "units")
save("mongo_views_vs_units.csv", rows, ["product_id", "name", "category", "views", "units", "revenue", "units_per_100_views"])
print("Done.")
