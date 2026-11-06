"""Sanity checks for the generated dataset (run after generate.py).
Usage: python data_gen/validate.py [data_dir]
Checks: record counts, duplicate ids, total = subtotal - discount, empty items,
session linkage, null rates, referential integrity of products/users, subcategory presence."""
import glob, json, sys, collections

D = sys.argv[1] if len(sys.argv) > 1 else "data"

def jsonl(path):
    with open(path) as f:
        for line in f:
            yield json.loads(line)

users = {u["user_id"] for u in jsonl(f"{D}/users.jsonl")}
products = {p["product_id"]: p for p in jsonl(f"{D}/products.jsonl")}
tx = list(jsonl(f"{D}/transactions.jsonl"))
print(f"users: {len(users):,}  products: {len(products):,}  transactions: {len(tx):,}")
print("products missing subcategory_id:", sum(1 for p in products.values() if not p.get("subcategory_id")))
print("negative stock:", sum(1 for p in products.values() if p["current_stock"] < 0))

ids = [t["transaction_id"] for t in tx]
print("duplicate transaction ids:", len(ids) - len(set(ids)))
print("total != subtotal - discount:", sum(abs(t["subtotal"] - t["discount"] - t["total"]) > 0.011 for t in tx))
print("empty items:", sum(len(t["items"]) == 0 for t in tx))
print("unknown user in transactions:", sum(t["user_id"] not in users for t in tx))
print("unknown product in items:", sum(i["product_id"] not in products for t in tx for i in t["items"]))
linked = sum(t["session_id"] is not None for t in tx)
print(f"transactions linked to a session: {linked:,} ({linked / max(len(tx), 1):.1%}); unlinked: {len(tx) - linked:,}")
print("status mix:", dict(collections.Counter(t["status"] for t in tx)))

n = 0; sids = set(); dup = 0; status = collections.Counter(); dev = collections.Counter()
empty_pv = 0; pv_total = 0; null_prod_on_pd = 0
for path in sorted(glob.glob(f"{D}/sessions_*.jsonl")):
    for s in jsonl(path):
        n += 1
        if s["session_id"] in sids: dup += 1
        sids.add(s["session_id"])
        status[s["conversion_status"]] += 1
        dev[s["device_profile"]["type"]] += 1
        pv_total += len(s["page_views"])
        empty_pv += not s["page_views"]
        null_prod_on_pd += sum(1 for p in s["page_views"] if p["page_type"] == "product_detail" and not p["product_id"])
print(f"sessions: {n:,}  duplicate ids: {dup}  avg page views: {pv_total / max(n, 1):.1f}  sessions without page views: {empty_pv}")
print("conversion_status:", dict(status))
print("device mix:", dict(dev))
print("product_detail views with null product_id:", null_prod_on_pd)
orphan = sum(1 for t in tx if t["session_id"] and t["session_id"] not in sids)
print("linked transactions whose session is not in the files:", orphan)
