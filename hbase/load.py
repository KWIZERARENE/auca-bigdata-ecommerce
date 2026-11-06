"""Load a subset of session data into HBase through the Thrift gateway (happybase).

Usage: python hbase/load.py [--host localhost] [--port 9090] [--data data] [--session-files 1] [--limit 0]

Tables
  user_sessions   key user_id#reverse_ts
        info:  session_id, start, device, os, browser, referrer, country, duration, status, n_pv
        pv:    one sparse column per page view, qualifier = 3-digit sequence, value = ts|type|product|category|secs
        cart:  one column per cart line, qualifier = product_id, value = quantity|price
  product_metrics key product_id#yyyymmdd   family m: views, cart_adds, purchases
        views/cart_adds come from the loaded sessions; purchases from transactions LINKED to a loaded session,
        so the three numbers describe the same population (consistent funnel).
        views = product_detail page views; cart_adds = sessions that added the product; purchases = units bought.
"""
import argparse, glob, json, os, sys
from collections import defaultdict
from keys import session_key, metric_key, day_of

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=9090)
ap.add_argument("--data", default="data")
ap.add_argument("--session-files", type=int, default=1)
ap.add_argument("--limit", type=int, default=0, help="max sessions to load (0 = all in the chosen files)")
ap.add_argument("--batch", type=int, default=1000)
a = ap.parse_args()

def jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def connect():
    import happybase
    try:
        c = happybase.Connection(a.host, port=a.port, timeout=60000)
        c.open()
        return c
    except Exception as e:
        sys.exit(f"Cannot reach HBase Thrift at {a.host}:{a.port}. Is the container up and ready (can take ~60 s)?\n{str(e)[:200]}")

def ensure_tables(conn):
    existing = set(conn.tables())
    for name, fams in (("user_sessions", ["info", "pv", "cart"]), ("product_metrics", ["m"])):
        if name.encode() not in existing:
            conn.create_table(name, {f: dict(max_versions=1) for f in fams})
            print("created table", name)

def session_cells(s):
    d = {b"info:session_id": s["session_id"], b"info:start": s["start_time"],
         b"info:device": s["device_profile"]["type"], b"info:os": s["device_profile"]["os"],
         b"info:browser": s["device_profile"]["browser"], b"info:referrer": s["referrer"],
         b"info:country": s["geo_data"]["country"], b"info:duration": str(s["duration_seconds"]),
         b"info:status": s["conversion_status"], b"info:n_pv": str(len(s["page_views"]))}
    for i, pv in enumerate(s["page_views"]):
        d[f"pv:{i:03d}".encode()] = "|".join("" if pv[k] is None else str(pv[k]) for k in
                                             ("timestamp", "page_type", "product_id", "category_id", "view_duration"))
    for pid, c in s["cart_contents"].items():
        d[f"cart:{pid}".encode()] = f"{c['quantity']}|{c['price']}"
    return {k: v.encode() for k, v in d.items()}

if __name__ == "__main__":
    files = sorted(glob.glob(os.path.join(a.data, "sessions_*.jsonl")))[: a.session_files]
    if not files:
        sys.exit("No sessions_*.jsonl in " + a.data)
    conn = connect()
    ensure_tables(conn)
    sess_tbl, met_tbl = conn.table("user_sessions"), conn.table("product_metrics")

    views, carts, buys = defaultdict(int), defaultdict(int), defaultdict(int)
    loaded_ids, n = set(), 0
    try:
        with sess_tbl.batch(batch_size=a.batch) as b:
            for path in files:
                for s in jsonl(path):
                    b.put(session_key(s["user_id"], s["start_time"]), session_cells(s))
                    loaded_ids.add(s["session_id"])
                    day = day_of(s["start_time"])
                    for pv in s["page_views"]:
                        if pv["page_type"] == "product_detail" and pv["product_id"]:
                            views[(pv["product_id"], day)] += 1
                    for pid in s["cart_contents"]:
                        carts[(pid, day)] += 1
                    n += 1
                    if n % 20000 == 0:
                        print(f"  {n:,} sessions loaded")
                    if a.limit and n >= a.limit:
                        break
                if a.limit and n >= a.limit:
                    break
    except Exception as e:
        sys.exit(f"Batch write failed after {n:,} sessions: {str(e)[:200]}")
    print(f"user_sessions: {n:,} sessions loaded")

    for t in jsonl(os.path.join(a.data, "transactions.jsonl")):
        if t["session_id"] in loaded_ids:
            day = day_of(t["timestamp"])
            for it in t["items"]:
                buys[(it["product_id"], day)] += it["quantity"]

    keys = set(views) | set(carts) | set(buys)
    with met_tbl.batch(batch_size=a.batch) as b:
        for (pid, day) in keys:
            b.put(metric_key(pid, day), {b"m:views": str(views.get((pid, day), 0)).encode(),
                                         b"m:cart_adds": str(carts.get((pid, day), 0)).encode(),
                                         b"m:purchases": str(buys.get((pid, day), 0)).encode()})
    print(f"product_metrics: {len(keys):,} (product, day) rows loaded. Done.")
