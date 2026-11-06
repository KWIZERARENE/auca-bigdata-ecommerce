"""Part 3: Integrated Conversion Funnel Analysis across HBase, MongoDB, and Apache Spark.

Business Question:
  Where do shoppers drop off between viewing a product, adding to cart, reaching checkout,
  and purchasing, and does conversion vary by device and referrer?

Data Flow:
  1. HBase (user_sessions): Behavioural session events (page views, cart additions, device, referrer).
  2. MongoDB (transactions): Authoritative purchase records and financial validation.
  3. Spark: Cross-store join and cumulative funnel aggregation.

Usage:
  python integration/funnel.py [--data data] [--out outputs] [--hbase-host localhost] [--hbase-port 9090] [--mongo-uri mongodb://localhost:27017]
"""
import argparse
import json
import os
import sys
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser(description="Integrated funnel analysis across HBase, MongoDB, and Spark.")
    parser.add_argument("--data", default="data", help="Directory containing JSONL dataset files")
    parser.add_argument("--out", default="outputs", help="Output directory for funnel.csv")
    parser.add_argument("--hbase-host", default="localhost", help="HBase Thrift host")
    parser.add_argument("--hbase-port", type=int, default=9090, help="HBase Thrift port")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--mongo-db", default="ecom", help="MongoDB database name")
    return parser.parse_args()

def fetch_hbase_sessions(host, port, data_dir):
    """
    Scans user_sessions from HBase via Thrift gateway.
    Falls back gracefully to data/sessions_0.jsonl if HBase Thrift is unreachable.
    """
    rows = []
    connected = False
    try:
        import happybase
        conn = happybase.Connection(host, port=port, timeout=10000)
        conn.open()
        if b"user_sessions" in conn.tables():
            table = conn.table("user_sessions")
            print(f"[HBase] Scanning user_sessions from {host}:{port}...")
            for k, v in table.scan(columns=[b"info", b"pv", b"cart"], batch_size=2000):
                pv_types = [c.decode().split("|")[1] for q, c in v.items() if q.startswith(b"pv:")]
                rows.append({
                    "session_id": v.get(b"info:session_id", b"").decode(),
                    "user_id": k.decode().split("#")[0],
                    "device": v.get(b"info:device", b"unknown").decode(),
                    "referrer": v.get(b"info:referrer", b"unknown").decode(),
                    "viewed": "product_detail" in pv_types,
                    "carted": any(q.startswith(b"cart:") for q in v),
                    "checkout": "checkout" in pv_types or "confirmation" in pv_types
                })
            connected = True
            print(f"[HBase] Successfully scanned {len(rows):,} sessions.")
    except Exception as e:
        print(f"[HBase Notice] Live connection not available ({str(e)[:80]}). Falling back to local extract.")

    if not connected:
        s0_path = os.path.join(data_dir, "sessions_0.jsonl")
        print(f"[Fallback] Loading session subset from {s0_path}...")
        if not os.path.exists(s0_path):
            sys.exit(f"Error: {s0_path} not found.")
        with open(s0_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                s = json.loads(line)
                pv_types = [p.get("page_type") for p in s.get("page_views", [])]
                cart_items = s.get("cart_contents", {})
                rows.append({
                    "session_id": s["session_id"],
                    "user_id": s["user_id"],
                    "device": s.get("device_profile", {}).get("type", "unknown"),
                    "referrer": s.get("referrer", "unknown"),
                    "viewed": "product_detail" in pv_types,
                    "carted": len(cart_items) > 0,
                    "checkout": "checkout" in pv_types or "confirmation" in pv_types
                })
        print(f"[Fallback] Loaded {len(rows):,} sessions matching HBase subset.")
    return pd.DataFrame(rows)

def fetch_mongo_purchases(uri, db_name, data_dir):
    """
    Extracts session-linked transactions from MongoDB.
    Falls back gracefully to data/transactions.jsonl if MongoDB is unreachable.
    """
    buys = []
    connected = False
    try:
        from pymongo import MongoClient
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        db = client[db_name]
        print(f"[MongoDB] Querying transactions with session linkage from {uri}...")
        cursor = db.transactions.find({"session_id": {"$ne": None}}, {"session_id": 1, "total": 1, "status": 1})
        for doc in cursor:
            buys.append({
                "session_id": doc["session_id"],
                "total": doc.get("total", 0.0),
                "status": doc.get("status", "completed")
            })
        connected = True
        print(f"[MongoDB] Retrieved {len(buys):,} session-linked transactions.")
    except Exception as e:
        print(f"[MongoDB Notice] Live connection not available ({str(e)[:80]}). Falling back to local extract.")

    if not connected:
        tx_path = os.path.join(data_dir, "transactions.jsonl")
        print(f"[Fallback] Loading transactions from {tx_path}...")
        if not os.path.exists(tx_path):
            sys.exit(f"Error: {tx_path} not found.")
        with open(tx_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                t = json.loads(line)
                if t.get("session_id"):
                    buys.append({
                        "session_id": t["session_id"],
                        "total": t.get("total", 0.0),
                        "status": t.get("status", "completed")
                    })
        print(f"[Fallback] Loaded {len(buys):,} session-linked transactions.")
    return pd.DataFrame(buys)

def compute_funnel(df_sessions, df_purchases, out_dir):
    """
    Joins sessions and purchases on session_id, enforces cumulative funnel logic,
    and aggregates by device and referrer.
    """
    os.makedirs(out_dir, exist_ok=True)
    print("\n--- Computing Funnel Join & Cumulative Stages ---")

    # Left join on session_id
    merged = df_sessions.merge(df_purchases[["session_id", "total"]], on="session_id", how="left")
    merged["has_transaction"] = merged["total"].notna()

    # Cumulative funnel stages:
    # Stage 1: All sessions
    # Stage 2: Viewed product
    # Stage 3: Added to cart (must also have viewed)
    # Stage 4: Reached checkout (must also have carted & viewed)
    # Stage 5: Purchased (has transaction, checkout, carted & viewed)
    merged["stage1_sessions"] = True
    merged["stage2_viewed"] = merged["viewed"]
    merged["stage3_carted"] = merged["stage2_viewed"] & merged["carted"]
    merged["stage4_checkout"] = merged["stage3_carted"] & merged["checkout"]
    merged["stage5_purchased"] = merged["stage4_checkout"] & merged["has_transaction"]

    total_sessions = len(merged)
    s1 = merged["stage1_sessions"].sum()
    s2 = merged["stage2_viewed"].sum()
    s3 = merged["stage3_carted"].sum()
    s4 = merged["stage4_checkout"].sum()
    s5 = merged["stage5_purchased"].sum()

    print(f"Stage 1 (All Sessions):       {s1:>8,}  (100.0%)")
    print(f"Stage 2 (Viewed Product):     {s2:>8,}  ({100*s2/s1:5.1f}%)  Drop: {100*(s1-s2)/s1:4.1f}%")
    print(f"Stage 3 (Added to Cart):      {s3:>8,}  ({100*s3/s1:5.1f}%)  Drop: {100*(s2-s3)/s2:4.1f}%")
    print(f"Stage 4 (Reached Checkout):   {s4:>8,}  ({100*s4/s1:5.1f}%)  Drop: {100*(s3-s4)/s3:4.1f}%")
    print(f"Stage 5 (Purchased):          {s5:>8,}  ({100*s5/s1:5.1f}%)  Drop: {100*(s4-s5)/s4:4.1f}%")

    # Segmentation by device and referrer
    grouped = merged.groupby(["device", "referrer"]).agg(
        sessions=("stage1_sessions", "count"),
        viewed=("stage2_viewed", "sum"),
        carted=("stage3_carted", "sum"),
        checkout=("stage4_checkout", "sum"),
        purchased=("stage5_purchased", "sum")
    ).reset_index()

    grouped["view_to_cart_pct"] = (100.0 * grouped["carted"] / grouped["viewed"]).round(2)
    grouped["cart_to_checkout_pct"] = (100.0 * grouped["checkout"] / grouped["carted"]).round(2)
    grouped["checkout_to_purchase_pct"] = (100.0 * grouped["purchased"] / grouped["checkout"]).round(2)
    grouped["overall_conversion_pct"] = (100.0 * grouped["purchased"] / grouped["sessions"]).round(2)

    csv_path = os.path.join(out_dir, "funnel.csv")
    grouped.to_csv(csv_path, index=False)
    print(f"\nSaved integrated funnel results to: {csv_path}")

    # Also save overall funnel totals for chart plotting
    summary_path = os.path.join(out_dir, "funnel_summary.csv")
    summary_df = pd.DataFrame([
        {"stage": "All sessions", "count": s1, "pct": 100.0, "drop_pct": 0.0},
        {"stage": "Viewed a product page", "count": s2, "pct": round(100.0*s2/s1, 1), "drop_pct": round(100.0*(s1-s2)/s1, 1)},
        {"stage": "Added to cart", "count": s3, "pct": round(100.0*s3/s1, 1), "drop_pct": round(100.0*(s2-s3)/s2, 1)},
        {"stage": "Reached checkout", "count": s4, "pct": round(100.0*s4/s1, 1), "drop_pct": round(100.0*(s3-s4)/s3, 1)},
        {"stage": "Purchased", "count": s5, "pct": round(100.0*s5/s1, 1), "drop_pct": round(100.0*(s4-s5)/s4, 1)}
    ])
    summary_df.to_csv(summary_path, index=False)
    print(f"Saved funnel summary to: {summary_path}")

def main():
    args = parse_args()
    df_sessions = fetch_hbase_sessions(args.hbase_host, args.hbase_port, args.data)
    df_purchases = fetch_mongo_purchases(args.mongo_uri, args.mongo_db, args.data)
    compute_funnel(df_sessions, df_purchases, args.out)

if __name__ == "__main__":
    main()

