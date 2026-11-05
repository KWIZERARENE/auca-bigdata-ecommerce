"""Part 3 - Integrated analytics: FUNNEL CONVERSION across HBase + MongoDB + Spark.

Business question
  Where do shoppers drop off between viewing a product, adding it to the cart, reaching checkout and buying,
  and does the drop-off differ by device, referrer and customer purchase-frequency band?

Systems and why
  HBase   (user_sessions)  event data: page views + cart per session  -> funnel stages 1-3
  MongoDB (transactions)   authoritative purchases (session_id link)  -> funnel stage 4
  MongoDB (users)          profile + embedded purchase_summary        -> customer segment
  Spark                    the only place the three sources can be joined and aggregated at scale

Stages (cumulative: a session counts at a stage only if it also passed the earlier ones)
  sessions > viewed a product page > added to cart > reached checkout/confirmation > purchased (transaction exists)

Usage: python integration/funnel.py [--limit N] [--out outputs]   (needs HBase + MongoDB running and loaded)
"""
import argparse, csv, importlib.util, os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))

def load_module(name, path):
    """Load spark/common.py and mongo/common.py under different names (both files are called common.py)."""
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, "..", path))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

ap = argparse.ArgumentParser()
ap.add_argument("--hbase-host", default="localhost")
ap.add_argument("--hbase-port", type=int, default=9090)
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecom")
ap.add_argument("--limit", type=int, default=0, help="max HBase sessions to read (0 = all)")
ap.add_argument("--out", default="outputs")

# ---------- 1) HBase -> one row per session with stage flags ----------
def read_hbase(host, port, limit=0):
    import happybase
    conn = happybase.Connection(host, port=port, timeout=180000)
    table = conn.table("user_sessions")
    rows, t0 = [], time.time()
    # request only the columns the funnel needs (not the whole row): info subset, pv and cart families
    cols = [b"info:session_id", b"info:device", b"info:referrer", b"pv", b"cart"]
    for key, cells in table.scan(columns=cols, batch_size=2000, limit=limit or None):
        page_types = set()
        has_cart = False
        for q, v in cells.items():
            if q.startswith(b"pv:"):
                page_types.add(v.split(b"|", 2)[1].decode())          # value = ts|page_type|product|category|secs
            elif q.startswith(b"cart:"):
                has_cart = True
        viewed = "product_detail" in page_types
        rows.append((cells[b"info:session_id"].decode(), key.decode().split("#")[0],
                     cells[b"info:device"].decode(), cells[b"info:referrer"].decode(),
                     viewed, has_cart, bool(page_types & {"checkout", "confirmation"})))
    print(f"HBase : {len(rows):,} sessions scanned in {time.time() - t0:.1f}s")
    return rows

# ---------- 2) MongoDB -> purchases linked to sessions, and user segments ----------
def read_mongo(db):
    buys = [(d["session_id"], float(d["total"])) for d in
            db.transactions.find({"session_id": {"$ne": None}}, {"session_id": 1, "total": 1})]
    users = [(u["_id"], int(u.get("purchase_summary", {}).get("total_orders", 0)))
             for u in db.users.find({}, {"purchase_summary.total_orders": 1})]
    print(f"Mongo : {len(buys):,} session-linked transactions, {len(users):,} users")
    return buys, users

if __name__ == "__main__":
    a = ap.parse_args()
    mongo_common = load_module("mongo_common", "mongo/common.py")
    spark_common = load_module("spark_common", "spark/common.py")
    from pyspark.sql import functions as F

    hb_rows = read_hbase(a.hbase_host, a.hbase_port, a.limit)
    if not hb_rows:
        sys.exit("No sessions in HBase. Run: python hbase/load.py")
    buys, users = read_mongo(mongo_common.get_db(a.uri, a.db))

    spark = spark_common.get_spark("funnel")
    sess = spark.createDataFrame(hb_rows, "session_id string, user_id string, device string, referrer string, "
                                          "viewed boolean, carted boolean, checkout boolean")
    purch = spark.createDataFrame(buys, "session_id string, total double")
    usr = spark.createDataFrame(users, "user_id string, total_orders int")

    # users dimension -> frequency band (small table: broadcast join avoids shuffling the 100k sessions)
    band = (F.when(F.col("total_orders") == 0, "0 orders").when(F.col("total_orders") == 1, "1 order")
             .when(F.col("total_orders") <= 3, "2-3 orders").when(F.col("total_orders") <= 6, "4-6 orders")
             .otherwise("7+ orders"))
    usr = usr.withColumn("segment", band).select("user_id", "segment")

    f = (sess.join(F.broadcast(purch.withColumn("purchased_raw", F.lit(True)).select("session_id", "purchased_raw")),
                   "session_id", "left")
             .join(F.broadcast(usr), "user_id", "left")
             .withColumn("purchased_raw", F.coalesce("purchased_raw", F.lit(False)))
             # cumulative funnel: each stage requires the previous ones
             .withColumn("s1_viewed", F.col("viewed"))
             .withColumn("s2_carted", F.col("viewed") & F.col("carted"))
             .withColumn("s3_checkout", F.col("viewed") & F.col("carted") & F.col("checkout"))
             .withColumn("s4_purchased", F.col("viewed") & F.col("carted") & F.col("checkout") & F.col("purchased_raw")))
    f.cache()

    # --- reconciliation (data-quality checks that go in the report) ---
    n = f.count()
    raw_purchased = f.filter("purchased_raw").count()
    seq_purchased = f.filter("s4_purchased").count()
    print(f"Spark : {n:,} sessions joined | purchases matched {raw_purchased:,} | "
          f"purchases that pass all earlier stages {seq_purchased:,}")
    print(f"        linked transactions whose session is not among the loaded sessions: {len(buys) - raw_purchased:,}")

    def funnel(dim_col, dim_name):
        g = (f.groupBy(F.col(dim_col).alias("value") if dim_col else F.lit("all").alias("value"))
              .agg(F.count("*").alias("sessions"),
                   F.sum(F.col("s1_viewed").cast("int")).alias("viewed"),
                   F.sum(F.col("s2_carted").cast("int")).alias("carted"),
                   F.sum(F.col("s3_checkout").cast("int")).alias("checkout"),
                   F.sum(F.col("s4_purchased").cast("int")).alias("purchased")))
        return g.withColumn("dimension", F.lit(dim_name))

    res = (funnel(None, "overall").unionByName(funnel("device", "device"))
           .unionByName(funnel("referrer", "referrer")).unionByName(funnel("segment", "customer_segment"))
           .withColumn("view_rate", F.round(F.col("viewed") / F.col("sessions"), 4))
           .withColumn("view_to_cart", F.round(F.col("carted") / F.col("viewed"), 4))
           .withColumn("cart_to_checkout", F.round(F.col("checkout") / F.col("carted"), 4))
           .withColumn("checkout_to_purchase", F.round(F.col("purchased") / F.col("checkout"), 4))
           .withColumn("overall_conversion", F.round(F.col("purchased") / F.col("sessions"), 4))
           .orderBy("dimension", "value"))
    out_rows = res.select("dimension", "value", "sessions", "viewed", "carted", "checkout", "purchased", "view_rate",
                          "view_to_cart", "cart_to_checkout", "checkout_to_purchase", "overall_conversion").collect()
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, "funnel.csv")
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(out_rows[0].__fields__)
        w.writerows([tuple(r) for r in out_rows])
    print(f"\n{'dimension':<18}{'value':<15}{'sessions':>9}{'viewed':>8}{'carted':>8}{'checkout':>9}{'bought':>8}  conv%")
    for r in out_rows:
        print(f"{r['dimension']:<18}{r['value']:<15}{r['sessions']:>9,}{r['viewed']:>8,}{r['carted']:>8,}"
              f"{r['checkout']:>9,}{r['purchased']:>8,}  {100 * r['overall_conversion']:.2f}")
    print("\nsaved", path)
    spark.stop()
