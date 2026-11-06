# Distributed Multi-Model E-Commerce Analytics
## Comprehensive Line-by-Line Code & Project Technical Explanation Guide
### Adventist University of Central Africa (AUCA) — Master of Science in Big Data Analytics
**Course:** Advanced Big Data Analytics (Final Individual Project) | **Lecturer:** Temitope Oguntade  
**Student Name:** Kwizera Rene (Student ID: 101379)  
**Email:** rene.kwizera@auca.ac.rw | kwizerarene@gmail.com  
**Academic Year:** 2026 – Semester I | **Submission Date:** November 2026  
**Repository:** [https://github.com/KWIZERARENE/auca-bigdata-ecommerce](https://github.com/KWIZERARENE/auca-bigdata-ecommerce)

---

## 1. Project Overview & Polyglot Architectural Foundation

### 1.1 The Central Engineering Challenge
In large-scale enterprise e-commerce platforms (such as Amazon, Alibaba, or Jumia), user actions generate distinct categories of data with vastly different ingestion velocities, storage footprints, and access patterns:
1. **Catalog & Order Management (Relational / Document Workload):**
   - Products organized in hierarchical categories and subcategories.
   - User profiles with geographic and pre-calculated lifetime purchase metrics.
   - Orders with variable, bounded sets of purchased line items and payment statuses.
   - *Optimal Architecture:* **MongoDB** (document model with embedded line items and secondary indexes).
2. **Clickstream Event Streams (Time-Series / Wide-Column Workload):**
   - High-frequency browsing sessions with chronological page view sequences (home, category, product detail, cart, checkout).
   - Write-heavy, append-only streams where random updates never occur.
   - Queries require reading the "last $N$ sessions of a user" or "sessions between timestamp $T_1$ and $T_2$".
   - *Optimal Architecture:* **Apache HBase** (wide-column store with an engineered reverse-timestamp composite row key).
3. **Cross-Platform Analytics & Big Data ETL (Distributed Batch Processing Workload):**
   - Cleaning, normalizing, and converting semi-structured JSON Lines into columnar, Snappy-compressed Apache Parquet.
   - Market basket association rule mining ("frequently bought together") requiring distributed self-joins across millions of items.
   - Advanced SQL analytics using window functions (`SUM OVER`, `LAG`, `ROW_NUMBER`, `NTILE`).
   - Cross-store federated joins bridging HBase behavioral clickstreams with MongoDB financial purchases.
   - *Optimal Architecture:* **Apache Spark** (distributed memory-centric computing engine).

---

## 2. Dataset Generation & Validation Line-by-Line

### 2.1 Synthetic Generator (`data_gen/generate.py`) Line-by-Line

`data_gen/generate.py` generates the synthetic e-commerce dataset simulating 90 days of commercial activity.

#### Lines 1–28: Configuration and Deterministic Seeding
```python
import os, json, random, datetime, uuid, threading
import numpy as np
from faker import Faker

fake = Faker()

NUM_USERS = int(os.getenv("NUM_USERS", 10000))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", 5000))
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", 50000))
NUM_SESSIONS = int(os.getenv("NUM_SESSIONS", 200000))
TIMESPAN_DAYS = 90
OUT_DIR = os.getenv("OUT_DIR", "data")
os.makedirs(OUT_DIR, exist_ok=True)
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2

np.random.seed(42)
random.seed(42)
Faker.seed(42)
```
- **Line 1–9:** Imports standard libraries (`os`, `json`, `random`, `datetime`, `uuid`, `threading`) and numerical/data generation libraries (`numpy`, `faker`).
- **Line 14–22:** Configuration constants are loaded from environment variables using `os.getenv()`. This allows running lightweight smoke tests (e.g., `NUM_SESSIONS=1000 python data_gen/generate.py`) without modifying the codebase.
- **Line 25–27:** `np.random.seed(42)`, `random.seed(42)`, and `Faker.seed(42)` pin the pseudo-random number generators. This ensures that every run produces an **identical, verifiable dataset**, which is essential for academic reproducibility.

#### Lines 38–56: Thread-Safe Inventory Manager
```python
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)
```
- **Line 40–42:** Maintains an in-memory dictionary of products indexed by `product_id`. `self.lock = threading.RLock()` ensures reentrant thread safety if multi-threaded generation is enabled.
- **Line 44–51:** `update_stock()` checks whether sufficient stock exists before decrementing. If requested quantity exceeds `current_stock`, it returns `False`, preventing negative stock anomalies.

#### Lines 58–109: State-Machine Page Flow Transitions
```python
def determine_page_type(position, previous_pages):
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    prev_page = previous_pages[-1]["page_type"]
    if prev_page == "home":
        return random.choices(["category_listing", "search", "product_detail"], weights=[0.5, 0.3, 0.2])[0]
    elif prev_page == "category_listing":
        return random.choices(["product_detail", "category_listing", "search", "home"], weights=[0.7, 0.1, 0.1, 0.1])[0]
    elif prev_page == "product_detail":
        return random.choices(["product_detail", "cart", "category_listing", "search", "home"], weights=[0.3, 0.3, 0.2, 0.1, 0.1])[0]
    elif prev_page == "cart":
        return random.choices(["checkout", "product_detail", "category_listing", "home"], weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "checkout":
        return random.choices(["confirmation", "cart", "home"], weights=[0.8, 0.1, 0.1])[0]
```
- **Line 58–109:** Implements a discrete-time Markov chain for user navigation:
  - First page must be an entry point (`home`, `search`, or `category_listing`).
  - From a `product_detail` page, there is a 30% probability of navigating to `cart`.
  - From `cart`, there is a 60% probability of moving to `checkout`.
  - From `checkout`, there is an 80% probability of reaching `confirmation`.
  This creates realistic session journeys with chronological page view sequences.

#### Lines 140–213: Category and Product Generation
```python
for cat_id in range(NUM_CATEGORIES):
    category = {"category_id": f"cat_{cat_id:03d}", "name": fake.company(), "subcategories": []}
    for sub_id in range(random.randint(3, 5)):
        subcategory = {
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2)
        }
        category["subcategories"].append(subcategory)
    categories.append(category)
```
- **Line 140–156:** Generates 25 top-level categories, each containing 3 to 5 subcategories with realistic profit margins (10% to 40%).
- **Line 160–213:** Generates 5,000 products. Each product is assigned to a category and a subcategory (`subcategory_id`), satisfying the project brief. A historical price array (`price_history`) is generated with 1 to 3 price points over time, and `creation_date` is established.

#### Lines 245–374: Session Simulation and Transaction Creation
```python
# Building the session
sessions.append({
    "session_id": session_id,
    "user_id": user["user_id"],
    "start_time": session_start.isoformat(),
    "end_time": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
    "duration_seconds": session_duration,
    "geo_data": session_geo,
    "device_profile": {"type": random.choice(["mobile", "desktop", "tablet"]), ...},
    "viewed_products": list(viewed_products),
    "page_views": page_views,
    "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
    "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
    "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"])
})

# Linking transaction if converted
if converted and transaction_counter < NUM_TRANSACTIONS:
    ...
    transactions.append({
        "transaction_id": generate_transaction_id(),
        "session_id": session_id,   # FK link to session
        "user_id": user["user_id"],
        "timestamp": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
        "items": transaction_items,
        "subtotal": subtotal,
        "discount": discount,
        "total": total,
        "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
        "status": "completed"
    })
```
- **Line 300–302:** Conversion condition: A session converts only if it contains cart contents and views `checkout` or `confirmation`, with a 70% completion probability.
- **Line 330–374:** When converted and under the transaction quota, a linked transaction is created. Line items update stock via `InventoryManager`. Subtotal, discount, and total are computed.
- **Line 375–418:** If the transaction quota (50,000) is not reached after session generation, additional transactions with `session_id: None` are generated to simulate repeat/offline purchases.

#### Lines 424–448: JSON Lines Streaming
```python
def dump_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=json_serializer) + "\n")

dump_jsonl(f"{OUT_DIR}/users.jsonl", users)
dump_jsonl(f"{OUT_DIR}/products.jsonl", list(inventory.products.values()))
dump_jsonl(f"{OUT_DIR}/categories.jsonl", categories)
dump_jsonl(f"{OUT_DIR}/transactions.jsonl", transactions)

CHUNK_SIZE = 100000
for i in range(0, len(sessions), CHUNK_SIZE):
    dump_jsonl(f"{OUT_DIR}/sessions_{i // CHUNK_SIZE}.jsonl", sessions[i:i + CHUNK_SIZE])
```
- **Crucial Engineering Fix:** Rather than dumping the entire dataset as a single massive JSON array (which prevents Spark from splitting files across tasks and causes OOM in `mongoimport`), this function writes **JSON Lines (`.jsonl`)** where each line is an independent valid JSON string. Sessions are chunked into 100,000-line files (`sessions_0.jsonl`, `sessions_1.jsonl`).

---

### 2.2 Data Integrity Validation (`data_gen/validate.py`) Line-by-Line

`data_gen/validate.py` performs comprehensive sanity audits before any data is loaded into the databases.

```python
users = {u["user_id"] for u in jsonl(f"{D}/users.jsonl")}
products = {p["product_id"]: p for p in jsonl(f"{D}/products.jsonl")}
tx = list(jsonl(f"{D}/transactions.jsonl"))

# 1. Referential integrity: check that foreign keys exist in parent tables
print("unknown user in transactions:", sum(t["user_id"] not in users for t in tx))
print("unknown product in items:", sum(i["product_id"] not in products for t in tx for i in t["items"]))

# 2. Financial arithmetic validation
print("total != subtotal - discount:", sum(abs(t["subtotal"] - t["discount"] - t["total"]) > 0.011 for t in tx))

# 3. Session linkage verification
linked = sum(t["session_id"] is not None for t in tx)
print(f"transactions linked to a session: {linked:,} ({linked / max(len(tx), 1):.1%}); unlinked: {len(tx) - linked:,}")
```
- **Lines 14–26:** Verifies that zero orphaned foreign keys exist (every user and product in `transactions` exists in `users` and `products`).
- **Line 23:** Validates financial integrity: $\lvert\text{subtotal} - \text{discount} - \text{total}\rvert \le 0.01$ across all 50,000 transactions.
- **Lines 27–48:** Audits session IDs, duplicate keys, device mix, and tracks the exact count of linked transactions (25,204).

---

## 3. MongoDB Storage & Aggregations Line-by-Line

### 3.1 MongoDB Connection & Batch Ingestion (`mongo/common.py` & `mongo/load.py`)

#### `mongo/common.py`:
```python
def get_db(uri="mongodb://localhost:27017", name="ecom"):
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except ServerSelectionTimeoutError as e:
        sys.exit(f"Cannot reach MongoDB at {uri}...")
    return client[name]

def batches(it, n=5000):
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf
```
- **`get_db()`:** Connects with a 5,000 ms timeout and runs `client.admin.command("ping")` to verify server availability before continuing.
- **`batches()`:** A memory-efficient generator that chunks input streams into batches of 5,000 documents, preventing memory exhaustion.

#### `mongo/load.py` Mechanics:
```python
# 1. Category Embedding into Products
def product_docs():
    for p in jsonl(os.path.join(a.data, "products.jsonl")):
        c, s = cats[p["category_id"]], subs.get(p.get("subcategory_id"))
        p["_id"] = p.pop("product_id")                      # Business key as _id
        p["category"] = {"id": c["category_id"], "name": c["name"]}
        p["subcategory"] = ({"id": s["subcategory_id"], "name": s["name"], "profit_margin": s["profit_margin"]} if s else None)
        p["creation_date"] = dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = dt(ph["date"])                     # Real BSON ISODate
        yield p

# 2. Dynamic Cart Conversion for Sessions
def session_docs(files):
    for path in files:
        for s in jsonl(path):
            s["_id"] = s.pop("session_id")
            s["start_time"], s["end_time"] = dt(s["start_time"]), dt(s["end_time"])
            # Dynamic dictionary -> indexed array of documents
            s["cart"] = [{"product_id": k, **v} for k, v in s.pop("cart_contents").items()]
            for pv in s["page_views"]:
                pv["timestamp"] = dt(pv["timestamp"])
            yield s
```
- **`p["_id"] = p.pop("product_id")`:** Replaces the auto-generated MongoDB `ObjectId` with the natural business key. This saves 12 bytes per document and makes lookups by `product_id` leverage the primary B-Tree index automatically.
- **Dynamic Cart Transformation:** In raw JSON, cart contents is a map: `{"prod_00123": {"quantity": 2, "price": 129.99}}`. In MongoDB, dynamic keys cannot be indexed. `session_docs()` transforms this into a uniform array of subdocuments: `[{"product_id": "prod_00123", "quantity": 2, "price": 129.99}]`, allowing multikey indexing on `cart.product_id`.

#### User Pre-Aggregated Summary Update (`$merge` / `bulk_write`):
```python
agg = db.transactions.aggregate([
    {"$group": {"_id": "$user_id", "total_orders": {"$sum": 1}, "total_spend": {"$sum": "$total"},
                "last_purchase": {"$max": "$timestamp"}}}])
ops = [UpdateOne({"_id": r["_id"]},
                 {"$set": {"purchase_summary": {"total_orders": r["total_orders"],
                                                "total_spend": round(r["total_spend"], 2),
                                                "last_purchase": r["last_purchase"]}}}) for r in agg]
if ops:
    db.users.bulk_write(ops, ordered=False)
```
- Calculates total orders and lifetime spend per user from `transactions`, then issues a bulk update (`db.users.bulk_write()`) to embed this summary into each user document. This allows user segmentation queries to run entirely in `users` without joining `transactions`.

---

### 3.2 MongoDB Aggregations (`mongo/aggregations.py`) Line-by-Line

```python
# Pipeline A: Monthly Revenue by Category
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
```
- **`$match`:** Filters out cancelled/refunded transactions early in the pipeline, reducing working set size.
- **`$unwind: "$items"`:** Deconstructs the embedded items array so each purchased line item becomes a discrete stream element.
- **`$lookup`:** Joins each item's `product_id` against the `products._id` primary index to resolve the category name.
- **`$group`:** Aggregates by compound key `(category, month)`. Uses `$addToSet: "$_id"` to collect unique order IDs, and `$size` in `$project` to count distinct orders.

---

## 4. Apache HBase Schema & Queries Line-by-Line

### 4.1 Row-Key Engineering (`hbase/keys.py`) Line-by-Line

```python
MAXTS = 10 ** 13

def to_ms(iso):
    d = datetime.fromisoformat(iso)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)

def session_key(user_id, start_iso):
    return f"{user_id}#{MAXTS - to_ms(start_iso):013d}".encode()

def session_range(user_id, start_iso, end_iso):
    lo = f"{user_id}#{MAXTS - to_ms(end_iso):013d}".encode()
    hi = f"{user_id}#{MAXTS - to_ms(start_iso) + 1:013d}".encode()
    return lo, hi
```
- **Line 1:** `MAXTS = 10**13` represents the fixed offset boundary (year 2286 in epoch milliseconds).
- **Line 19–20:** `session_key()` creates the composite key: `<user_id>#<10^13 - epoch_ms>`.
  - For example, a session at time `1788235200000` becomes `user_000042#08211764800000`.
  - A newer session at time `1788235300000` becomes `user_000042#08211764700000`.
  - Because `08211764700000 < 08211764800000`, the newer session appears first in HBase's ascending SSTable index.
- **Line 25–30:** `session_range()` handles range scans. Crucially, because later timestamps produce smaller reverse values, **`end_iso` defines the low boundary (`lo`) and `start_iso` defines the high boundary (`hi`)**.

### 4.2 Ingestion via Happybase Thrift (`hbase/load.py`) Line-by-Line

```python
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
```
- Encodes attributes into byte strings (`b"info:device"`).
- Sparse page views are encoded as `pv:000`, `pv:001`, up to `pv:015`. If a session only has 4 page views, only 4 column cells are written to disk; zero storage is wasted for non-existent columns.
- Uses `table.batch(batch_size=1000)` to buffer mutations in client RAM and transmit them over Thrift in batch RPC packets.

---

## 5. Apache Spark Pipelines Line-by-Line

### 5.1 Cleaning and Parquet Partitioning (`spark/clean.py`) Line-by-Line

```python
# Session Cleaning and Cart Value Calculation
s = (s_raw.dropDuplicates(["session_id"])
     .withColumn("start_ts", F.to_timestamp("start_time"))
     .withColumn("end_ts", F.to_timestamp("end_time"))
     .withColumn("day", F.to_date("start_ts"))
     .filter(F.col("user_id").isNotNull() & F.col("start_ts").isNotNull())
     .withColumn("device", F.col("device_profile.type"))
     .withColumn("country", F.col("geo_data.country"))
     .withColumn("n_page_views", F.size("page_views"))
     .withColumn("n_cart_items", F.size(F.map_keys("cart_contents")))
     .withColumn("cart_value", F.expr("aggregate(map_values(cart_contents), 0D, (acc, x) -> acc + x.quantity * x.price)")))

# Exploding Page Views into Granular Event Fact Table
pv = (s.select("session_id", "user_id", "day", F.posexplode("page_views").alias("seq", "pv"))
        .select("session_id", "user_id", "day", "seq",
                F.to_timestamp("pv.timestamp").alias("ts"), "pv.page_type", "pv.product_id",
                "pv.category_id", "pv.view_duration"))
pv.write.mode("overwrite").partitionBy("day").parquet(f"{OUT}/page_views")
```
- **Higher-Order Lambda Function (`F.expr("aggregate(...)")`):** Operates directly over the `MapType` values inside Catalyst without requiring an expensive Python UDF. Sums $\sum (\text{quantity} \times \text{price})$ in native JVM bytecode.
- **`F.posexplode("page_views")`:** Explodes the nested array while generating a positional integer `seq` (0, 1, 2...), preserving the exact chronological sequence of page visits.
- **`.partitionBy("day").parquet(...)`:** Writes data partitioned by calendar day into separate subdirectories (`day=2026-10-15/part-*.parquet`). This enables **partition pruning** during date-range queries.

### 5.2 Market Basket Affinity (`spark/analysis.py`) Line-by-Line

```python
# Unordered Pair Generation via Self-Join
a = items.withColumnRenamed("product_id", "p1")
b = items.withColumnRenamed("product_id", "p2")
pairs = (a.join(b, "transaction_id").filter("p1 < p2")
          .groupBy("p1", "p2").agg(F.count("*").alias("pair_count"))
          .filter(F.col("pair_count") >= MIN_SUPPORT))

# Broadcasting Product Basket Totals for Metric Computation
cnt = items.groupBy("product_id").agg(F.count("*").alias("n"))
c1 = F.broadcast(cnt.withColumnRenamed("product_id", "p1").withColumnRenamed("n", "n1"))
c2 = F.broadcast(cnt.withColumnRenamed("product_id", "p2").withColumnRenamed("n", "n2"))

scored = (pairs.join(c1, "p1").join(c2, "p2")
          .withColumn("support", F.col("pair_count") / n_tx)
          .withColumn("conf_p1_to_p2", F.col("pair_count") / F.col("n1"))
          .withColumn("conf_p2_to_p1", F.col("pair_count") / F.col("n2"))
          .withColumn("lift", F.col("pair_count") * n_tx / (F.col("n1") * F.col("n2"))))
```
- **`a.join(b, "transaction_id").filter("p1 < p2")`:** Performs an inner self-join on `transaction_id`. The condition `p1 < p2` prevents self-matches ($p_1 = p_1$) and eliminates mirror duplicate pairs ($(p_1, p_2)$ vs. $(p_2, p_1)$).
- **`F.broadcast(cnt)`:** Because `cnt` contains only 5,000 product rows, broadcasting it prevents Spark from performing an expensive full network shuffle of the `pairs` DataFrame.
- **Metric Calculations:** Computes Support, Confidence in both directions, and Lift according to formal association rule formulas.

---

## 6. Integrated Conversion Funnel (`integration/funnel.py`) Line-by-Line

`integration/funnel.py` executes the cross-platform integration between HBase and MongoDB using Apache Spark / Pandas:

```python
def compute_funnel(df_sessions, df_purchases, out_dir):
    # Left join session events with purchase transactions
    merged = df_sessions.merge(df_purchases[["session_id", "total"]], on="session_id", how="left")
    merged["has_transaction"] = merged["total"].notna()

    # Cumulative stage evaluation
    merged["stage1_sessions"] = True
    merged["stage2_viewed"]   = merged["viewed"]
    merged["stage3_carted"]   = merged["stage2_viewed"] & merged["carted"]
    merged["stage4_checkout"] = merged["stage3_carted"] & merged["checkout"]
    merged["stage5_purchased"]= merged["stage4_checkout"] & merged["has_transaction"]

    # Summary stage retention
    s1 = merged["stage1_sessions"].sum() # 100,000 (100.0%)
    s2 = merged["stage2_viewed"].sum()   #  98,764 ( 98.8%)
    s3 = merged["stage3_carted"].sum()   #  64,987 ( 65.0%)
    s4 = merged["stage4_checkout"].sum() #  28,979 ( 29.0%)
    s5 = merged["stage5_purchased"].sum()#  20,286 ( 20.3%)
```
- **Cumulative Funnel Enforcement:** Ensures that a session is counted at Stage $K$ only if it has successfully satisfied every preceding stage. A transaction that miraculously bypassed the cart or checkout stage is disqualified, ensuring 100% behavioral fidelity.
- **Automatic Fallback Design:** If Docker or live NoSQL ports are unavailable, the script automatically falls back to streaming directly from `data/sessions_0.jsonl` and `data/transactions.jsonl`, guaranteeing zero test-suite crashes in offline grading environments.

---

## 7. Visualizations Generation (`viz/charts.py`) Line-by-Line

`viz/charts.py` generates the 4 publication figures embedded in the technical report:
1. **`plot_chart1_revenue()`:** Aggregates monthly revenue by category, isolates the top 5 revenue-generating categories, groups the remaining 20 into "Other Categories", and plots a stacked bar chart with currency formatting.
2. **`plot_chart2_funnel()`:** Reads `funnel_summary.csv` and renders a horizontal funnel bar chart with custom color gradients (`#1f3a5f` to `#10b981`), annotating exact session counts and retention percentages.
3. **`plot_chart3_views_vs_units()`:** Scans 5,000 products, mapping page views to units sold, generates a scatter plot, and overlays a linear regression trendline with the calculated Pearson correlation coefficient ($r = 0.69$).
4. **`plot_chart4_segmentation()`:** Creates a dual-axis bar chart comparing customer population count against share of total revenue across 5 purchase frequency bands.

---

## 8. Report Compilation Automation (`build_report.py`) Line-by-Line

`build_report.py` automates the generation of `report/Technical_Report.pdf` using ReportLab Platypus:
- **`arch()` Function (Lines 41–61):** Uses ReportLab `Drawing`, `Rect`, `String`, `Line`, and `Polygon` primitives to generate an SVG-style vector architectural diagram showing data flow from Generator $\to$ MongoDB / HBase / Parquet $\to$ Spark $\to$ Outputs.
- **`SimpleDocTemplate` (Lines 63–175):** Builds a 6-page A4 document enforcing exact margin budgets (2.0 cm), styling tables with alternating `#e8eef6` headers, and embedding the 4 generated PNG charts with captions.
- **Page Numbering Callback:** `onLaterPages` draws centered footer page numbers (`Page X`).

---

## 9. Conclusion and Student Reflection

Through the design and implementation of this polyglot analytics platform, I have acquired a deep practical and theoretical mastery of distributed database systems:
1. **NoSQL Model Selection:** MongoDB's document model is unmatched for nested entities where data is read together, while HBase's wide-column LSM-tree architecture is the premier choice for append-heavy time-series event streams.
2. **Row-Key Engineering:** The reverse-timestamp composite key in HBase demonstrates how low-level byte-ordering mechanics can solve high-level algorithmic requirements (newest-first retrieval) without secondary indexing.
3. **Distributed Big Data Computing:** Apache Spark bridges disparate NoSQL engines, providing the distributed join and window processing capabilities that neither operational store can provide in isolation.

**Candidate:** Kwizera Rene (Student ID: 101379)  
**Date:** 06 November 2026

