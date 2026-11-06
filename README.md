# Distributed Multi-Model E-Commerce Analytics (MongoDB + HBase + Spark)

AUCA Big Data Analytics - individual final project. A synthetic e-commerce dataset (users, products, categories,
transactions, browsing sessions) is stored in two NoSQL systems chosen for their access patterns, processed with
Apache Spark, and combined in an integrated funnel analysis.

| Part | What | Where |
|---|---|---|
| Data | Seeded generator + validator | `data_gen/` |
| 1a | MongoDB document model, loader, 4 aggregations, index evidence | `mongo/` |
| 1b | HBase row-key design, loader, 3 query types | `hbase/` |
| 2 | Spark cleaning, bought-together analysis, Spark SQL | `spark/` |
| 3 | Integrated funnel: HBase + MongoDB joined in Spark | `integration/funnel.py` |
| 4 | Four charts | `viz/charts.py` |

## Architecture

```
generator -> JSON Lines (data/)
   |-- mongo/load.py  -> MongoDB  (users, products, transactions, sessions: profiles, catalogue, orders)
   |-- hbase/load.py  -> HBase    (user_sessions, product_metrics: time-ordered session events)
   |-- spark/clean.py -> Parquet  (partitioned by day) -> spark/analysis.py, spark/sql_queries.py
   '-- integration/funnel.py: reads HBase + MongoDB -> Spark join -> outputs/funnel.csv
outputs/*.csv -> viz/charts.py -> outputs/chart*.png
```

Why each system: **MongoDB** for nested, query-by-attribute documents (orders with embedded items, catalogue with
embedded category) and aggregation pipelines; **HBase** for very large, append-heavy, time-ordered session events read by
row-key range ("last N sessions of a user"); **Spark** for the joins and aggregations that neither store does well.

## Requirements

Linux or WSL2 Ubuntu, Python 3.10+ (tested 3.13/3.14), Java 17+ (for Spark), Docker with Compose.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d          # MongoDB on 27017, HBase Thrift on 9090 (give HBase ~60 s to start)
```

## Reproduce everything (about 10-15 minutes)

```bash
# 0. Data (seed 42; defaults = 10,000 users, 5,000 products, 200,000 sessions, 50,000 transactions)
python data_gen/generate.py
python data_gen/validate.py data

# 1a. MongoDB
python mongo/load.py                 # loads the first session file (100,000 sessions) + creates indexes
python mongo/aggregations.py         # writes outputs/mongo_*.csv
python mongo/index_demo.py           # explain() before/after the compound index

# 1b. HBase
python hbase/load.py                 # 100,000 sessions + product_metrics
python hbase/queries.py              # last-N sessions, time range, product metrics

# 2. Spark
python spark/clean.py data parquet
python spark/analysis.py parquet outputs
python spark/sql_queries.py parquet data outputs

# 3. Integration (needs HBase + MongoDB loaded)
python integration/funnel.py         # writes outputs/funnel.csv

# 4. Charts
python viz/charts.py                 # writes outputs/chart1..4_*.png
```

Smaller/faster run: `NUM_SESSIONS=20000 NUM_TRANSACTIONS=5000 python data_gen/generate.py`.

## Design notes

* **Mongo `_id`** is the business key (`user_id`, `product_id`, `transaction_id`, `session_id`).
  Embedded: category/subcategory in products, items in transactions, page views in sessions,
  `purchase_summary` in users (pre-aggregated for profile reads). Indexes: `(user_id, timestamp desc)`,
  multikey `items.product_id`, `page_views.product_id`.
* **HBase `user_sessions`** row key `user_id#reverse_ts` (`reverse_ts = 10^13 - epoch_ms`, zero-padded), so a user's newest
  sessions are the first rows of a prefix scan. Families `info` / `pv` / `cart`. **`product_metrics`** row key
  `product_id#yyyymmdd`, family `m` (views, cart_adds, purchases).
* **Funnel stages** are cumulative: session > viewed product > added to cart > reached checkout > purchased
  (a transaction exists for the session). "Purchased" is defined by the transaction record, because the generator's
  `converted` flag is inconsistent with the number of session-linked transactions.
* Spark uses explicit schemas, Parquet partitioned by day, broadcast joins for the small dimensions, window functions
  (running totals, month-over-month growth, top-N per category, NTILE quartiles).

## Known limitations

* The data is synthetic and largely random, so signals (e.g. device or referrer effects) are weak by construction.
* HBase and MongoDB hold the first 100,000 of the 200,000 sessions; 4,918 session-linked transactions belong to the
  second session file and are not matched in the funnel.
* The customer-frequency segment uses total orders, which include the purchases being measured, so that comparison is
  partly circular.
* Single-node Docker HBase is for demonstration; its Thrift server can time out after long idle periods
  (`docker compose restart hbase` fixes it).
* First and last calendar months of the data are partial.
* On native Windows, Spark needs `winutils`/`hadoop.dll`; run in WSL2/Linux instead.
