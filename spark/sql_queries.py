"""Spark SQL analytics. Usage: python spark/sql_queries.py [parquet_dir] [data_dir] [outputs_dir]

Four queries over views built from the cleaned Parquet:
  Q1 monthly revenue by category with running total and month-over-month growth (CTEs + window functions)
  Q2 top-3 products per category by units sold (ROW_NUMBER)
  Q3 session -> purchase conversion by device and referrer (LEFT JOIN across sessions and transactions)
  Q4 "Mongo + HBase" style join: a user-profile sample that would live in MongoDB
     (mongo_users, with purchase_summary) joined to engagement metrics that would live in
     HBase (hbase_engagement). Both are built here as DataFrames; the point is the SQL logic.
Revenue = sum of line-item subtotals (BEFORE order-level discount); say so in the report.
"""
import sys
from pyspark.sql import functions as F
from common import get_spark

PQ = sys.argv[1] if len(sys.argv) > 1 else "parquet"
DATA = sys.argv[2] if len(sys.argv) > 2 else "data"
OUT = sys.argv[3] if len(sys.argv) > 3 else "outputs"
spark = get_spark("ecom-sql")

for name in ["users", "products", "transactions", "items", "sessions", "page_views"]:
    spark.read.parquet(f"{PQ}/{name}").createOrReplaceTempView(name)
(spark.read.json(f"{DATA}/categories.jsonl")
      .select("category_id", F.col("name").alias("category_name")).createOrReplaceTempView("categories"))

# --- simulated MongoDB users collection (profile + embedded purchase summary) ---
spark.sql("""
  SELECT u.user_id, u.geo_data.country AS country, u.reg_month,
         named_struct('total_orders', COUNT(t.transaction_id),
                      'total_spend',  ROUND(COALESCE(SUM(t.total), 0), 2),
                      'last_purchase', MAX(t.ts)) AS purchase_summary
  FROM users u LEFT JOIN transactions t ON u.user_id = t.user_id
  GROUP BY u.user_id, u.geo_data.country, u.reg_month
""").createOrReplaceTempView("mongo_users")
# --- simulated HBase per-user engagement (what a prefix scan per user would yield) ---
spark.sql("""
  SELECT user_id, COUNT(*) AS sessions, ROUND(AVG(duration_seconds), 1) AS avg_duration,
         ROUND(AVG(n_page_views), 2) AS avg_page_views
  FROM sessions GROUP BY user_id
""").createOrReplaceTempView("hbase_engagement")

QUERIES = {
"q1_monthly_revenue_by_category": """
WITH rev AS (
  SELECT c.category_name, date_format(i.ts, 'yyyy-MM') AS month,
         ROUND(SUM(i.subtotal), 2) AS revenue, SUM(i.quantity) AS units
  FROM items i
  JOIN products p   ON i.product_id = p.product_id
  JOIN categories c ON p.category_id = c.category_id
  GROUP BY c.category_name, date_format(i.ts, 'yyyy-MM')
)
SELECT category_name, month, revenue, units,
       ROUND(SUM(revenue) OVER (PARTITION BY category_name ORDER BY month), 2) AS running_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (PARTITION BY category_name ORDER BY month))
                  / LAG(revenue) OVER (PARTITION BY category_name ORDER BY month), 1) AS mom_growth_pct
FROM rev ORDER BY category_name, month
""",
"q2_top3_products_per_category": """
SELECT * FROM (
  SELECT c.category_name, i.product_id, p.name AS product_name,
         SUM(i.quantity) AS units, ROUND(SUM(i.subtotal), 2) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY c.category_name ORDER BY SUM(i.quantity) DESC, SUM(i.subtotal) DESC) AS rk
  FROM items i
  JOIN products p   ON i.product_id = p.product_id
  JOIN categories c ON p.category_id = c.category_id
  GROUP BY c.category_name, i.product_id, p.name
) WHERE rk <= 3 ORDER BY category_name, rk
""",
"q3_conversion_by_device_referrer": """
SELECT s.device, s.referrer, COUNT(*) AS sessions,
       SUM(CASE WHEN s.conversion_status = 'converted' THEN 1 ELSE 0 END) AS flagged_converted,
       COUNT(t.transaction_id) AS with_transaction,
       ROUND(100.0 * COUNT(t.transaction_id) / COUNT(*), 2) AS txn_conv_pct,
       ROUND(100.0 * SUM(CASE WHEN s.conversion_status = 'converted' THEN 1 ELSE 0 END) / COUNT(*), 2) AS flagged_conv_pct
FROM sessions s LEFT JOIN transactions t ON s.session_id = t.session_id
GROUP BY s.device, s.referrer ORDER BY txn_conv_pct DESC
""",
"q4_profile_x_engagement_quartiles": """
WITH joined AS (
  SELECT u.user_id, u.country, u.purchase_summary.total_orders AS orders,
         u.purchase_summary.total_spend AS spend, e.sessions, e.avg_duration,
         NTILE(4) OVER (ORDER BY u.purchase_summary.total_spend) AS spend_quartile
  FROM mongo_users u JOIN hbase_engagement e ON u.user_id = e.user_id
)
SELECT spend_quartile, COUNT(*) AS users, ROUND(AVG(spend), 2) AS avg_spend,
       ROUND(AVG(orders), 2) AS avg_orders, ROUND(AVG(sessions), 1) AS avg_sessions,
       ROUND(AVG(avg_duration), 1) AS avg_session_secs
FROM joined GROUP BY spend_quartile ORDER BY spend_quartile
""",
}

for name, q in QUERIES.items():
    df = spark.sql(q)
    print(f"\n=== {name} ===")
    df.show(8, truncate=False)
    df.toPandas().to_csv(f"{OUT}/{name}.csv", index=False)
print("CSV results saved to", OUT)
spark.stop()
