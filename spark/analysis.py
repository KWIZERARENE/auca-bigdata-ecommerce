"""Spark batch job 2: product recommendation indicators ("bought together").

Usage: python spark/analysis.py [parquet_dir] [outputs_dir]

For every pair of products appearing in the same transaction we compute:
  support      = pair_count / n_transactions
  confidence   = P(B in basket | A in basket) = pair_count / count(A)
  lift         = confidence / P(B) = support / (support(A) * support(B))
Lift > 1 means the pair co-occurs more than chance. We keep pairs with pair_count >= MIN_SUPPORT.

NOTE: the data are synthetic and baskets are random, so we expect lift ~ 1 and tiny counts.
This is reported honestly in the report; the pipeline is what matters.
"""
import sys
from pyspark.sql import functions as F
from common import get_spark

PQ = sys.argv[1] if len(sys.argv) > 1 else "parquet"
OUT = sys.argv[2] if len(sys.argv) > 2 else "outputs"
MIN_SUPPORT = 2
spark = get_spark("ecom-bought-together")

items = spark.read.parquet(f"{PQ}/items").select("transaction_id", "product_id").distinct()
products = spark.read.parquet(f"{PQ}/products").select("product_id", "name", "category_id")
n_tx = items.select("transaction_id").distinct().count()

# self-join on transaction_id; p1 < p2 keeps each unordered pair once
a = items.withColumnRenamed("product_id", "p1")
b = items.withColumnRenamed("product_id", "p2")
pairs = (a.join(b, "transaction_id").filter("p1 < p2")
          .groupBy("p1", "p2").agg(F.count("*").alias("pair_count"))
          .filter(F.col("pair_count") >= MIN_SUPPORT))

# per-product basket counts: small table -> broadcast
cnt = items.groupBy("product_id").agg(F.count("*").alias("n"))
c1 = F.broadcast(cnt.withColumnRenamed("product_id", "p1").withColumnRenamed("n", "n1"))
c2 = F.broadcast(cnt.withColumnRenamed("product_id", "p2").withColumnRenamed("n", "n2"))
scored = (pairs.join(c1, "p1").join(c2, "p2")
          .withColumn("support", F.col("pair_count") / n_tx)
          .withColumn("conf_p1_to_p2", F.col("pair_count") / F.col("n1"))
          .withColumn("conf_p2_to_p1", F.col("pair_count") / F.col("n2"))
          .withColumn("lift", F.col("pair_count") * n_tx / (F.col("n1") * F.col("n2"))))

pn = F.broadcast(products)
named = (scored.join(pn.select(F.col("product_id").alias("p1"), F.col("name").alias("name1"),
                               F.col("category_id").alias("cat1")), "p1")
               .join(pn.select(F.col("product_id").alias("p2"), F.col("name").alias("name2"),
                               F.col("category_id").alias("cat2")), "p2")
               .orderBy(F.desc("pair_count"), F.desc("lift")))
named.cache()
total_pairs = named.count()
print(f"transactions: {n_tx:,} | pairs with count >= {MIN_SUPPORT}: {total_pairs:,}")
named.select("p1", "p2", "pair_count", F.round("lift", 1).alias("lift"),
             F.round("conf_p1_to_p2", 3).alias("conf")).show(10, truncate=False)

# distribution of pair counts: shows how weak the signal is
print("pair_count distribution (all pairs, before support filter):")
(a.join(b, "transaction_id").filter("p1 < p2").groupBy("p1", "p2").count()
  .groupBy("count").agg(F.count("*").alias("n_pairs")).orderBy("count").show(10))

named.limit(500).toPandas().to_csv(f"{OUT}/bought_together_top500.csv", index=False)
print("saved", f"{OUT}/bought_together_top500.csv")
spark.stop()
