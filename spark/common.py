"""Shared Spark helpers: session builder and explicit schemas (avoids a costly inference pass
and avoids a struct with thousands of fields for the dynamic-key cart_contents dict)."""
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Make Spark use the same Python as this script (important on Windows).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

def get_spark(app="ecom", mem="4g"):
    # Windows only: Hadoop's local file access needs hadoop.dll + winutils.exe from %HADOOP_HOME%\\bin.
    # Java finds hadoop.dll through PATH, so put that folder first on PATH before the JVM starts.
    # (Do NOT pass -Djava.library.path via Spark options: backslashes are treated as escapes.)
    hh = os.environ.get("HADOOP_HOME")
    if os.name == "nt" and hh:
        hb = os.path.join(hh, "bin")
        if hb.lower() not in os.environ.get("PATH", "").lower():
            os.environ["PATH"] = hb + os.pathsep + os.environ.get("PATH", "")
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.driver.memory", mem)
            .config("spark.sql.shuffle.partitions", "16")      # small data, local mode
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())

GEO = StructType([StructField(n, StringType()) for n in ("city", "state", "country", "ip_address")])
SESSION_SCHEMA = StructType([
    StructField("session_id", StringType()), StructField("user_id", StringType()),
    StructField("start_time", StringType()), StructField("end_time", StringType()),
    StructField("duration_seconds", IntegerType()), StructField("geo_data", GEO),
    StructField("device_profile", StructType([StructField(n, StringType()) for n in ("type", "os", "browser")])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(StructType([
        StructField("timestamp", StringType()), StructField("page_type", StringType()),
        StructField("product_id", StringType()), StructField("category_id", StringType()),
        StructField("view_duration", IntegerType())]))),
    # dynamic keys (product ids) -> MapType, not a struct
    StructField("cart_contents", MapType(StringType(), StructType(
        [StructField("quantity", IntegerType()), StructField("price", DoubleType())]))),
    StructField("conversion_status", StringType()), StructField("referrer", StringType())])

TX_SCHEMA = StructType([
    StructField("transaction_id", StringType()), StructField("session_id", StringType()),
    StructField("user_id", StringType()), StructField("timestamp", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType()), StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()), StructField("subtotal", DoubleType())]))),
    StructField("subtotal", DoubleType()), StructField("discount", DoubleType()),
    StructField("total", DoubleType()), StructField("payment_method", StringType()),
    StructField("status", StringType())])

USER_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("geo_data", StructType([StructField(n, StringType()) for n in ("city", "state", "country")])),
    StructField("registration_date", StringType()), StructField("last_active", StringType())])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType()), StructField("name", StringType()),
    StructField("category_id", StringType()), StructField("subcategory_id", StringType()),
    StructField("base_price", DoubleType()), StructField("current_stock", IntegerType()),
    StructField("is_active", BooleanType()),
    StructField("price_history", ArrayType(StructType([
        StructField("price", DoubleType()), StructField("date", StringType())]))),
    StructField("creation_date", StringType())])
