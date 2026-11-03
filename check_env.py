import importlib, shutil, sys

def ok(name, cond, extra=""):
    print(("OK " if cond else "FAIL ") + name, extra)

ok("python >= 3.10", sys.version_info >= (3, 10), sys.version.split()[0])
for m in ["faker", "pandas", "numpy", "pymongo", "happybase", "pyspark", "matplotlib", "seaborn", "plotly"]:
    try:
        importlib.import_module(m); ok("import " + m, True)
    except Exception as e:
        ok("import " + m, False, str(e)[:80])
ok("java on PATH", shutil.which("java") is not None)
ok("docker on PATH", shutil.which("docker") is not None)
try:
    import pymongo
    v = pymongo.MongoClient("localhost", 27017, serverSelectionTimeoutMS=3000).server_info()["version"]
    ok("MongoDB reachable", True, "server " + v)
except Exception as e:
    ok("MongoDB reachable", False, str(e)[:80])
try:
    import happybase
    c = happybase.Connection("localhost", port=9090, timeout=5000); c.open()
    ok("HBase Thrift reachable", True, "tables: %s" % c.tables())
except Exception as e:
    ok("HBase Thrift reachable", False, str(e)[:80])
try:
    from pyspark.sql import SparkSession
    s = SparkSession.builder.master("local[1]").appName("check").getOrCreate()
    ok("Spark starts", True, "spark " + s.version + ", count=" + str(s.range(1000).count()))
    s.stop()
except Exception as e:
    ok("Spark starts", False, str(e)[:120])
