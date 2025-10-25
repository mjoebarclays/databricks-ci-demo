# Simple “hello job” – prints row count from system table
df = spark.sql("SELECT 1 AS ok")
print("Row count:", df.count())
 