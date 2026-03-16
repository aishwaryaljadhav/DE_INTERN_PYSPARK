from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

spark = SparkSession.builder \
    .appName("Snapshot_Load_Example") \
    .getOrCreate()

data = [
    (1, "Aishwarya", "Bangalore"),
    (2, "Rahul", "Mumbai"),
    (3, "Sneha", "Delhi")
]

columns = ["id", "name", "city"]

df = spark.createDataFrame(data, columns)

snapshot_df = df.withColumn(
    "snapshot_date",
    current_date()
)

print("Snapshot Data")
snapshot_df.show()

snapshot_df.write \
    .mode("append") \
    .parquet("output/snapshot_table")

print("Snapshot Stored Successfully")