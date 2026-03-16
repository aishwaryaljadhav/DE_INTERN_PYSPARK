from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Incremental_Load_Example") \
    .getOrCreate()

existing_data = [
    (1, "Aishwarya", "Bangalore"),
    (2, "Rahul", "Mumbai")
]

columns = ["id", "name", "city"]

target_df = spark.createDataFrame(existing_data, columns)

new_data = [
    (1, "Aishwarya", "Bangalore"),
    (2, "Rahul", "Mumbai"),
    (3, "Sneha", "Delhi"),
    (4, "Raj", "Pune")
]

source_df = spark.createDataFrame(new_data, columns)

incremental_df = source_df.join(
    target_df,
    "id",
    "left_anti"
)

print("New Records to Load")
incremental_df.show()

incremental_df.write \
    .mode("append") \
    .parquet("output/incremental_load")

print("Incremental Load Completed")