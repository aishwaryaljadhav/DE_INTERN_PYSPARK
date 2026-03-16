from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Full_Load_Example").getOrCreate()

data = [
    (1, "Aishwarya", "Bangalore"),
    (2, "Rahul", "Mumbai"),
    (3, "Sneha", "Delhi")
]

columns = ["id", "name", "city"]

source_df = spark.createDataFrame(data, columns)

print("Source Data")
source_df.show()

source_df.write \
    .mode("overwrite") \
    .parquet("output/full_load_customers")

print("Full Load Completed")