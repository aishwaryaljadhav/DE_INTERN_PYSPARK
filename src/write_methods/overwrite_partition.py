from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionOverwrite").getOrCreate()

data = [
    (1,"Aisha","India"),
    (2,"Rahul","India"),
    (3,"John","USA"),
    (4,"David","USA")
]

columns = ["id","name","country"]

df = spark.createDataFrame(data,columns)

df.write \
.mode("overwrite") \
.partitionBy("country") \
.parquet("partition_output")