from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDDExample").getOrCreate()

sc = spark.sparkContext

data = [1,2,3,4,5]
rdd = sc.parallelize(data)

print(rdd.collect())