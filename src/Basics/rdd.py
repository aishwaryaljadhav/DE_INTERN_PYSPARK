from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDDExample").getOrCreate()
sc = spark.sparkContext

data = [5, 12, 8, 20, 3, 15]
rdd = sc.parallelize(data)

result = rdd.filter(lambda x: x > 10).collect()

print(result)