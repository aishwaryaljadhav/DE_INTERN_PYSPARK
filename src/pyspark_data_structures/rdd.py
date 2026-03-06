from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD Example").getOrCreate()

sc = spark.sparkContext

numbers = [10, 20, 30, 40, 50]
rdd = sc.parallelize(numbers)

squared_rdd = rdd.map(lambda x: x * x)

result = squared_rdd.collect()

print("Squared Numbers:", result)