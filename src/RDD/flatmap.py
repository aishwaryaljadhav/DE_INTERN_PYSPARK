from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("FlatMapEg")\
    .getOrCreate()
sc=spark.sparkContext
data=["Hello Spark", "PySpark is easy"]
rdd=sc.parallelize(data)
rdd2=rdd.flatMap(lambda x: x.split(" "))
print(rdd2.collect())
spark.stop()