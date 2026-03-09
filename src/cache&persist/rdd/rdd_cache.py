from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("cache_eg")\
    .getOrCreate()
sc=spark.sparkContext
data=[1,2,3,2,4,1,5]
rdd=sc.parallelize(data)
rdd2=rdd.map(lambda x: x*2)
rdd2.cache()
print(rdd2.collect())
print(rdd2.sum())

rdd2.unpersist()
spark.stop()
