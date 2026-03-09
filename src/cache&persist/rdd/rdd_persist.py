from pyspark.sql import SparkSession
from pyspark import StorageLevel
spark=SparkSession.builder\
    .appName("persist_eg")\
    .getOrCreate()
sc=spark.sparkContext
data=[1,2,3,2,4,1,5]
rdd=sc.parallelize(data)
rdd2=rdd.map(lambda x: x*2)
rdd2.persist(StorageLevel.MEMORY_AND_DISK)
print(rdd2.collect())
print(rdd2.sum())

rdd2.unpersist()
spark.stop()
