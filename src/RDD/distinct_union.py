from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("distinct_union")\
    .getOrCreate()
sc=spark.sparkContext
data1=[1,2,2,3,4,5]
data2=[5,6,7]
rdd1=sc.parallelize(data1)
rdd2=sc.parallelize(data1)
rdd3=rdd1.distinct()
print(rdd3.count())

print(rdd1.union(rdd2).take(3))
print(rdd1.first())


