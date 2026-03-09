from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("ExampleRDD")\
    .getOrCreate()
sc=spark.sparkContext
data=[1,2,3,2,4,1,5]
rdd=sc.parallelize(data)
rdd2=rdd.map(lambda x: x*2)
print(rdd2.collect())

rdd3=rdd.filter(lambda x: x%2==0)
print(rdd3.collect())

rdd4=rdd.reduce(lambda x,y: x+y)
print(rdd4)
spark.stop()
