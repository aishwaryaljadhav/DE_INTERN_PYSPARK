from pyspark import StorageLevel
from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("persist_eg")\
    .getOrCreate()

data=[('Aish', 21), ('Sam', 22), ('Vishal', 25)]
column=['name','age']
df= spark.createDataFrame(data, column)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.show()
df.filter(df.age>22).show()
df.unpersist()

spark.stop()