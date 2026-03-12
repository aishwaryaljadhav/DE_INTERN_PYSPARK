from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ExplodeArrayExample").getOrCreate()

data = [
(1,"Aish",["Python","SQL","Spark"]),
(2,"Rahul",["Java","AWS"]),
(3,"Neha",["Excel"]),
(4,"Riya",None)
]

columns = ["id","name","skills"]

df = spark.createDataFrame(data,columns)

df.select('id', 'name', explode('skills').alias('skill')).show()

df.select('id', 'name', explode_outer('skills').alias('skill')).show()

df.select('id', 'name', posexplode('skills').alias('position','skill')).show()