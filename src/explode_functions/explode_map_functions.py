from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ExplodeMapExample").getOrCreate()

data = [
(1, {"Math":90,"Science":85}),
(2, {"Math":70,"English":75}),
(3, {"Science":91}),
(4, None)
]

columns = ["id","marks"]

df = spark.createDataFrame(data,columns)

df.select('id', explode('marks').alias('subject','mark')).show()

df.select('id', explode_outer('marks').alias('subject','mark')).show()

df.select('id', posexplode('marks').alias('position','subject','mark')).show()