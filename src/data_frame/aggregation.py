from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
spark=SparkSession.builder\
    .appName("aggregation")\
    .getOrCreate()

data = [
(1,"Aishwarya",21,"Bangalore"),
(2,"Rahul",22,"Mumbai"),
(3,"Neha",20,"Delhi"),
(4,"Amit",23,"Mumbai"),
(1,"Aishwarya",21, None)
]

columns = ["id","name","age","city"]

df = spark.createDataFrame(data,columns)

df.select(avg(df.age)).show()

df.groupBy("city").count().show()

df.orderBy("age").show()

df.orderBy(df.age.desc()).show()

spark.stop()


