from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("selecting")\
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

df.select('name','city').show()

df.withColumn('age_after_5_yrs',df.age+5).show()

df.drop('city').show()

df.dropna().show()

spark.stop()