from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OverwriteExample").getOrCreate()

data = [
    (1,"Aisha"),
    (2,"Rahul"),
    (3,"Neha")
]

columns = ["id","name"]

df = spark.createDataFrame(data,columns)

df.write \
.mode("overwrite") \
.option("header","true") \
.csv("overwrite_output")