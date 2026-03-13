from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AppendExample").getOrCreate()

data = [
    (4,"Ram"),
    (5,"Vishal")
]

columns = ["id","name"]

df = spark.createDataFrame(data,columns)

df.write \
.mode("append") \
.option("header","true") \
.csv("overwrite_output")