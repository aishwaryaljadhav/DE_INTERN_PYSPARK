from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("DataFrameExample").getOrCreate()
data=[("Aish", 25), ("Sam", 30), ("Vishal", 35)]
df=spark.createDataFrame(data,["Name", "Age"])
df.show()
spark.stop()