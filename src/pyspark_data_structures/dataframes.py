from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

data = [("Aishwarya",21),
        ("Rahul",23),
        ("Sneha",22),
        ("Rohan",24)]

columns = ["Name","Age"]

df = spark.createDataFrame(data, columns)


df.show()

df.select("Name").show()

df.filter(df.Age > 22).show()

df.select(avg("Age")).show()