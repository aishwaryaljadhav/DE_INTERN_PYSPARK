from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WriteCSVExample").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

data = [
(1,"Aisha","IT",50000),
(2,"Rahul","HR",45000),
(3,"Neha","Finance",60000)
]

df = spark.createDataFrame(data, schema)

df.write \
.mode("overwrite") \
.csv("csv_output")