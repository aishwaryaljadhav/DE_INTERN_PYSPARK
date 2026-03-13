from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("WriteJSONExample").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

data = [
    (1, "Aisha", "IT", 50000),
    (2, "Rahul", "HR", 45000),
    (3, "Neha", "Finance", 60000)
]

columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)

df.show()


df.write \
.mode("overwrite") \
.option("ignoreNullFields", True) \
.json("json_output")