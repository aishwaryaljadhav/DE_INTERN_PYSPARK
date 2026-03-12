from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", FloatType(), True)
])

df = spark.read.csv("employee.csv", header=True, schema=schema)

df.show()
df.printSchema()