from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

df=spark.read.csv("employee.csv", header=True, inferSchema=True)
df.show()
df.printSchema()