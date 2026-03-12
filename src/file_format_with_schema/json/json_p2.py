from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()


df=spark.read.json('students.json')
df.show()
df.printSchema()